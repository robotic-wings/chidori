//! The detached-agent hub: the process-wide registry of `AgentEntry`s, the
//! spawn-name file/mutex locking, the supervision-thread spawn/stop machinery,
//! the hibernation re-arm timer, and `SpawnOptions`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, OnceLock, TryLockError as MutexTryLockError, Weak,
};
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::{json, Value};

use crate::runtime::context::RuntimeContext;
use crate::runtime::snapshot::{QueuedSignal, SIGNAL_INBOX_FILE};
use crate::runtime::store::RunStoreFactory;

use super::*;

pub(crate) struct AgentState {
    pub(crate) descriptor: AgentDescriptor,
    /// The live iteration's context while a supervision thread is running —
    /// the address live sends enqueue into (durably, through the ctx store).
    pub(crate) live_ctx: Option<RuntimeContext>,
    /// Whether this process's lease-owning supervisor is actively driving the
    /// agent, including setup and handoff. `thread_live` also covers lifecycle
    /// work such as waiting for a lease.
    pub(crate) execution_active: bool,
    pub(crate) thread_live: bool,
    /// A matching delivery can arrive after hibernation releases its lease but
    /// before the current supervisor thread has returned. The thread wrapper
    /// consumes this bit and starts the queued wake after marking itself dead.
    pub(crate) wake_pending: bool,
    pub(crate) stop_requested: bool,
}

impl AgentEntry {
    pub(crate) fn is_settled(state: &AgentState) -> bool {
        matches!(
            state.descriptor.status.as_str(),
            "completed" | "failed" | "stopped" | "paused"
        )
    }
}

/// Process-global supervisor for detached agents. One per process; agents are
/// identified by their registered (or generated) name.
pub struct DetachedAgentHub {
    pub(crate) parts: Mutex<Option<AgentRuntimeParts>>,
    pub(crate) entries: Mutex<HashMap<String, Arc<AgentEntry>>>,
    pub(crate) timer_started: Mutex<bool>,
}

static HUB: OnceLock<DetachedAgentHub> = OnceLock::new();

static SPAWN_NAME_MUTEXES: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();

pub(crate) const SPAWN_NAME_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn spawn_name_mutex(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = SPAWN_NAME_MUTEXES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

pub(crate) fn acquire_spawn_name_mutex<'a>(
    lock: &'a Mutex<()>,
    name: &str,
) -> Result<MutexGuard<'a, ()>, String> {
    let deadline = Instant::now() + SPAWN_NAME_LOCK_TIMEOUT;
    loop {
        match lock.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(MutexTryLockError::Poisoned(_)) => {
                return Err(format!(
                    "chidori.agents.spawn: in-process name lock for `{name}` is poisoned"
                ));
            }
            Err(MutexTryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(MutexTryLockError::WouldBlock) => {
                return Err(format!(
                    "chidori.agents.spawn: timed out waiting for in-process name lock for `{name}`"
                ));
            }
        }
    }
}

pub(crate) fn acquire_spawn_name_file_lock(
    path: &Path,
    name: &str,
) -> Result<std::fs::File, String> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|err| {
            format!(
                "chidori.agents.spawn: opening name lock {}: {err}",
                path.display()
            )
        })?;
    let deadline = Instant::now() + SPAWN_NAME_LOCK_TIMEOUT;
    #[cfg(test)]
    let mut reported_contention = false;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::Error(err)) => {
                return Err(format!(
                    "chidori.agents.spawn: locking name `{name}` at {}: {err}",
                    path.display()
                ));
            }
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                #[cfg(test)]
                if !reported_contention {
                    crate::runtime::host_agent::tests::report_same_name_spawn_lock_wait(name);
                    reported_contention = true;
                }
                #[cfg(not(test))]
                let _ = name;
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(format!(
                    "chidori.agents.spawn: timed out waiting for name lock `{name}` at {}",
                    path.display()
                ));
            }
        }
    }
}

pub(crate) fn with_agent_name_lock<T>(
    parts: &AgentRuntimeParts,
    name: &str,
    operation: impl FnOnce(&RunStoreFactory) -> Result<T, String>,
) -> Result<T, String> {
    let factory = RunStoreFactory::shared(&parts.run_base);
    factory
        .ensure_initialized()
        .map_err(|err| format!("chidori.agents: initializing run store: {err:#}"))?;
    let lock_path = factory
        .registry_name_lock_path(name)
        .map_err(|err| format!("chidori.agents: preparing name lock: {err:#}"))?;
    let process_lock = spawn_name_mutex(&lock_path);
    let _process_name_guard = acquire_spawn_name_mutex(&process_lock, name)?;
    let _name_file_guard = acquire_spawn_name_file_lock(&lock_path, name)?;
    operation(&factory)
}

pub fn hub() -> &'static DetachedAgentHub {
    HUB.get_or_init(|| DetachedAgentHub {
        parts: Mutex::new(None),
        entries: Mutex::new(HashMap::new()),
        timer_started: Mutex::new(false),
    })
}

impl DetachedAgentHub {
    /// Install (or refresh) the engine parts the supervisor runs agents with.
    /// Entries capture the parts current at their creation, so refreshing is
    /// safe for existing agents.
    pub fn install_parts(&self, parts: AgentRuntimeParts) {
        *self.parts.lock().unwrap() = Some(parts);
    }

    /// The parts installed at boot — how code without a spawning backend
    /// (the server's HTTP handlers) addresses the hub.
    pub fn installed_parts(&self) -> Result<AgentRuntimeParts, String> {
        self.parts()
    }

    pub(crate) fn parts(&self) -> Result<AgentRuntimeParts, String> {
        self.parts
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "detached agents: no runtime parts installed".to_string())
    }

    /// The entry for `name`, re-materialized from the durable registry when
    /// this process has never seen it (the post-restart / replayed-spawn path).
    pub(crate) fn ensure_entry(
        &self,
        parts: &AgentRuntimeParts,
        name: &str,
    ) -> Result<Arc<AgentEntry>, String> {
        // Preserve cache-first behavior for incarnation-bound operations such
        // as status/stop/join. Name-addressed lookup/send use the locked
        // resolver directly because they must follow the current registry name.
        if let Some(entry) = self.entries.lock().unwrap().get(name).cloned() {
            return Ok(entry);
        }
        with_agent_name_lock(parts, name, |factory| {
            self.resolve_entry_locked(parts, name, factory)?
                .ok_or_else(|| format!("chidori.agents: unknown agent `{name}`"))
        })
    }

    pub(crate) fn resolve_entry_locked(
        &self,
        parts: &AgentRuntimeParts,
        name: &str,
        factory: &RunStoreFactory,
    ) -> Result<Option<Arc<AgentEntry>>, String> {
        let registered = factory
            .registry_get(name)
            .map_err(|err| format!("chidori.agents: reading registry: {err}"))?;
        let Some(registered) = registered else {
            return Ok(None);
        };
        let registered_run_id = registered
            .get("run_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("chidori.agents: registry entry `{name}` has no run_id"))?;
        let descriptor: AgentDescriptor =
            serde_json::from_value(registered.get("descriptor").cloned().unwrap_or_default())
                .map_err(|err| format!("chidori.agents: parsing registry entry: {err}"))?;
        if descriptor.run_id != registered_run_id {
            return Err(format!(
                "chidori.agents: registry entry `{name}` run_id does not match its descriptor"
            ));
        }
        if let Some(entry) = self.entries.lock().unwrap().get(name).cloned() {
            let state = entry.state.lock().unwrap();
            if state.descriptor.run_id == registered_run_id {
                return Ok(Some(entry.clone()));
            }
            if state.thread_live || state.execution_active || state.live_ctx.is_some() {
                return Err(format!(
                    "chidori.agents: registry incarnation for `{name}` changed while its local predecessor was active"
                ));
            }
        }
        let parts = parts.clone();
        let entry = Arc::new(AgentEntry {
            name: name.to_string(),
            parts,
            state: Mutex::new(AgentState {
                descriptor,
                live_ctx: None,
                execution_active: false,
                thread_live: false,
                wake_pending: false,
                stop_requested: false,
            }),
            signal: Condvar::new(),
            #[cfg(test)]
            live_send_notify_count: std::sync::atomic::AtomicUsize::new(0),
        });
        self.entries
            .lock()
            .unwrap()
            .insert(name.to_string(), entry.clone());
        Ok(Some(entry))
    }

    /// Spawn a detached agent: persist its descriptor + registry entry, then
    /// start its supervision thread. Returns `{ name, runId }`.
    pub fn spawn(
        &self,
        parts: &AgentRuntimeParts,
        source: &str,
        input: Value,
        options: &SpawnOptions,
        owner_run_id: String,
    ) -> Result<Value, String> {
        let parts = parts.clone();
        ensure_run_store_initialized(
            &parts.run_base,
            "chidori.agents.spawn: initializing run store",
        )?;
        let factory = RunStoreFactory::shared(&parts.run_base);
        let name = options
            .name
            .clone()
            .unwrap_or_else(|| format!("agent-{}", &uuid::Uuid::new_v4().to_string()[..8]));
        let lock_path = factory
            .registry_name_lock_path(&name)
            .map_err(|err| format!("chidori.agents.spawn: preparing name lock: {err:#}"))?;
        let process_lock = spawn_name_mutex(&lock_path);
        let _process_name_guard = acquire_spawn_name_mutex(&process_lock, &name)?;
        #[cfg(test)]
        tests::report_same_name_spawn_lock_attempt(&name);
        let _name_file_guard = acquire_spawn_name_file_lock(&lock_path, &name)?;
        #[cfg(test)]
        tests::pause_same_name_spawn_after_lock(&name);

        // A live (non-settled) agent squats on its name; a settled one may be
        // replaced by a fresh spawn.
        let mut predecessor_run_id: Option<String> = None;
        if let Some(existing) = factory
            .registry_get(&name)
            .map_err(|err| format!("chidori.agents.spawn: reading registry entry: {err:#}"))?
        {
            let status = existing
                .get("descriptor")
                .and_then(|d| d.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if matches!(status, "running" | "hibernating") {
                return Err(format!(
                    "chidori.agents.spawn: agent name `{name}` is already registered and live"
                ));
            }
            predecessor_run_id = existing
                .get("run_id")
                .or_else(|| existing.get("descriptor").and_then(|d| d.get("run_id")))
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        let predecessor_mailbox = predecessor_run_id
            .as_deref()
            .map(|old_run_id| {
                let old_store = factory.store_for(old_run_id);
                load_detached_signal_inbox(old_store.as_ref(), old_run_id)
                    .map_err(|err| format!("migrating predecessor mailbox: {err}"))
            })
            .transpose()?;
        let now = Utc::now();
        let descriptor = AgentDescriptor {
            name: name.clone(),
            run_id: uuid::Uuid::new_v4().to_string(),
            source: source.to_string(),
            input,
            restart: options.restart.clone(),
            max_restarts: options.max_restarts,
            backoff_ms: options.backoff_ms,
            owner_run_id,
            model: options.model.clone(),
            status: "running".to_string(),
            listen: None,
            restarts: 0,
            output: None,
            error: None,
            created_at: now,
            updated_at: now,
        };
        let run_id = descriptor.run_id.clone();

        // Mail follows the NAME, not the incarnation: whatever the settled
        // predecessor never consumed is re-queued for the replacement instead
        // of stranding in the dead run's inbox.
        let mut predecessor_to_clear = None;
        if let (Some(old_run_id), Some(leftover)) =
            (predecessor_run_id.as_deref(), predecessor_mailbox)
        {
            if !leftover.is_empty() {
                let migrated: Vec<QueuedSignal> = leftover
                    .into_iter()
                    .enumerate()
                    .map(|(i, queued)| QueuedSignal {
                        delivery_seq: (i + 1) as u64,
                        ..queued
                    })
                    .collect();
                tracing::info!(
                    agent = %name,
                    from_run = %old_run_id,
                    messages = migrated.len(),
                    "migrating unconsumed mailbox from the settled predecessor"
                );
                let bytes = serde_json::to_vec_pretty(&migrated)
                    .map_err(|err| format!("serializing predecessor inbox migration: {err}"))?;

                #[cfg(test)]
                tests::pause_before_predecessor_destination_write(&name, &run_id);

                factory
                    .store_for(&run_id)
                    .put_blob(SIGNAL_INBOX_FILE, &bytes)
                    .map_err(|err| {
                        format!(
                            "migrating predecessor inbox `{SIGNAL_INBOX_FILE}` to replacement run `{run_id}`: {err:#}"
                        )
                    })?;

                predecessor_to_clear = Some(old_run_id.to_string());
            }
        }

        // Publish the replacement only after its migrated mailbox is durable.
        // A failed/partial mirror write may leave a destination orphan. A
        // complete local registry rename is the commit point for the shared
        // run-base topology, even if the SQLite mirror reports a later error.
        persist_spawn_descriptor_with_local_commit(&factory, &descriptor)?;

        let entry = Arc::new(AgentEntry {
            name: name.clone(),
            parts,
            state: Mutex::new(AgentState {
                descriptor,
                live_ctx: None,
                execution_active: false,
                thread_live: false,
                wake_pending: false,
                stop_requested: false,
            }),
            signal: Condvar::new(),
            #[cfg(test)]
            live_send_notify_count: std::sync::atomic::AtomicUsize::new(0),
        });
        self.entries
            .lock()
            .unwrap()
            .insert(name.clone(), entry.clone());
        if let Err(start_error) = self.start_thread(entry.clone()) {
            let cleanup = self.mark_spawn_start_failed(&factory, &entry, &start_error);
            return Err(match cleanup {
                Ok(()) => format!("chidori.agents.spawn: starting supervisor: {start_error}"),
                Err(cleanup_error) => format!(
                    "chidori.agents.spawn: starting supervisor failed: {start_error}; also failed to persist retryable failed state: {cleanup_error}"
                ),
            });
        }

        // Do not discard predecessor mail until the replacement has both a
        // published registry entry and a successfully started supervisor.
        if let Some(old_run_id) = predecessor_to_clear {
            if let Err(err) = factory
                .store_for(&old_run_id)
                .put_blob(SIGNAL_INBOX_FILE, b"[]")
            {
                tracing::warn!(
                    agent = %name,
                    from_run = %old_run_id,
                    to_run = %run_id,
                    error = %err,
                    "replacement supervisor started but predecessor inbox cleanup failed"
                );
            }
        }
        Ok(json!({ "name": name, "runId": run_id }))
    }

    pub(crate) fn mark_spawn_start_failed(
        &self,
        factory: &RunStoreFactory,
        entry: &Arc<AgentEntry>,
        start_error: &str,
    ) -> Result<(), String> {
        let descriptor = {
            let mut state = entry.state.lock().unwrap();
            state.thread_live = false;
            state.execution_active = false;
            state.live_ctx = None;
            state.descriptor.status = "failed".to_string();
            state.descriptor.error = Some(format!("supervisor start failed: {start_error}"));
            state.descriptor.updated_at = Utc::now();
            entry.signal.notify_all();
            state.descriptor.clone()
        };
        persist_spawn_descriptor_with_local_commit(factory, &descriptor)
    }

    /// Start the supervision thread for an entry that isn't already running.
    pub(crate) fn start_thread(&self, entry: Arc<AgentEntry>) -> Result<(), String> {
        ensure_run_store_initialized(
            &entry.parts.run_base,
            "chidori.agents: initializing run store before worker start",
        )?;
        #[cfg(test)]
        if tests::fail_next_spawn_thread_start(&entry.name) {
            return Err("injected supervisor thread-start failure".to_string());
        }
        {
            let mut state = entry.state.lock().unwrap();
            if state.thread_live {
                return Ok(());
            }
            state.thread_live = true;
            state.descriptor.status = "running".to_string();
            state.descriptor.updated_at = Utc::now();
        }
        persist_descriptor(
            &entry.factory(),
            &entry.state.lock().unwrap().descriptor.clone(),
        );
        let name = entry.name.clone();
        std::thread::Builder::new()
            .name(format!("chidori-agent-{name}"))
            .stack_size(AGENT_THREAD_STACK_BYTES)
            .spawn(move || {
                supervise_detached(hub(), &entry);
                let wake_pending = {
                    let mut state = entry.state.lock().unwrap();
                    state.thread_live = false;
                    state.execution_active = false;
                    state.live_ctx = None;
                    let wake_pending = std::mem::take(&mut state.wake_pending)
                        && state.descriptor.status == "running"
                        && !state.stop_requested;
                    entry.signal.notify_all();
                    wake_pending
                };
                if wake_pending {
                    if let Err(err) = hub().start_thread(entry.clone()) {
                        tracing::warn!(agent = %entry.name, error = %err, "starting queued detached-agent wake");
                    }
                }
            })
            .map_err(|err| format!("chidori.agents.spawn: spawning agent thread: {err}"))?;
        Ok(())
    }

    /// Deliver a named message to an agent's durable mailbox. Live agents get
    /// it in-memory too (write-through, like the server's live signal path);
    /// a hibernating agent whose listen set matches is woken.
    pub fn send(
        &self,
        parts: &AgentRuntimeParts,
        to: &str,
        name: &str,
        payload: Value,
        from: Value,
    ) -> Result<Value, String> {
        // Let concurrent test senders rendezvous before same-name
        // serialization. Once released, the production name lock owns the
        // registry reread, inbox update, and wake decision as one operation.
        #[cfg(test)]
        tests::pause_before_cross_process_inbox_update(to);
        with_agent_name_lock(parts, to, |factory| {
            let entry = self
                .resolve_entry_locked(parts, to, factory)?
                .ok_or_else(|| format!("chidori.agents: unknown agent `{to}`"))?;
            let mut wake = false;
            {
                let had_live_context = entry.state.lock().unwrap().live_ctx.is_some();
                if had_live_context {
                    // Do not carry a context snapshot across the handoff to the
                    // durable-mailbox path. The worker may clear it while a send
                    // is in flight, so pause here and re-check under the state
                    // lock before choosing which inbox writer owns this delivery.
                    #[cfg(test)]
                    tests::pause_after_live_context_snapshot(&entry.name);
                }
                let mut state = entry.state.lock().unwrap();
                if let Some(ctx) = state.live_ctx.clone() {
                    // Write-through: in-memory mailbox + durable inbox mutate in
                    // one critical section. Keep AgentState locked until the
                    // context has persisted, so the worker cannot clear this
                    // context and race a store-backed whole-file inbox write.
                    if let Err(err) = ctx.enqueue_live_signal(name, payload, from) {
                        return Err(format!(
                            "chidori.agents.send: live signal persistence failed before acknowledgement: {err}"
                        ));
                    }
                    #[cfg(test)]
                    entry
                        .live_send_notify_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    entry.signal.notify_all();
                    return Ok(json!({ "delivered": true }));
                }
                let run_id = state.descriptor.run_id.clone();
                let store = factory.store_for(&run_id);
                let mut update = |current: Option<Vec<u8>>| -> anyhow::Result<Vec<u8>> {
                    let mut inbox: Vec<QueuedSignal> = current
                        .as_deref()
                        .map(serde_json::from_slice)
                        .transpose()?
                        .unwrap_or_default();
                    let delivery_seq = inbox
                        .iter()
                        .map(|signal| signal.delivery_seq)
                        .max()
                        .unwrap_or(0)
                        .saturating_add(1);
                    inbox.push(QueuedSignal {
                        name: name.to_string(),
                        payload: payload.clone(),
                        from: from.clone(),
                        delivery_seq,
                        enqueued_at: Utc::now(),
                    });
                    Ok(serde_json::to_vec_pretty(&inbox)?)
                };
                if let Err(err) = store.update_blob(SIGNAL_INBOX_FILE, &mut update) {
                    return Err(format!(
                        "chidori.agents.send: inbox persistence failed before acknowledgement; persistence outcome may be uncertain and retry may duplicate: {err}"
                    ));
                }
                if state.descriptor.status == "hibernating" {
                    let matches = state
                        .descriptor
                        .listen
                        .as_ref()
                        .is_some_and(|l| l.names.iter().any(|n| n == name));
                    if matches {
                        wake = true;
                    }
                }
                // `running` with no worker thread means the previous driver died
                // mid-run: the delivery is durably queued, so use it as the wake
                // signal instead of letting the message strand.
                if state.descriptor.status == "running"
                    && !state.thread_live
                    && !state.stop_requested
                {
                    wake = true;
                }
                if wake {
                    state.descriptor.status = "running".to_string();
                    state.descriptor.updated_at = Utc::now();
                    if state.thread_live {
                        state.wake_pending = true;
                    }
                }
            }
            if wake {
                persist_descriptor(factory, &entry.state.lock().unwrap().descriptor.clone());
                if !entry.state.lock().unwrap().thread_live {
                    self.start_thread(entry.clone())?;
                }
            }
            let live = {
                let state = entry.state.lock().unwrap();
                !AgentEntry::is_settled(&state)
            };
            Ok(json!({ "delivered": live }))
        })
    }

    /// Wait for an agent to settle (completed / failed / stopped / paused).
    /// With a timeout, returns the current status snapshot on expiry. A
    /// hibernating agent does not settle — services hibernate indefinitely by
    /// design — so join a service only with a timeout.
    pub fn join(
        &self,
        parts: &AgentRuntimeParts,
        to: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Value, String> {
        let entry = self.ensure_entry(parts, to)?;
        let deadline = timeout_ms.map(|ms| std::time::Instant::now() + Duration::from_millis(ms));
        let mut state = entry.state.lock().unwrap();
        loop {
            if AgentEntry::is_settled(&state) {
                return Ok(state.descriptor.status_json());
            }
            match deadline {
                Some(deadline) => {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Ok(state.descriptor.status_json());
                    }
                    state = entry
                        .signal
                        .wait_timeout(state, deadline.saturating_duration_since(now))
                        .unwrap()
                        .0;
                }
                None => {
                    // A hibernating agent with no live thread and no alarm
                    // deadline can only be woken by a send — an unbounded
                    // join would hang; fail fast with guidance instead.
                    if state.descriptor.status == "hibernating"
                        && !state.thread_live
                        && state
                            .descriptor
                            .listen
                            .as_ref()
                            .is_none_or(|l| l.deadline.is_none())
                    {
                        return Err(format!(
                            "chidori.agents.join: `{to}` is hibernating on {:?} with no \
                             deadline; join it with a timeoutMs or send it a message",
                            state
                                .descriptor
                                .listen
                                .as_ref()
                                .map(|l| l.names.clone())
                                .unwrap_or_default()
                        ));
                    }
                    state = entry.signal.wait(state).unwrap();
                }
            }
        }
    }

    pub fn status(&self, parts: &AgentRuntimeParts, to: &str) -> Result<Value, String> {
        let entry = self.ensure_entry(parts, to)?;
        let state = entry.state.lock().unwrap();
        let mut status = state.descriptor.status_json();
        // The descriptor is durable state; `live` reports whether THIS
        // process's supervisor holds the lease and is actively driving the
        // agent, including setup and handoff. A supervisor thread can exist
        // while waiting for another process's lease, so thread existence is
        // tracked separately.
        status["live"] = json!(state.execution_active);
        Ok(status)
    }

    pub fn lookup(&self, parts: &AgentRuntimeParts, name: &str) -> Result<Value, String> {
        with_agent_name_lock(parts, name, |factory| {
            match self.resolve_entry_locked(parts, name, factory)? {
                Some(entry) => {
                    let state = entry.state.lock().unwrap();
                    Ok(json!({
                        "name": state.descriptor.name,
                        "runId": state.descriptor.run_id,
                        "status": state.descriptor.status,
                    }))
                }
                None => Ok(Value::Null),
            }
        })
    }

    /// Request a cooperative stop for the currently registered run. Active
    /// local execution finishes its current host call and stops at the next
    /// boundary. When this process is not the execution owner, the durable
    /// request remains pending until a matching wake or alarm rearms the run;
    /// the supervisor checks it before replay or effects. A hibernating run
    /// without a wake or alarm can therefore remain pending indefinitely.
    pub fn stop(&self, parts: &AgentRuntimeParts, to: &str) -> Result<Value, String> {
        self.stop_current_name(parts, to)
    }

    /// Stop the incarnation currently registered under `name`. The name lock
    /// keeps spawn/replacement from changing the target while the request is
    /// applied. If this process is actively executing the run, wait for it to
    /// settle; otherwise return a persisted `stopRequested` result without
    /// rewriting the descriptor. A hibernating run remains pending until a
    /// matching wake or alarm rearms it.
    pub fn stop_current_name(
        &self,
        parts: &AgentRuntimeParts,
        name: &str,
    ) -> Result<Value, String> {
        let mut wait_for_entry = None;
        let response = with_agent_name_lock(parts, name, |factory| {
            let entry = self
                .resolve_entry_locked(parts, name, factory)?
                .ok_or_else(|| format!("chidori.agents: unknown agent `{name}`"))?;
            let mut state = entry.state.lock().unwrap();
            if AgentEntry::is_settled(&state) {
                return Ok(state.descriptor.status_json());
            }

            let run_id = state.descriptor.run_id.clone();
            let store = factory.store_for(&run_id);
            persist_stop_request(store.as_ref(), &run_id)?;

            state.stop_requested = true;
            let local_execution = state.execution_active;
            entry.signal.notify_all();
            let mut response = state.descriptor.status_json();
            if !local_execution {
                // This process has no active execution owner to settle the
                // descriptor. A future matching wake/alarm observes the
                // durable request before replay/effects.
                response["stopRequested"] = json!(true);
            }
            drop(state);
            wait_for_entry = local_execution.then(|| entry.clone());
            Ok(response)
        })?;

        // Wait only when this process currently owns execution, and do so
        // after releasing the per-name lock so the worker can finish calls
        // that address agents by name. The captured entry preserves Y's
        // identity if the name is later reused.
        if let Some(entry) = wait_for_entry {
            return Ok(Self::wait_for_entry_stop(&entry, Duration::from_secs(60)));
        }
        Ok(response)
    }

    pub(crate) fn wait_for_entry_stop(entry: &Arc<AgentEntry>, timeout: Duration) -> Value {
        let deadline = Instant::now() + timeout;
        let mut state = entry.state.lock().unwrap();
        loop {
            if AgentEntry::is_settled(&state) {
                return state.descriptor.status_json();
            }
            let now = Instant::now();
            if now >= deadline {
                let mut response = state.descriptor.status_json();
                response["stopRequested"] = json!(true);
                return response;
            }
            state = entry
                .signal
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap()
                .0;
        }
    }

    /// Stop the incarnation captured by a detached-agent handle. A stale
    /// handle must not stop a newer registration that reused the same name.
    /// Without active local execution, persist the request and return
    /// `stopRequested`; a hibernating run remains pending until a matching
    /// wake or alarm rearms it.
    pub fn stop_incarnation(
        &self,
        parts: &AgentRuntimeParts,
        name: &str,
        run_id: &str,
    ) -> Result<Value, String> {
        if let Some(entry) = self.entries.lock().unwrap().get(name).cloned() {
            let matches_run = entry.state.lock().unwrap().descriptor.run_id == run_id;
            if matches_run {
                let store = entry.factory().store_for(run_id);
                let mut state = entry.state.lock().unwrap();
                if AgentEntry::is_settled(&state) {
                    return Ok(state.descriptor.status_json());
                }
                persist_stop_request(store.as_ref(), run_id)?;
                let local_execution = state.execution_active;
                state.stop_requested = true;
                entry.signal.notify_all();
                let mut response = state.descriptor.status_json();
                if !local_execution {
                    // This hub has no active execution owner to settle the
                    // descriptor. Persist the request without rewriting it;
                    // a future matching wake/alarm observes it before effects.
                    response["stopRequested"] = json!(true);
                }
                drop(state);
                if local_execution {
                    return Ok(Self::wait_for_entry_stop(&entry, Duration::from_secs(60)));
                }
                return Ok(response);
            }
        }

        let factory = RunStoreFactory::shared(&parts.run_base);
        factory
            .ensure_initialized()
            .map_err(|err| format!("chidori.agents.stop: initializing run store: {err:#}"))?;
        let store = factory.store_for(run_id);
        let bytes = store
            .get_blob(AGENT_DESCRIPTOR_FILE)
            .map_err(|err| format!("reading detached-agent run {run_id} descriptor: {err:#}"))?
            .ok_or_else(|| format!("detached-agent handle run {run_id} has no descriptor"))?;
        let descriptor: AgentDescriptor = serde_json::from_slice(&bytes)
            .map_err(|err| format!("parsing detached-agent run {run_id} descriptor: {err}"))?;
        if descriptor.name != name || descriptor.run_id != run_id {
            return Err(format!(
                "detached-agent handle identity mismatch for `{name}` run {run_id}"
            ));
        }
        if matches!(
            descriptor.status.as_str(),
            "completed" | "failed" | "stopped" | "paused"
        ) {
            return Ok(descriptor.status_json());
        }

        persist_stop_request(store.as_ref(), run_id)?;
        let mut response = descriptor.status_json();
        response["stopRequested"] = json!(true);
        Ok(response)
    }

    /// Registry snapshot for the server's `/agents` listing.
    pub fn list(&self, parts: &AgentRuntimeParts) -> Result<Vec<Value>, String> {
        RunStoreFactory::shared(&parts.run_base)
            .registry_list()
            .map_err(|err| format!("listing agents: {err}"))
    }

    /// Re-arm the fleet from the durable registry at process boot: install
    /// the parts, re-create entries, wake agents that were mid-run when the
    /// previous process died, and re-arm alarm deadlines. Returns how many
    /// agents were re-armed.
    pub fn rearm_from_registry(&self, parts: AgentRuntimeParts) -> Result<usize, String> {
        ensure_run_store_initialized(
            &parts.run_base,
            "chidori.agents.rearm: initializing run store",
        )?;
        let factory = RunStoreFactory::shared(&parts.run_base);
        let parts_for_entries = parts.clone();
        let values = factory
            .registry_list()
            .map_err(|err| format!("listing agent registry: {err}"))?;
        // Parse and cross-check the complete snapshot before hydration, entry
        // publication, or worker start. A damaged late row must not leave an
        // otherwise-valid prefix of the fleet running.
        let mut entries = Vec::with_capacity(values.len());
        for value in values {
            let registry_name = value
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| "detached-agent registry row has no string name".to_string())?;
            let registry_run_id = value.get("run_id").and_then(Value::as_str).ok_or_else(|| {
                format!("detached-agent registry row {registry_name:?} has no string run_id")
            })?;
            let descriptor_value = value.get("descriptor").cloned().ok_or_else(|| {
                format!("detached-agent registry row {registry_name:?} has no descriptor")
            })?;
            let descriptor: AgentDescriptor = serde_json::from_value(descriptor_value)
                .map_err(|error| {
                    format!(
                        "invalid descriptor for detached-agent registry row {registry_name:?}: {error}"
                    )
                })?;
            if descriptor.name != registry_name {
                return Err(format!(
                    "detached-agent registry row {registry_name:?} contains descriptor for {:?}",
                    descriptor.name
                ));
            }
            if descriptor.run_id != registry_run_id {
                return Err(format!(
                    "detached-agent registry row {registry_name:?} run_id {:?} does not match descriptor run_id {:?}",
                    registry_run_id, descriptor.run_id
                ));
            }
            entries.push(descriptor);
        }
        self.install_parts(parts);
        let mut rearmed = 0;
        for descriptor in entries {
            let name = descriptor.name.clone();
            match descriptor.status.as_str() {
                "running" | "hibernating" => {
                    factory.hydrate(&descriptor.run_id).map_err(|err| {
                        format!("hydrating detached-agent journal for {name}: {err:#}")
                    })?;
                    let entry = self.ensure_entry(&parts_for_entries, &name)?;
                    let completed_listen = if let Some(listen) = descriptor.listen.as_ref() {
                        let records = factory
                            .store_for(&descriptor.run_id)
                            .load_call_log()
                            .map_err(|err| {
                                format!("loading detached-agent journal for {name}: {err:#}")
                            })?
                            .unwrap_or_default();
                        listen_has_completed_record(listen, &records)
                    } else {
                        false
                    };
                    if completed_listen {
                        // The durable descriptor can lag a successful listen
                        // record when the process exits between inbox commit
                        // and publishing the live context. Journal evidence
                        // proves this listen already completed, so don't use
                        // its stale names to decide whether replay can start.
                        entry.state.lock().unwrap().descriptor.listen = None;
                    }
                    if descriptor.status == "running" || completed_listen {
                        // A mid-run descriptor always resumes. A hibernating
                        // descriptor resumes here only when the journal proves
                        // its persisted listener is already past the frontier.
                        self.start_thread(entry)?;
                    }
                    rearmed += 1;
                }
                _ => {}
            }
        }
        // The timer doubles as the fleet reconciler: it re-arms alarm
        // deadlines AND re-drives `running` entries whose driver died (e.g. a
        // wake that stood down on a contended lease). Always run it when a
        // fleet exists.
        if rearmed > 0 {
            self.ensure_timer();
        }
        Ok(rearmed)
    }

    /// Lazily start the alarm/reconciler timer thread. Scans entries once a
    /// second and (a) wakes hibernating agents whose alarm deadline passed,
    /// (b) re-drives `running` agents that have no worker thread — an agent
    /// whose previous driver died (or whose wake stood down on a contended
    /// lease) must not stay `running` forever with no one executing it. The
    /// re-drive is rate-limited by `updated_at` so a stand-down against a
    /// genuinely live remote driver retries cheaply, not in a hot loop. Poll
    /// granularity is one second — alarms are minute-scale scheduling, not
    /// precise timers.
    pub(crate) fn ensure_timer(&self) {
        let mut started = self.timer_started.lock().unwrap();
        if *started {
            return;
        }
        *started = true;
        std::thread::Builder::new()
            .name("chidori-agents-timer".to_string())
            .spawn(|| loop {
                std::thread::sleep(Duration::from_secs(1));
                let hub = hub();
                let due: Vec<Arc<AgentEntry>> = {
                    let entries = hub.entries.lock().unwrap();
                    entries
                        .values()
                        .filter(|entry| {
                            let state = entry.state.lock().unwrap();
                            let alarm_due = state.descriptor.status == "hibernating"
                                && !state.thread_live
                                && state
                                    .descriptor
                                    .listen
                                    .as_ref()
                                    .and_then(|l| l.deadline)
                                    .is_some_and(|deadline| deadline <= Utc::now());
                            let driverless = state.descriptor.status == "running"
                                && !state.thread_live
                                && !state.stop_requested
                                && state.descriptor.updated_at
                                    <= Utc::now() - chrono::Duration::seconds(30);
                            alarm_due || driverless
                        })
                        .cloned()
                        .collect()
                };
                for entry in due {
                    if let Err(err) = ensure_run_store_initialized(
                        &entry.parts.run_base,
                        "chidori.agents.alarm: initializing run store",
                    ) {
                        tracing::warn!(agent = %entry.name, error = %err, "skipping detached-agent wake because run store initialization failed");
                        continue;
                    }
                    {
                        let mut state = entry.state.lock().unwrap();
                        state.descriptor.status = "running".to_string();
                        state.descriptor.updated_at = Utc::now();
                    }
                    persist_descriptor(
                        &entry.factory(),
                        &entry.state.lock().unwrap().descriptor.clone(),
                    );
                    let _ = hub.start_thread(entry);
                }
            })
            .ok();
    }
}

/// Parsed `chidori.agents.spawn` options.
#[derive(Debug, Clone)]
pub struct SpawnOptions {
    pub name: Option<String>,
    pub restart: String,
    pub max_restarts: u32,
    pub backoff_ms: u64,
    /// Default model for the agent's prompts. Explicit spawn option, or
    /// filled with the spawner's resolved model by the binding.
    pub model: Option<String>,
}

impl SpawnOptions {
    pub(crate) fn parse(value: &Value) -> Result<Self, String> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(ref name) = name {
            if name.is_empty()
                || name
                    .chars()
                    .any(|c| !(c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'))
            {
                return Err(format!(
                    "chidori.agents.spawn: `{name}` is not a registrable agent name \
                     (allowed: ASCII letters, digits, `-`, `_`, `.`)"
                ));
            }
        }
        let restart = value
            .get("restart")
            .and_then(Value::as_str)
            .unwrap_or("resume")
            .to_string();
        if !matches!(restart.as_str(), "never" | "clean" | "resume") {
            return Err(format!(
                "chidori.agents.spawn: unknown restart strategy `{restart}` \
                 (expected \"never\", \"clean\", or \"resume\")"
            ));
        }
        Ok(Self {
            name,
            restart,
            max_restarts: value
                .get("maxRestarts")
                .and_then(Value::as_u64)
                .unwrap_or(3) as u32,
            backoff_ms: value.get("backoffMs").and_then(Value::as_u64).unwrap_or(0),
            model: value
                .get("model")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        })
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "restart": self.restart,
            "maxRestarts": self.max_restarts,
            "backoffMs": self.backoff_ms,
            "model": self.model,
        })
    }
}

// ---------------------------------------------------------------------------
// The supervision loop
// ---------------------------------------------------------------------------
