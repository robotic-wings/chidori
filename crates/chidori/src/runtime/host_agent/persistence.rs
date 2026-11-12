//! Descriptor persistence and the lease protocol that guarantees
//! single-writer ownership of an agent's run across processes.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;

use crate::runtime::store::{RunStore, RunStoreFactory};

use super::*;

/// Persist `descriptor` to the registry (and the run dir's `agent.json`),
/// the durable record a fresh process re-arms the fleet from.
pub(crate) fn persist_descriptor(factory: &RunStoreFactory, descriptor: &AgentDescriptor) {
    let value = serde_json::to_value(descriptor).unwrap_or_default();
    if let Err(err) = factory.registry_put(&descriptor.name, &descriptor.run_id, &value) {
        tracing::warn!(agent = %descriptor.name, error = %err, "persisting agent registry entry");
    }
    let store = factory.store_for(&descriptor.run_id);
    if let Ok(bytes) = serde_json::to_vec_pretty(descriptor) {
        let _ = store.put_blob(AGENT_DESCRIPTOR_FILE, &bytes);
    }
}

/// Persist a new spawn's run descriptor before publishing its name in the
/// registry. Unlike lifecycle updates, spawn must observe persistence errors
/// so it does not discard a predecessor's mailbox after a partial setup.
pub(crate) fn persist_spawn_descriptor(
    factory: &RunStoreFactory,
    descriptor: &AgentDescriptor,
) -> Result<(), String> {
    let value = serde_json::to_value(descriptor)
        .map_err(|err| format!("serializing replacement descriptor: {err}"))?;
    let bytes = serde_json::to_vec_pretty(descriptor)
        .map_err(|err| format!("serializing replacement agent descriptor: {err}"))?;
    factory
        .store_for(&descriptor.run_id)
        .put_blob(AGENT_DESCRIPTOR_FILE, &bytes)
        .map_err(|err| format!("persisting replacement agent descriptor: {err:#}"))?;
    factory
        .registry_put(&descriptor.name, &descriptor.run_id, &value)
        .map_err(|err| format!("publishing replacement registry descriptor: {err:#}"))?;
    Ok(())
}

/// Treat a complete local registry rename as committed for this run base.
/// Registry mirrors are updated after that rename, so a mirror failure cannot
/// be rolled back safely; continue with the locally published run when the
/// exact replacement descriptor is visible. This does not imply remote
/// mirror completion or cross-store atomicity.
pub(crate) fn persist_spawn_descriptor_with_local_commit(
    factory: &RunStoreFactory,
    descriptor: &AgentDescriptor,
) -> Result<(), String> {
    match persist_spawn_descriptor(factory, descriptor) {
        Ok(()) => Ok(()),
        Err(error) => {
            let local_commit_is_visible = factory
                .registry_get(&descriptor.name)
                .ok()
                .flatten()
                .is_some_and(|entry| {
                    entry.get("run_id").and_then(Value::as_str) == Some(&descriptor.run_id)
                        && entry
                            .get("descriptor")
                            .and_then(|value| value.get("status"))
                            .and_then(Value::as_str)
                            == Some(descriptor.status.as_str())
                });
            if local_commit_is_visible {
                tracing::warn!(
                    agent = %descriptor.name,
                    run_id = %descriptor.run_id,
                    error,
                    "local spawn registry committed but a configured mirror update failed; keeping the local run authoritative"
                );
                Ok(())
            } else {
                Err(error)
            }
        }
    }
}

pub(crate) fn persist_descriptor_for_lease(
    factory: &RunStoreFactory,
    descriptor: &AgentDescriptor,
    lease: Option<&crate::runtime::store::FencedLease>,
) -> anyhow::Result<()> {
    let store = factory.store_for(&descriptor.run_id);
    if let Some(lease) = lease.filter(|_| store.supports_lease_fencing()) {
        let fenced = crate::runtime::store::LeaseFencedRunStore::new_with_token(
            store,
            lease.lease().clone(),
            lease.token().clone(),
        )?;
        if let Ok(bytes) = serde_json::to_vec_pretty(descriptor) {
            fenced.put_blob(AGENT_DESCRIPTOR_FILE, &bytes)?;
        }
        factory.registry_put_fenced_with_token(
            lease.lease(),
            lease.token(),
            &descriptor.name,
            &descriptor.run_id,
            &serde_json::to_value(descriptor)?,
        )?;
        Ok(())
    } else {
        persist_descriptor(factory, descriptor);
        Ok(())
    }
}

/// The process-stable lease owner id: one per OS process, so every agent this
/// process drives is leased under the same identity.
pub(crate) fn process_lease_owner() -> &'static str {
    static OWNER: OnceLock<String> = OnceLock::new();
    OWNER.get_or_init(|| format!("chidori-{}", uuid::Uuid::new_v4()))
}

pub(crate) fn detached_lease_ttl() -> chrono::Duration {
    #[cfg(test)]
    if let Some(milliseconds) = std::env::var("CHIDORI_TEST_SQLITE_LEASE_TTL_MS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
    {
        return chrono::Duration::milliseconds(milliseconds.max(30));
    }
    chrono::Duration::minutes(5)
}

/// Renew a SQLite tenure independently of agent execution, which may be
/// blocked in a long host call. Every renewal carries the generation token;
/// an expired or displaced owner cannot renew itself.
pub(crate) struct LeaseHeartbeat {
    stop: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl LeaseHeartbeat {
    pub(crate) fn start(
        store: Arc<dyn crate::runtime::store::RunStore>,
        lease: crate::runtime::store::FencedLease,
        ttl: chrono::Duration,
    ) -> Self {
        let (stop, stopped) = std::sync::mpsc::channel();
        let interval =
            (ttl.to_std().unwrap_or(Duration::from_secs(1)) / 3).max(Duration::from_millis(10));
        let thread = std::thread::spawn(move || loop {
            match stopped.recv_timeout(interval) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            match store.renew_lease_with_token(lease.lease(), lease.token(), ttl) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    tracing::warn!(
                        owner = %lease.lease().owner,
                        generation = lease.token().generation_for_log(),
                        "SQLite run lease heartbeat was fenced"
                    );
                    break;
                }
                Err(err) => {
                    tracing::error!(
                        owner = %lease.lease().owner,
                        generation = lease.token().generation_for_log(),
                        error = %err,
                        "SQLite run lease heartbeat failed"
                    );
                    break;
                }
            }
        });
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for LeaseHeartbeat {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Release the agent run's lease — called when the agent stops executing here
/// (hibernate or settle), so any process may drive the next wake.
pub(crate) fn release_agent_lease(
    entry: &Arc<AgentEntry>,
    lease: Option<&crate::runtime::store::FencedLease>,
) {
    let run_id = entry.state.lock().unwrap().descriptor.run_id.clone();
    let store = entry.factory().store_for(&run_id);
    if let Some(lease) = lease.filter(|_| store.supports_lease_fencing()) {
        let _ = crate::runtime::store::release_lease_with_token(
            store.as_ref(),
            lease.lease(),
            lease.token(),
        );
    } else {
        let _ = crate::runtime::store::release_lease(store.as_ref(), process_lease_owner());
    }
}

/// Keep a worker retryable when durable state needed for recovery cannot be
/// read. The state lock remains held while the descriptor is persisted,
/// preserving state -> store lock order and preventing a sender from
/// observing a half-written handoff.
pub(crate) fn defer_after_durable_read_error(
    entry: &Arc<AgentEntry>,
    listen: Option<ListenState>,
    lease: Option<&crate::runtime::store::FencedLease>,
    artifact: &str,
    error: &str,
) {
    let persisted = {
        let mut state = entry.state.lock().unwrap();
        state.descriptor.status = "running".to_string();
        if let Some(listen) = listen {
            state.descriptor.listen = Some(listen);
        }
        state.descriptor.updated_at = Utc::now();
        state.execution_active = false;
        let descriptor = state.descriptor.clone();
        persist_descriptor_for_lease(&entry.factory(), &descriptor, lease)
    };
    if let Err(persist_error) = persisted {
        tracing::error!(
            agent = %entry.name,
            error = %persist_error,
            "persisting retryable detached-agent state after durable read failure"
        );
    }
    release_agent_lease(entry, lease);
    tracing::warn!(
        agent = %entry.name,
        artifact,
        error,
        "detached-agent durable state is unreadable; leaving the run driverless for retry"
    );
    entry.signal.notify_all();
}
