//! The per-agent supervision loop: resume-by-replay the module, settle the
//! iteration (completed/failed/stopped terminal, signal listen → hibernate),
//! and the listen-state extraction that drives delivery injection.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};

use crate::policy::PolicyCache;
use crate::runtime::call_log::CallRecord;
use crate::runtime::context::{HostCallPreflight, PendingSignal, RuntimeContext};
use crate::runtime::errors::RunInterrupt;
use crate::runtime::host_actor::strip_crash_frontier;
use crate::runtime::host_core;
use crate::runtime::snapshot::RuntimePolicy;
use crate::runtime::typescript::bindings::HostBindingBackend;

use super::*;

/// One supervision pass for a detached agent: run the module under
/// resume-by-replay, then settle — completed/failed/stopped are terminal,
/// a signal wait persists its listen state and HIBERNATES (the thread exits;
/// deliveries and alarm deadlines re-enter), a failure consumes the restart
/// budget per the spawn's strategy.
pub(crate) fn supervise_detached(hub: &DetachedAgentHub, entry: &Arc<AgentEntry>) {
    let parts = entry.parts.clone();
    let factory = entry.factory();

    // Single-writer ownership: take the agent run's lease before executing.
    // Two processes sharing a durable mirror (or a filesystem) cannot both
    // drive the same agent — the loser backs off and leaves the run to the
    // live holder; an expired lease (a dead node) transfers on the next wake.
    let lease_owner = process_lease_owner();
    let lease_ttl = detached_lease_ttl();
    let (lease_store, active_lease) = {
        let run_id = entry.state.lock().unwrap().descriptor.run_id.clone();
        let store = factory.store_for(&run_id);
        // Contended lease: wait out the holder's TTL instead of standing down
        // for good. A dead process never renews, so its lease expires and this
        // wake takes over; a live holder keeps pushing `expires_at` forward —
        // THAT is the genuinely-driven-elsewhere case worth yielding to.
        // (Standing down permanently on a not-yet-expired stale lease was how
        // an agent could wedge forever as `running` with no worker anywhere.)
        let lease = loop {
            #[cfg(test)]
            tests::lease_wait_test_gate(&entry.name, tests::LeaseWaitGatePoint::BeforeAcquire);
            #[cfg(test)]
            let acquisition = if tests::fail_next_lease_acquire(&entry.name) {
                Err(anyhow::anyhow!(
                    "injected detached lease acquisition failure"
                ))
            } else {
                crate::runtime::store::acquire_lease_with_token(
                    store.as_ref(),
                    lease_owner,
                    lease_ttl,
                )
            };
            #[cfg(not(test))]
            let acquisition = crate::runtime::store::acquire_lease_with_token(
                store.as_ref(),
                lease_owner,
                lease_ttl,
            );
            match acquisition {
                Ok(Ok(lease)) => {
                    #[cfg(test)]
                    tests::pause_stale_owner_after_lease_acquired(&entry.name);
                    break Some(lease);
                }
                Ok(Err(holder)) => {
                    #[cfg(test)]
                    tests::lease_wait_test_gate(
                        &entry.name,
                        tests::LeaseWaitGatePoint::ContendedBeforeWait,
                    );
                    #[cfg(test)]
                    if tests::stale_owner_retry_after_contention(&entry.name) {
                        continue;
                    }
                    let previous_expiry = holder.expires_at;
                    tracing::warn!(
                        agent = %entry.name,
                        holder = %holder.owner,
                        expires_at = %holder.expires_at,
                        "detached agent is leased to another process; waiting for the lease \
                         to expire or be renewed"
                    );
                    // Even after a local stop request, keep this supervisor
                    // alive until it can take the lease and settle the run.
                    // Returning here strands a persisted stop as `running`:
                    // the send path suppresses new wakes for stopped entries.
                    loop {
                        let remaining = previous_expiry - Utc::now();
                        if remaining <= chrono::Duration::zero() {
                            break;
                        }
                        let slice = remaining
                            .to_std()
                            .unwrap_or(Duration::from_secs(1))
                            .min(Duration::from_secs(1));
                        std::thread::sleep(slice);
                    }
                    // Renewed while we waited → a live driver owns this agent.
                    if let Ok(Some(bytes)) = store.get_blob(crate::runtime::store::LEASE_FILE) {
                        if let Ok(current) =
                            serde_json::from_slice::<crate::runtime::store::RunLease>(&bytes)
                        {
                            if current.owner != lease_owner && current.expires_at > previous_expiry
                            {
                                tracing::info!(
                                    agent = %entry.name,
                                    holder = %current.owner,
                                    "lease was renewed by a live driver; standing down"
                                );
                                return;
                            }
                        }
                    }
                    // Expired and unrenewed (dead node): loop re-acquires.
                }
                Err(err) => {
                    tracing::warn!(agent = %entry.name, error = %err, "acquiring agent lease");
                    break None;
                }
            }
        };
        (store, lease)
    };
    if lease_store.supports_lease_fencing() && active_lease.is_none() {
        tracing::warn!(agent = %entry.name, "SQLite run lease was not acquired; refusing to execute without ownership");
        return;
    }
    {
        let mut state = entry.state.lock().unwrap();
        state.execution_active = true;
    }
    #[cfg(test)]
    tests::pause_after_alarm_wake_lease_before_inbox_read(&entry.name);
    let _heartbeat = active_lease
        .as_ref()
        .filter(|_| lease_store.supports_lease_fencing())
        .map(|lease| LeaseHeartbeat::start(lease_store.clone(), lease.clone(), lease_ttl));
    let settle_run = |status, output, error| {
        entry.state.lock().unwrap().execution_active = false;
        settle(hub, entry, status, output, error, active_lease.as_ref())
    };
    let hibernate_run = |listen| {
        entry.state.lock().unwrap().execution_active = false;
        finish_hibernate_transition(entry, hibernate(hub, entry, listen, active_lease.as_ref()))
    };

    loop {
        // SQLite runs use the independent heartbeat above during long host
        // calls; check the tenure again before beginning another iteration.
        // Advisory backends retain their existing per-iteration renewal.
        {
            let run_id = entry.state.lock().unwrap().descriptor.run_id.clone();
            if active_lease.is_none() || !lease_store.supports_lease_fencing() {
                let store = factory.store_for(&run_id);
                if let Ok(Err(holder)) = crate::runtime::store::acquire_lease_with_token(
                    store.as_ref(),
                    lease_owner,
                    lease_ttl,
                ) {
                    tracing::warn!(
                        agent = %entry.name,
                        holder = %holder.owner,
                        "lost the agent lease mid-run; standing down"
                    );
                    return;
                }
            } else if let Some(lease) = active_lease.as_ref() {
                if let Err(err) = lease_store.check_lease_with_token(lease.lease(), lease.token()) {
                    tracing::warn!(agent = %entry.name, error = %err, "lost the agent lease mid-run");
                    return;
                }
            }
        }
        let (descriptor, stop_requested) = {
            let state = entry.state.lock().unwrap();
            (state.descriptor.clone(), state.stop_requested)
        };
        if stop_requested {
            settle_run("stopped", None, None);
            return;
        }

        let run_id = descriptor.run_id.clone();
        let run_dir = parts.run_base.join(&run_id);
        let store = factory.store_for(&run_id);
        match stop_request_matches_run(store.as_ref(), &run_id) {
            Ok(true) => {
                entry.state.lock().unwrap().stop_requested = true;
                settle_run("stopped", None, None);
                return;
            }
            Ok(false) => {}
            Err(err) => {
                settle_run(
                    "failed",
                    None,
                    Some(format!("checking detached-agent stop request: {err}")),
                );
                return;
            }
        }
        let write_store: Arc<dyn crate::runtime::store::RunStore> =
            if store.supports_lease_fencing() {
                if let Some(lease) = active_lease.as_ref() {
                    match crate::runtime::store::LeaseFencedRunStore::new_with_token(
                        store.clone(),
                        lease.lease().clone(),
                        lease.token().clone(),
                    ) {
                        Ok(store) => Arc::new(store),
                        Err(err) => {
                            settle_run("failed", None, Some(err.to_string()));
                            return;
                        }
                    }
                } else {
                    store.clone()
                }
            } else {
                store.clone()
            };

        // The accumulated journal (empty on first run), plus a synthetic
        // timeout-sentinel record when this wake is an expired alarm/timeout
        // rather than a matching delivery.
        let mut replay: Vec<CallRecord> = match store.load_call_log() {
            Ok(records) => records.unwrap_or_default(),
            Err(err) => {
                settle_run(
                    "failed",
                    None,
                    Some(format!("loading detached-agent journal: {err:#}")),
                );
                return;
            }
        };
        let inbox = match load_detached_signal_inbox(store.as_ref(), &run_id) {
            Ok(inbox) => inbox,
            Err(err) => {
                defer_after_durable_read_error(
                    entry,
                    descriptor.listen.clone(),
                    active_lease.as_ref(),
                    "signal inbox",
                    &err,
                );
                return;
            }
        };
        #[cfg(test)]
        tests::pause_after_supervisor_inbox_snapshot(&entry.name);
        let consumed_delivery_seq = std::cell::Cell::new(None::<u64>);
        if let Some(listen) = descriptor.listen.clone() {
            let matched = inbox
                .iter()
                .any(|queued| listen.names.iter().any(|n| n == &queued.name));
            let expired = listen
                .deadline
                .is_some_and(|deadline| deadline <= Utc::now());
            if !matched && expired {
                // Woken by the deadline: resolve the listen point with the
                // timeout sentinel — the alarm fired.
                replay.retain(|r| r.seq != listen.seq);
                replay.push(CallRecord {
                    seq: listen.seq,
                    parent_seq: None,
                    function: listen.function.clone(),
                    args: if listen.function == "signal_any" {
                        json!({ "names": listen.names })
                    } else {
                        json!({ "name": listen.names[0] })
                    },
                    result: host_core::signal_timeout_sentinel(&listen.names),
                    duration_ms: 0,
                    token_usage: None,
                    timestamp: Utc::now(),
                    error: None,
                });
            } else if !matched {
                // Spuriously started with nothing to do — go back to sleep.
                match hibernate_run(listen.clone()) {
                    Ok(true) => return,
                    Ok(false) => continue,
                    Err(HibernateError::RetryableRead { artifact, error }) => {
                        defer_after_durable_read_error(
                            entry,
                            Some(listen),
                            active_lease.as_ref(),
                            artifact,
                            &error,
                        );
                        return;
                    }
                    Err(HibernateError::Other(err)) => {
                        settle_run(
                            "failed",
                            None,
                            Some(format!(
                                "checking detached-agent inbox before hibernate: {err}"
                            )),
                        );
                        return;
                    }
                }
            }
        }

        let Ok(policy) = RuntimePolicy::from_env_for_durable_run(&run_id) else {
            settle_run("failed", None, Some("building runtime policy".into()));
            return;
        };
        let resolved = match resolve_wake_source(&parts, store.as_ref(), &descriptor) {
            Ok(resolved) => resolved,
            Err(err) => {
                settle_run("failed", None, Some(err));
                return;
            }
        };
        let source_text = match std::fs::read_to_string(&resolved) {
            Ok(source) => source,
            Err(err) => {
                settle_run(
                    "failed",
                    None,
                    Some(format!("reading {}: {err}", resolved.display())),
                );
                return;
            }
        };
        // Build the replay context only after the authoritative inbox refresh.
        // Timeout promise reconciliation mutates durable host-promise state,
        // so it must follow this fallible read.
        let context_result: Result<RuntimeContext, HibernateError> = {
            let mut state = entry.state.lock().unwrap();
            // A store-only send can append during source loading. Keep the
            // state lock across the authoritative read, reconciliation, and
            // context publication so senders use the live context afterwards.
            (|| {
                let inbox =
                    load_detached_signal_inbox(store.as_ref(), &run_id).map_err(|error| {
                        HibernateError::RetryableRead {
                            artifact: "signal inbox",
                            error,
                        }
                    })?;
                let snapshot_store =
                    crate::runtime::snapshot::SnapshotStore::with_store(&run_dir, store.clone());
                let snapshot_manifest =
                    snapshot_store.load_manifest_if_present().map_err(|err| {
                        HibernateError::RetryableRead {
                            artifact: "snapshot manifest",
                            error: format!("loading detached-agent snapshot manifest: {err:#}"),
                        }
                    })?;
                let fresh_first_run =
                    descriptor.listen.is_none() && replay.is_empty() && descriptor.restarts == 0;
                let vfs = match snapshot_manifest {
                    Some(manifest) => manifest.vfs,
                    None if fresh_first_run => crate::runtime::context::vfs_from_seed_env(),
                    None => {
                        return Err(HibernateError::RetryableRead {
                            artifact: "snapshot manifest",
                            error: format!(
                                "loading detached-agent snapshot manifest: reading {}: not found",
                                run_dir
                                    .join(crate::runtime::snapshot::SNAPSHOT_MANIFEST_FILE)
                                    .display()
                            ),
                        })
                    }
                };
                if let Some(listen) = descriptor.listen.as_ref() {
                    // Consume the matched delivery HERE rather than leaving it
                    // for re-execution's take_queued_signal: inject the exact
                    // CallRecord the live consume would write (lowest
                    // delivery_seq across the name set wins, mirroring
                    // take_queued_signal_any) and durably acknowledge the
                    // entry in the same critical section. Both resume paths
                    // then agree — replay consumes via try_replay_checked,
                    // and a mainline VM image validates its frontier-plus-one
                    // delivery (mainline_image::accept) and resolves the
                    // parked listen from this record.
                    let matched = inbox
                        .iter()
                        .filter(|queued| listen.names.iter().any(|name| name == &queued.name))
                        .min_by_key(|queued| queued.delivery_seq)
                        .cloned();
                    if let Some(queued) = matched.as_ref() {
                        // The in-memory snapshot handed to the context must
                        // stay in step with the durable acknowledgement: a
                        // later listen point must not re-consume this entry
                        // after the delivery record already answered it.
                        consumed_delivery_seq.set(Some(queued.delivery_seq));
                    }
                    if let Some(queued) = matched {
                        ack_detached_signal_delivery(store.as_ref(), &queued).map_err(|err| {
                            HibernateError::Other(format!(
                                "acknowledging detached-agent signal delivery: {err}"
                            ))
                        })?;
                        replay.retain(|record| {
                            !(record.seq == listen.seq
                                && record.function == listen.function
                                && record.result.get("timedOut") == Some(&Value::Bool(true)))
                        });
                        let delivery = CallRecord {
                            seq: listen.seq,
                            parent_seq: None,
                            function: listen.function.clone(),
                            args: if listen.function == "signal_any" {
                                json!({ "names": listen.names })
                            } else {
                                json!({ "name": listen.names[0] })
                            },
                            result: json!({
                                "name": queued.name,
                                "payload": queued.payload,
                                "from": queued.from,
                            }),
                            duration_ms: 0,
                            token_usage: None,
                            timestamp: Utc::now(),
                            error: None,
                        };
                        // The record must be durable NOW: the replay consume
                        // path (try_replay_checked) returns early without
                        // re-recording, so a transient-only injection would
                        // vanish from the next wake's journal and the
                        // re-executed listen would pause again on history it
                        // already consumed.
                        write_store.append_record(&delivery).map_err(|err| {
                            HibernateError::Other(format!(
                                "appending detached-agent signal delivery record: {err}"
                            ))
                        })?;
                        replay.push(delivery);
                    }
                }
                crate::runtime::snapshot::reconcile_detached_signal_timeout_promises(
                    write_store.as_ref(),
                    &replay,
                )
                .map_err(|err| {
                    HibernateError::Other(format!(
                        "reconciling detached-agent alarm promises: {err:#}"
                    ))
                })?;
                let host_promises = crate::runtime::snapshot::load_host_promise_records(
                    store.as_ref(),
                )
                .map_err(|err| {
                    HibernateError::Other(format!("loading detached-agent host promises: {err:#}"))
                })?;
                let consumed_seq = consumed_delivery_seq.take();
                let ctx = RuntimeContext::with_replay_host_promises_vfs_and_signals(
                    replay,
                    host_promises,
                    vfs,
                    match consumed_seq {
                        Some(seq) => inbox
                            .into_iter()
                            .filter(|entry| entry.delivery_seq != seq)
                            .collect::<Vec<_>>(),
                        None => inbox,
                    },
                );
                ctx.set_run_id(run_id.clone());
                ctx.set_input_mode(crate::runtime::context::InputMode::Pause);
                ctx.enable_persistence_with_store(run_dir.clone(), write_store.clone());
                let stop_store = store.clone();
                let stop_entry = entry.clone();
                let stop_run_id = run_id.clone();
                ctx.set_host_call_preflight(HostCallPreflight::new(move || {
                    match stop_request_matches_run(stop_store.as_ref(), &stop_run_id) {
                        Ok(true) => {
                            stop_entry.state.lock().unwrap().stop_requested = true;
                            anyhow::bail!("detached agent stop requested")
                        }
                        Ok(false) => Ok(()),
                        Err(err) => {
                            anyhow::bail!(
                                "checking detached-agent stop request for run {}: {err}",
                                stop_run_id,
                            )
                        }
                    }
                }));
                if let Some(ref model) = descriptor.model {
                    ctx.set_default_model(model.clone());
                }
                if ctx.workspace_root().is_none() {
                    if let Some(project_root) = parts.run_base.parent().and_then(|p| p.parent()) {
                        ctx.set_workspace_root(project_root.to_path_buf());
                    }
                }
                state.descriptor.listen = None;
                state.live_ctx = Some(ctx.clone());
                Ok(ctx)
            })()
        };
        let ctx = match context_result {
            Ok(ctx) => ctx,
            Err(err) => {
                match err {
                    HibernateError::RetryableRead { artifact, error } => {
                        defer_after_durable_read_error(
                            entry,
                            descriptor.listen.clone(),
                            active_lease.as_ref(),
                            artifact,
                            &error,
                        );
                    }
                    HibernateError::Other(err) => settle_run("failed", None, Some(err)),
                }
                return;
            }
        };
        // The same durable scaffold a run gets from the engine: manifest +
        // pending + checkpoint at every host-operation safepoint, so a crash
        // mid-iteration leaves a resumable artifact.
        crate::runtime::engine::install_journal_scaffold_safepoints(
            &parts.run_base,
            &run_id,
            &resolved,
            &source_text,
            &policy,
            &ctx,
        );

        let backend = HostBindingBackend::for_runtime(
            ctx.clone(),
            parts.providers.clone(),
            parts.template_engine.clone(),
            parts.tokio_rt.clone(),
            parts.policy.clone(),
            Arc::new(Mutex::new(PolicyCache::default())),
            policy.clone(),
            parts.tools.clone(),
            parts.mcp.clone(),
        );
        #[cfg(test)]
        tests::pause_after_supervisor_publishes_live_context(&entry.name);
        let result =
            crate::runtime::rust_engine::run_agent_file(&resolved, &descriptor.input, &backend);
        {
            let mut state = entry.state.lock().unwrap();
            state.live_ctx = None;
        }
        #[cfg(test)]
        tests::pause_after_worker_clears_live_context(&entry.name);
        ctx.clear_event_sender();
        drop(backend);
        let _ = crate::runtime::engine::persist_journal_scaffold(
            &parts.run_base,
            &run_id,
            &resolved,
            &source_text,
            &policy,
            &ctx,
        );

        if entry.state.lock().unwrap().stop_requested {
            settle_run("stopped", None, None);
            return;
        }

        match settle_iteration(result, &ctx) {
            IterationEnd::Completed(output) => {
                if let Some(store) = ctx.store() {
                    let _ = store.put_blob(
                        "output.json",
                        serde_json::to_string_pretty(&output)
                            .unwrap_or_default()
                            .as_bytes(),
                    );
                }
                settle_run("completed", Some(output), None);
                return;
            }
            IterationEnd::Parked(prompt) => {
                settle_run("paused", None, prompt.map(|p| format!("paused: {p}")));
                return;
            }
            IterationEnd::WaitSignal(pending) => {
                // Re-check the durable inbox before parking: a delivery that
                // raced in during the unwind must not be slept through.
                let names = pending.listen_names();
                let raced = ctx
                    .signal_inbox()
                    .iter()
                    .any(|queued| names.iter().any(|n| n == &queued.name));
                let listen = listen_state(&ctx, &pending);
                if raced {
                    continue;
                }
                match hibernate_run(listen.clone()) {
                    Ok(true) => return,
                    Ok(false) => continue,
                    Err(HibernateError::RetryableRead { artifact, error }) => {
                        defer_after_durable_read_error(
                            entry,
                            Some(listen),
                            active_lease.as_ref(),
                            artifact,
                            &error,
                        );
                        return;
                    }
                    Err(HibernateError::Other(err)) => {
                        settle_run(
                            "failed",
                            None,
                            Some(format!(
                                "checking detached-agent inbox before hibernate: {err}"
                            )),
                        );
                        return;
                    }
                }
            }
            IterationEnd::Failed(message) => {
                let stop = entry.state.lock().unwrap().stop_requested;
                if stop {
                    settle_run("stopped", None, None);
                    return;
                }
                let (restarts, budget_left) = {
                    let mut state = entry.state.lock().unwrap();
                    let left = descriptor.restart != "never"
                        && state.descriptor.restarts < descriptor.max_restarts;
                    if left {
                        state.descriptor.restarts += 1;
                    }
                    (state.descriptor.restarts, left)
                };
                if !budget_left {
                    settle_run("failed", None, Some(message));
                    return;
                }
                if descriptor.backoff_ms > 0 {
                    let backoff = descriptor
                        .backoff_ms
                        .saturating_mul(1u64 << (restarts - 1).min(16));
                    let deadline = std::time::Instant::now() + Duration::from_millis(backoff);
                    let mut state = entry.state.lock().unwrap();
                    while !state.stop_requested {
                        let now = std::time::Instant::now();
                        if now >= deadline {
                            break;
                        }
                        state = entry
                            .signal
                            .wait_timeout(state, deadline.saturating_duration_since(now))
                            .unwrap()
                            .0;
                    }
                }
                // Rewrite the durable journal per the restart strategy: a
                // `resume` restart strips the crash frontier so completed work
                // replays from cache; `clean` starts over.
                let records = ctx.call_log().into_records();
                let rewritten = match descriptor.restart.as_str() {
                    "clean" => Vec::new(),
                    _ => strip_crash_frontier(records),
                };
                let _ = write_store.write_call_log(&rewritten);
                if descriptor.restart == "clean" {
                    let _ =
                        write_store.delete_blob(crate::runtime::snapshot::HOST_PROMISE_TABLE_FILE);
                    let _ = write_store
                        .delete_blob(crate::runtime::snapshot::PENDING_HOST_OPERATION_FILE);
                    // Per-op promise blobs are part of the table; a clean
                    // restart retires them too or the union loader would
                    // resurrect the wiped state.
                    if let Ok(keys) = write_store.list_blobs() {
                        for key in keys {
                            if key.starts_with(crate::runtime::snapshot::HOST_PROMISE_EVENTS_PREFIX)
                            {
                                let _ = write_store.delete_blob(&key);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The pending listen call's durable wake state, including the alarm deadline
/// when the listen carries a timeout.
pub(crate) fn listen_state(ctx: &RuntimeContext, pending: &PendingSignal) -> ListenState {
    let names = pending.listen_names();
    let function = ctx
        .pending_host_operation(pending.id)
        .and_then(|op| op.function)
        .unwrap_or_else(|| {
            if names.len() > 1 {
                "signal_any"
            } else {
                "signal"
            }
            .to_string()
        });
    ListenState {
        names,
        seq: pending.seq,
        function,
        deadline: pending
            .timeout_ms
            .map(|ms| Utc::now() + chrono::Duration::milliseconds(ms as i64)),
    }
}

/// A completed journal record is authoritative when a crash left the
/// descriptor pointing at the listen that produced it. This lets rearm
/// distinguish an already-consumed listener from a genuinely pending hibernate
/// without inferring state from a descriptor status alone.
pub(crate) fn listen_has_completed_record(listen: &ListenState, records: &[CallRecord]) -> bool {
    let expected_args = match listen.function.as_str() {
        "signal" if listen.names.len() == 1 => json!({ "name": listen.names[0] }),
        "signal_any" => json!({ "names": listen.names }),
        _ => return false,
    };

    records.iter().any(|record| {
        record.seq == listen.seq
            && record.function == listen.function
            && record.error.is_none()
            && record.args == expected_args
    })
}

/// Persist the hibernation handoff while excluding senders. Returns `Ok(false)`
/// when a matching durable message arrived before the transition; the caller
/// should continue the supervisor loop and consume it instead of sleeping.
pub(crate) fn hibernate(
    hub: &DetachedAgentHub,
    entry: &Arc<AgentEntry>,
    listen: ListenState,
    lease: Option<&crate::runtime::store::FencedLease>,
) -> Result<bool, HibernateError> {
    let has_deadline = listen.deadline.is_some();
    let factory = entry.factory();
    {
        let mut state = entry.state.lock().unwrap();
        let run_id = state.descriptor.run_id.clone();
        let store = factory.store_for(&run_id);
        let owned_store: Arc<dyn crate::runtime::store::RunStore> =
            if let Some(lease) = lease.filter(|_| store.supports_lease_fencing()) {
                match crate::runtime::store::LeaseFencedRunStore::new_with_token(
                    store.clone(),
                    lease.lease().clone(),
                    lease.token().clone(),
                ) {
                    Ok(fenced) => Arc::new(fenced),
                    Err(err) => {
                        return Err(HibernateError::Other(format!(
                            "creating lease-fenced store: {err}"
                        )))
                    }
                }
            } else {
                store.clone()
            };
        if let Err(err) = owned_store.check_ownership() {
            return Err(HibernateError::Other(format!(
                "detached-agent lease ownership lost before hibernate: {err}"
            )));
        }
        let inbox = load_detached_signal_inbox(store.as_ref(), &run_id).map_err(|error| {
            HibernateError::RetryableRead {
                artifact: "signal inbox",
                error,
            }
        })?;
        if inbox
            .iter()
            .any(|queued| listen.names.iter().any(|name| name == &queued.name))
        {
            // Keep the pending listen in memory for the next supervisor pass.
            // It will observe the queued payload and clear the descriptor's
            // listen state as it begins replay.
            state.descriptor.listen = Some(listen);
            return Ok(false);
        }

        state.descriptor.status = "hibernating".to_string();
        state.descriptor.listen = Some(listen);
        state.descriptor.updated_at = Utc::now();
        let descriptor = state.descriptor.clone();

        // Keep senders behind AgentState until both durable transitions have
        // completed. Otherwise a sender can persist `running` and start a new
        // worker, then this stale hibernate write or lease release can undo
        // that wake.
        if let Err(err) = persist_descriptor_for_lease(&factory, &descriptor, lease) {
            return Err(HibernateError::Other(format!(
                "persisting detached-agent hibernation: {err}"
            )));
        }
        let released = if let Some(lease) = lease.filter(|_| store.supports_lease_fencing()) {
            crate::runtime::store::release_lease_with_token(
                store.as_ref(),
                lease.lease(),
                lease.token(),
            )
        } else {
            crate::runtime::store::release_lease(store.as_ref(), process_lease_owner())
        };
        if let Err(err) = released {
            tracing::warn!(agent = %entry.name, error = %err, "releasing detached-agent lease on hibernate");
        }
    }
    if has_deadline {
        hub.ensure_timer();
    }
    entry.signal.notify_all();
    Ok(true)
}

pub(crate) enum HibernateError {
    RetryableRead {
        artifact: &'static str,
        error: String,
    },
    Other(String),
}

/// A false hibernate result means a matching durable signal raced with the
/// transition. The supervisor still owns the lease and will continue driving
/// the agent, so restore the public execution state before the next pass.
pub(crate) fn finish_hibernate_transition(
    entry: &Arc<AgentEntry>,
    result: Result<bool, HibernateError>,
) -> Result<bool, HibernateError> {
    if matches!(result, Ok(false)) {
        entry.state.lock().unwrap().execution_active = true;
        #[cfg(test)]
        tests::pause_after_hibernate_race(&entry.name);
    }
    result
}

pub(crate) fn settle(
    _hub: &DetachedAgentHub,
    entry: &Arc<AgentEntry>,
    status: &str,
    output: Option<Value>,
    error: Option<String>,
    lease: Option<&crate::runtime::store::FencedLease>,
) {
    let descriptor = {
        let mut state = entry.state.lock().unwrap();
        state.descriptor.status = status.to_string();
        state.descriptor.output = output;
        state.descriptor.error = error;
        state.descriptor.listen = None;
        state.descriptor.updated_at = Utc::now();
        state.descriptor.clone()
    };
    if let Err(err) = persist_descriptor_for_lease(&entry.factory(), &descriptor, lease) {
        tracing::warn!(agent = %entry.name, error = %err, "persisting detached-agent settlement");
    }
    release_agent_lease(entry, lease);
    entry.signal.notify_all();
}

pub(crate) enum IterationEnd {
    Completed(Value),
    Failed(String),
    Parked(Option<String>),
    WaitSignal(PendingSignal),
}

pub(crate) fn settle_iteration(
    result: anyhow::Result<Value>,
    ctx: &RuntimeContext,
) -> IterationEnd {
    match result {
        Ok(output) => IterationEnd::Completed(output),
        Err(err) if RunInterrupt::from_error(&err).is_some() => {
            if let Some(pending) = ctx.take_pending_signal() {
                IterationEnd::WaitSignal(pending)
            } else if let Some(pending) = ctx.take_pending_input() {
                IterationEnd::Parked(Some(pending.prompt))
            } else if let Some(pending) = ctx.take_pending_approval() {
                IterationEnd::Parked(Some(format!("approval required: {}", pending.target)))
            } else {
                IterationEnd::Parked(None)
            }
        }
        Err(err) => IterationEnd::Failed(err.to_string()),
    }
}
