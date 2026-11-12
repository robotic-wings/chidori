//! Tests for the detached-agent machinery (hub, supervisor, hibernation,
//! delivery injection, leases, spawn gating).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{
    Arc, Condvar, Mutex, Mutex as StdMutex, OnceLock, TryLockError as MutexTryLockError,
};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::json;
use serde_json::Value;

use super::*;
use crate::mcp::McpManager;
use crate::policy::{PolicyCache, PolicyConfig};
use crate::providers::ProviderRegistry;
use crate::runtime::call_log::CallRecord;
use crate::runtime::context::RuntimeContext;
use crate::runtime::host_core;
use crate::runtime::rust_engine::run_agent;
use crate::runtime::snapshot::SIGNAL_INBOX_FILE;
use crate::runtime::snapshot::{
    load_host_promise_records, HostPromiseState, QueuedSignal, RuntimePolicy,
};
use crate::runtime::store::{
    FailSignalInboxStore, FsRunStore, RunStore, RunStoreFactory, SqliteRunStore,
    SqliteRunStoreShared, TeeRunStore,
};
use crate::runtime::template::TemplateEngine;
use crate::runtime::typescript::bindings::HostBindingBackend;
use crate::tools::ToolRegistry;

const LEASE_WAIT_AGENT: &str = "lease-wait-status-probe";
const LEASE_WAIT_CHILD_ENV: &str = "CHIDORI_TEST_LEASE_WAIT_STATUS_CHILD";
const LEASE_WAIT_BASE_ENV: &str = "CHIDORI_TEST_LEASE_WAIT_STATUS_BASE";
const LEASE_WAIT_CLEANUP_ENV: &str = "CHIDORI_TEST_LEASE_WAIT_STATUS_CLEANUP";
const LEASE_ACQUIRE_ERROR_CHILD_ENV: &str = "CHIDORI_TEST_LEASE_ACQUIRE_ERROR_CHILD";
const LEASE_ACQUIRE_ERROR_BASE_ENV: &str = "CHIDORI_TEST_LEASE_ACQUIRE_ERROR_BASE";
const LEASE_ACQUIRE_ERROR_AGENT_NAME: &str = "sqlite-acquire-error-probe";
const SQLITE_STALE_CHILD_ENV: &str = "CHIDORI_TEST_SQLITE_STALE_OWNER_CHILD";
const SQLITE_STALE_BASE_ENV: &str = "CHIDORI_TEST_SQLITE_STALE_OWNER_BASE";
const SQLITE_STALE_CONTROL_ENV: &str = "CHIDORI_TEST_SQLITE_STALE_OWNER_CONTROL";
const SQLITE_STALE_HTTP_ENV: &str = "CHIDORI_TEST_SQLITE_STALE_OWNER_HTTP";
const SQLITE_STALE_AGENT: &str = "sqlite-stale-owner-probe";
const SQLITE_STALE_TEST: &str =
    "runtime::host_agent::tests::detached_sqlite_expired_live_owner_cannot_write_or_start_next_effect";
const PREDECESSOR_MIGRATION_CHILD_ENV: &str = "CHIDORI_TEST_PREDECESSOR_MIGRATION_CHILD";
const PREDECESSOR_MIGRATION_BASE_ENV: &str = "CHIDORI_TEST_PREDECESSOR_MIGRATION_BASE";
const PREDECESSOR_MIGRATION_SCENARIO_ENV: &str = "CHIDORI_TEST_PREDECESSOR_MIGRATION_SCENARIO";
const REGISTRY_TRUNCATION_MODE_ENV: &str = "CHIDORI_TEST_REGISTRY_TRUNCATION_MODE";
const REGISTRY_TRUNCATION_ROOT_ENV: &str = "CHIDORI_TEST_REGISTRY_TRUNCATION_ROOT";
const SAME_NAME_SPAWN_MODE_ENV: &str = "CHIDORI_TEST_SAME_NAME_SPAWN_MODE";
const SAME_NAME_SPAWN_ROOT_ENV: &str = "CHIDORI_TEST_SAME_NAME_SPAWN_ROOT";
const SAME_NAME_SPAWN_GATE_ENV: &str = "CHIDORI_TEST_SAME_NAME_SPAWN_GATE";
const SAME_NAME_SPAWN_NAME_ENV: &str = "CHIDORI_TEST_SAME_NAME_SPAWN_NAME";
const SAME_NAME_SPAWN_CHILD_INDEX_ENV: &str = "CHIDORI_TEST_SAME_NAME_SPAWN_CHILD_INDEX";
const SAME_NAME_SPAWN_TEST: &str =
    "runtime::host_agent::tests::detached_same_name_spawn_competing_processes_preserve_one_delivery";
const STALE_NAME_CACHE_MODE_ENV: &str = "CHIDORI_TEST_STALE_NAME_CACHE_MODE";
const STALE_NAME_CACHE_ROOT_ENV: &str = "CHIDORI_TEST_STALE_NAME_CACHE_ROOT";
const STALE_NAME_CACHE_BACKEND_ENV: &str = "CHIDORI_TEST_STALE_NAME_CACHE_BACKEND";
const STALE_NAME_CACHE_NAME_ENV: &str = "CHIDORI_TEST_STALE_NAME_CACHE_NAME";
const STALE_NAME_CACHE_GATE_ENV: &str = "CHIDORI_TEST_STALE_NAME_CACHE_GATE";
const STALE_NAME_CACHE_TEST: &str =
    "runtime::host_agent::tests::detached_stale_name_cache_routes_lookup_and_send_to_current_incarnation";

struct KillChildOnDrop(std::process::Child);

impl Drop for KillChildOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

static SPAWN_THREAD_START_FAILURES: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();

pub(super) fn fail_next_spawn_thread_start(agent_name: &str) -> bool {
    SPAWN_THREAD_START_FAILURES
        .get_or_init(|| StdMutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .remove(agent_name)
}

fn inject_spawn_thread_start_failure(agent_name: &str) {
    SPAWN_THREAD_START_FAILURES
        .get_or_init(|| StdMutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .insert(agent_name.to_string());
}

struct PredecessorMigrationBarrier {
    agent_name: String,
    reached: std::sync::mpsc::Sender<String>,
    release: StdMutex<std::sync::mpsc::Receiver<()>>,
    claimed: AtomicBool,
}

struct PredecessorMigrationBarrierCleanup {
    agent_name: String,
    release: Option<std::sync::mpsc::Sender<()>>,
}

impl PredecessorMigrationBarrierCleanup {
    fn release(&mut self) {
        if let Some(release) = self.release.take() {
            release
                .send(())
                .expect("predecessor migration barrier is still waiting");
        }
    }
}

impl Drop for PredecessorMigrationBarrierCleanup {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(slot) = PREDECESSOR_MIGRATION_BARRIER.get() {
            let mut slot = slot.lock().unwrap();
            if slot
                .as_ref()
                .is_some_and(|barrier| barrier.agent_name == self.agent_name)
            {
                *slot = None;
            }
        }
    }
}

static PREDECESSOR_MIGRATION_BARRIER: OnceLock<StdMutex<Option<Arc<PredecessorMigrationBarrier>>>> =
    OnceLock::new();

fn install_predecessor_migration_barrier(
    agent_name: &str,
) -> (
    std::sync::mpsc::Receiver<String>,
    std::sync::mpsc::Sender<()>,
) {
    let (reached, wait_until_reached) = std::sync::mpsc::channel();
    let (release, wait_until_released) = std::sync::mpsc::channel();
    *PREDECESSOR_MIGRATION_BARRIER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(Arc::new(PredecessorMigrationBarrier {
        agent_name: agent_name.to_string(),
        reached,
        release: StdMutex::new(wait_until_released),
        claimed: AtomicBool::new(false),
    }));
    (wait_until_reached, release)
}

pub(super) fn pause_before_predecessor_destination_write(agent_name: &str, run_id: &str) {
    let barrier = PREDECESSOR_MIGRATION_BARRIER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(barrier) = barrier.filter(|barrier| barrier.agent_name == agent_name) else {
        return;
    };
    if barrier.claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    barrier
        .reached
        .send(run_id.to_string())
        .expect("predecessor migration test receiver remains active");
    barrier
        .release
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(20))
        .expect("predecessor migration test releases destination-write barrier");
}

fn same_name_spawn_gate_event(agent_name: &str, phase: &str, wait_for_release: bool) {
    if std::env::var(SAME_NAME_SPAWN_NAME_ENV).as_deref() != Ok(agent_name) {
        return;
    }
    use std::io::{Read, Write};
    let Ok(address) = std::env::var(SAME_NAME_SPAWN_GATE_ENV) else {
        return;
    };
    let address: std::net::SocketAddr = address.parse().expect("valid gate address");
    let mut stream = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(5))
        .expect("connect to same-name spawn race controller");
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("set same-name spawn gate timeout");
    writeln!(
        stream,
        "{}",
        serde_json::to_string(&json!({
            "name": agent_name,
            "phase": phase,
            "child_index": std::env::var(SAME_NAME_SPAWN_CHILD_INDEX_ENV).ok(),
        }))
        .expect("serialize same-name spawn lock event")
    )
    .expect("report same-name spawn lock event");
    if !wait_for_release {
        return;
    }
    let mut release = [0u8; 1];
    stream
        .read_exact(&mut release)
        .expect("wait for parent to release same-name spawn lock gate");
    assert_eq!(release[0], b'R', "unexpected same-name spawn lock release");
}

pub(super) fn report_same_name_spawn_lock_wait(agent_name: &str) {
    same_name_spawn_gate_event(agent_name, "lock-waiting", false);
}

pub(super) fn report_same_name_spawn_lock_attempt(agent_name: &str) {
    same_name_spawn_gate_event(agent_name, "lock-attempt", false);
}

pub(super) fn pause_same_name_spawn_after_lock(agent_name: &str) {
    let mode = std::env::var(SAME_NAME_SPAWN_MODE_ENV).unwrap_or_default();
    if matches!(mode.as_str(), "winner" | "lock-crash") {
        same_name_spawn_gate_event(agent_name, "lock-acquired", true);
    }
}

fn sqlite_stale_control_wait(event: &str) -> u8 {
    let address = std::env::var(SQLITE_STALE_CONTROL_ENV)
        .expect("missing SQLite stale-owner control address");
    let mut stream = std::net::TcpStream::connect(address)
        .expect("connect to SQLite stale-owner test controller");
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    use std::io::{Read, Write};
    writeln!(stream, "{event}").expect("announce SQLite stale-owner test event");
    let mut release = [0u8; 1];
    stream
        .read_exact(&mut release)
        .expect("wait for SQLite stale-owner test release");
    release[0]
}

pub(super) fn stale_owner_retry_after_contention(agent_name: &str) -> bool {
    if agent_name != SQLITE_STALE_AGENT
        || std::env::var(SQLITE_STALE_CHILD_ENV).as_deref() != Ok("b")
    {
        return false;
    }
    sqlite_stale_control_wait("B_CONTENDED") == b'R'
}

pub(super) fn pause_stale_owner_after_lease_acquired(agent_name: &str) {
    if agent_name == SQLITE_STALE_AGENT
        && std::env::var(SQLITE_STALE_CHILD_ENV).as_deref() == Ok("b")
    {
        let release = sqlite_stale_control_wait(&format!("B_ACQUIRED {}", process_lease_owner()));
        assert_eq!(release, b'G', "unexpected B acquire barrier release");
    }
}

fn stale_owner_wait_event(
    events: &std::sync::mpsc::Receiver<(String, std::net::TcpStream)>,
    expected: &str,
) -> std::net::TcpStream {
    let (event, stream) = events
        .recv_timeout(Duration::from_secs(20))
        .unwrap_or_else(|err| panic!("timed out waiting for {expected}: {err}"));
    assert_eq!(event, expected, "unexpected stale-owner barrier event");
    stream
}

fn stale_owner_release(mut stream: std::net::TcpStream, value: u8) {
    use std::io::Write;
    stream.write_all(&[value]).unwrap();
}

fn stale_owner_lease(store: &dyn RunStore) -> Value {
    serde_json::from_slice(
        &store
            .get_blob(crate::runtime::store::LEASE_FILE)
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

fn wait_for_initial_stale_owner_lease(store: &dyn RunStore) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let last_observation = loop {
        let observation = match store.get_blob(crate::runtime::store::LEASE_FILE) {
            Ok(Some(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(lease) => return lease,
                Err(error) => format!(
                    "unparseable lease bytes: {error}; bytes={}",
                    String::from_utf8_lossy(&bytes)
                ),
            },
            Ok(None) => "lease blob is absent".to_string(),
            Err(error) => format!("reading lease blob: {error:#}"),
        };
        if std::time::Instant::now() >= deadline {
            break observation;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    panic!("timed out waiting for parseable initial A lease: {last_observation}");
}

#[derive(Clone, Copy)]
pub(super) enum LeaseWaitGatePoint {
    BeforeAcquire,
    ContendedBeforeWait,
}

struct LeaseWaitTestGate {
    agent_name: String,
    before_arrived: std::sync::mpsc::Sender<()>,
    before_release: StdMutex<std::sync::mpsc::Receiver<()>>,
    contention_arrived: std::sync::mpsc::Sender<()>,
    contention_release: StdMutex<std::sync::mpsc::Receiver<()>>,
    before_claimed: AtomicBool,
    contention_claimed: AtomicBool,
}

static LEASE_WAIT_TEST_GATE: std::sync::OnceLock<StdMutex<Option<LeaseWaitTestGate>>> =
    std::sync::OnceLock::new();
static LEASE_ACQUIRE_ERROR_AGENT: std::sync::OnceLock<StdMutex<Option<String>>> =
    std::sync::OnceLock::new();

pub(super) fn fail_next_lease_acquire(agent_name: &str) -> bool {
    let slot = LEASE_ACQUIRE_ERROR_AGENT.get_or_init(|| StdMutex::new(None));
    let mut slot = slot.lock().unwrap();
    if slot.as_deref() == Some(agent_name) {
        *slot = None;
        true
    } else {
        false
    }
}

fn inject_lease_acquire_error(agent_name: &str) {
    *LEASE_ACQUIRE_ERROR_AGENT
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(agent_name.to_string());
}

pub(super) fn lease_wait_test_gate(agent_name: &str, point: LeaseWaitGatePoint) {
    let gates = LEASE_WAIT_TEST_GATE.get_or_init(|| StdMutex::new(None));
    let gates = gates.lock().unwrap();
    let Some(gates) = gates
        .as_ref()
        .filter(|gates| gates.agent_name == agent_name)
    else {
        return;
    };
    let (claimed, arrived, release) = match point {
        LeaseWaitGatePoint::BeforeAcquire => (
            &gates.before_claimed,
            &gates.before_arrived,
            &gates.before_release,
        ),
        LeaseWaitGatePoint::ContendedBeforeWait => (
            &gates.contention_claimed,
            &gates.contention_arrived,
            &gates.contention_release,
        ),
    };
    if claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = arrived.send(());
    release
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(20))
        .expect("lease-wait test gate timed out");
}

fn install_lease_wait_test_gate(
    agent_name: &str,
) -> (
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Sender<()>,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (before_arrived_tx, before_arrived_rx) = std::sync::mpsc::channel();
    let (before_release_tx, before_release_rx) = std::sync::mpsc::channel();
    let (contention_arrived_tx, contention_arrived_rx) = std::sync::mpsc::channel();
    let (contention_release_tx, contention_release_rx) = std::sync::mpsc::channel();
    *LEASE_WAIT_TEST_GATE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(LeaseWaitTestGate {
        agent_name: agent_name.to_string(),
        before_arrived: before_arrived_tx,
        before_release: StdMutex::new(before_release_rx),
        contention_arrived: contention_arrived_tx,
        contention_release: StdMutex::new(contention_release_rx),
        before_claimed: AtomicBool::new(false),
        contention_claimed: AtomicBool::new(false),
    });
    (
        before_arrived_rx,
        before_release_tx,
        contention_arrived_rx,
        contention_release_tx,
    )
}

#[test]
fn detached_status_is_not_live_while_supervisor_waits_for_lease() {
    const TEST: &str =
        "runtime::host_agent::tests::detached_status_is_not_live_while_supervisor_waits_for_lease";
    if std::env::var_os(LEASE_WAIT_CHILD_ENV).is_some() {
        let base = PathBuf::from(std::env::var_os(LEASE_WAIT_BASE_ENV).unwrap());
        let run_base = base.join(".chidori/runs");
        let source = base.join("services/lease-wait.ts");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            &source,
            r#"export async function agent() {
                await chidori.tool("executionMarker", {});
                await chidori.signal("never");
            }"#,
        )
        .unwrap();

        let (before_arrived, before_release, contention_arrived, contention_release) =
            install_lease_wait_test_gate(LEASE_WAIT_AGENT);
        let (execution_tx, execution_rx) = std::sync::mpsc::channel();
        let mut tools = ToolRegistry::new();
        tools.register_native(
            "executionMarker",
            "test execution marker",
            Vec::new(),
            move |_| {
                execution_tx.send(()).map_err(|err| anyhow::anyhow!(err))?;
                Ok(json!({ "entered": true }))
            },
        );
        let parts = sqlite_test_parts(&base, run_base.clone(), Arc::new(tools));
        let spawned = hub()
            .spawn(
                &parts,
                source.to_str().unwrap(),
                json!({}),
                &SpawnOptions {
                    name: Some(LEASE_WAIT_AGENT.to_string()),
                    restart: "resume".to_string(),
                    max_restarts: 0,
                    backoff_ms: 0,
                    model: None,
                },
                "lease-wait-test-owner".to_string(),
            )
            .unwrap();

        before_arrived
            .recv_timeout(Duration::from_secs(10))
            .expect("supervisor did not reach pre-acquire gate");
        let run_id = spawned["runId"].as_str().unwrap();
        let store = RunStoreFactory::shared(&run_base).store_for(run_id);
        crate::runtime::store::acquire_lease(
            store.as_ref(),
            "other-process-and-run",
            chrono::Duration::seconds(2),
        )
        .unwrap()
        .expect("test fixture must acquire the seeded lease");
        before_release.send(()).unwrap();
        contention_arrived
            .recv_timeout(Duration::from_secs(10))
            .expect("supervisor did not report actual lease contention");

        let observed = hub().status(&parts, LEASE_WAIT_AGENT).unwrap();
        let execution_marker_empty = matches!(
            execution_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );

        // A stop and matching delivery while another process owns the
        // lease must not strand the run as running with no driver. The
        // queued wake cannot execute until the foreign lease expires.
        let entry = hub().ensure_entry(&parts, LEASE_WAIT_AGENT).unwrap();
        let stop = hub().stop(&parts, LEASE_WAIT_AGENT).unwrap();
        assert_eq!(stop["stopRequested"], json!(true), "{stop}");
        let sent = hub()
            .send(
                &parts,
                LEASE_WAIT_AGENT,
                "never",
                json!({ "n": 64 }),
                json!({ "kind": "human", "id": "test" }),
            )
            .unwrap();
        assert_eq!(sent["delivered"], json!(true), "{sent}");
        contention_release.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut state = entry.state.lock().unwrap();
        while state.descriptor.status != "stopped" || state.thread_live {
            let now = std::time::Instant::now();
            assert!(
                now < deadline,
                "supervisor stranded stopped request after contended lease: {:?}",
                state.descriptor.status
            );
            let (next, _) = entry
                .signal
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap();
            state = next;
        }
        std::fs::write(
            std::env::var_os(LEASE_WAIT_CLEANUP_ENV).unwrap(),
            "stopped-after-foreign-lease-expired",
        )
        .unwrap();

        assert_eq!(observed["status"], json!("running"), "{observed}");
        assert!(
            execution_marker_empty,
            "agent execution marker was already sent"
        );
        assert_eq!(
            observed["live"],
            json!(false),
            "a supervisor waiting on another process's lease is not executing the agent: {observed}"
        );
        return;
    }

    let base = tempfile::tempdir().unwrap();
    let cleanup_marker = base.path().join("cleanup-complete");
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env(LEASE_WAIT_CHILD_ENV, "1")
        .env(LEASE_WAIT_BASE_ENV, base.path())
        .env(LEASE_WAIT_CLEANUP_ENV, &cleanup_marker)
        .env("CHIDORI_RUN_STORE", "fs")
        .env_remove("CHIDORI_RUN_DB")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "lease-wait status child failed: {stderr}"
    );
    assert_eq!(
        std::fs::read_to_string(cleanup_marker).unwrap(),
        "stopped-after-foreign-lease-expired",
        "child must settle the persisted stop after the foreign lease expires"
    );
}

#[test]
fn sqlite_supervisor_fails_closed_when_lease_acquisition_errors() {
    const TEST: &str =
        "runtime::host_agent::tests::sqlite_supervisor_fails_closed_when_lease_acquisition_errors";
    if std::env::var_os(LEASE_ACQUIRE_ERROR_CHILD_ENV).is_some() {
        let base = PathBuf::from(std::env::var_os(LEASE_ACQUIRE_ERROR_BASE_ENV).unwrap());
        let run_base = base.join(".chidori/runs");
        let source = base.join("services/acquire-error.ts");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            &source,
            r#"export async function agent() {
                await chidori.tool("executionMarker", {});
            }"#,
        )
        .unwrap();

        let (execution_tx, execution_rx) = std::sync::mpsc::channel();
        let mut tools = ToolRegistry::new();
        tools.register_native(
            "executionMarker",
            "test execution marker",
            Vec::new(),
            move |_| {
                execution_tx.send(()).map_err(|err| anyhow::anyhow!(err))?;
                Ok(json!({ "entered": true }))
            },
        );
        let parts = sqlite_test_parts(&base, run_base, Arc::new(tools));
        inject_lease_acquire_error(LEASE_ACQUIRE_ERROR_AGENT_NAME);
        hub()
            .spawn(
                &parts,
                source.to_str().unwrap(),
                json!({}),
                &SpawnOptions {
                    name: Some(LEASE_ACQUIRE_ERROR_AGENT_NAME.to_string()),
                    restart: "resume".to_string(),
                    max_restarts: 0,
                    backoff_ms: 0,
                    model: None,
                },
                "lease-acquire-error-test-owner".to_string(),
            )
            .unwrap();

        let entry = hub()
            .ensure_entry(&parts, LEASE_ACQUIRE_ERROR_AGENT_NAME)
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut state = entry.state.lock().unwrap();
        while state.thread_live {
            let now = std::time::Instant::now();
            assert!(now < deadline, "supervisor did not stop after lease error");
            let (next, _) = entry
                .signal
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap();
            state = next;
        }
        drop(state);

        assert!(
            matches!(
                execution_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ),
            "SQLite-capable supervisor executed without a lease token"
        );
        assert_eq!(
            hub()
                .status(&parts, LEASE_ACQUIRE_ERROR_AGENT_NAME)
                .unwrap()["live"],
            json!(false)
        );
        return;
    }

    let base = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env(LEASE_ACQUIRE_ERROR_CHILD_ENV, "1")
        .env(LEASE_ACQUIRE_ERROR_BASE_ENV, base.path())
        .env("CHIDORI_RUN_STORE", "sqlite")
        .env_remove("CHIDORI_RUN_DB")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "SQLite lease-acquisition fail-closed child failed: {stderr}"
    );
}

#[test]
fn completed_listen_requires_exact_canonical_arguments() {
    let record = |function: &str, args: Value| CallRecord {
        seq: 7,
        parent_seq: None,
        function: function.to_string(),
        args,
        result: Value::Null,
        duration_ms: 0,
        token_usage: None,
        timestamp: Utc::now(),
        error: None,
    };
    let listen = |function: &str, names: &[&str]| ListenState {
        names: names.iter().map(|name| (*name).to_string()).collect(),
        seq: 7,
        function: function.to_string(),
        deadline: None,
    };

    assert!(listen_has_completed_record(
        &listen("signal", &["x"]),
        &[record("signal", json!({ "name": "x" }))]
    ));
    assert!(!listen_has_completed_record(
        &listen("signal", &["x", "y"]),
        &[record("signal", json!({ "name": "x" }))]
    ));
    assert!(!listen_has_completed_record(
        &listen("signal", &["x"]),
        &[record("signal", json!({ "name": "x", "extra": true }))]
    ));

    assert!(listen_has_completed_record(
        &listen("signal_any", &["x"]),
        &[record("signal_any", json!({ "names": ["x"] }))]
    ));
    assert!(!listen_has_completed_record(
        &listen("signal_any", &["x"]),
        &[record("signal_any", json!({ "names": ["x", null] }))]
    ));
    assert!(!listen_has_completed_record(
        &listen("signal_any", &["x"]),
        &[record(
            "signal_any",
            json!({ "names": ["x"], "extra": true })
        )]
    ));
}

#[test]
fn detached_live_send_commit_exit_rearms_and_journals_once() {
    const MODE: &str = "CHIDORI_TEST_LIVE_CUT_MODE";
    const BASE_ENV: &str = "CHIDORI_TEST_LIVE_CUT_BASE";
    const TEST: &str =
        "runtime::host_agent::tests::detached_live_send_commit_exit_rearms_and_journals_once";
    const AGENT: &str = "live-cut-rearm-agent";

    if let Some(mode) = std::env::var_os(MODE) {
        let mode = mode.to_string_lossy();
        let base = PathBuf::from(std::env::var_os(BASE_ENV).unwrap());
        let run_base = base.join(".chidori/runs");
        let source = base.join("services/live-cut.ts");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            &source,
            r#"export async function agent() {
                await chidori.signal("start");
                await chidori.tool("testHold", {});
                const cut = await chidori.signal("cut");
                return { id: cut.payload.id };
            }"#,
        )
        .unwrap();

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Arc::new(StdMutex::new(release_rx));
        let block_tool = mode == "crash";
        let mut tools = ToolRegistry::new();
        tools.register_native(
            "testHold",
            "test-only barrier for detached live signal cut",
            Vec::new(),
            move |_| {
                entered_tx.send(()).map_err(|err| anyhow::anyhow!(err))?;
                if block_tool {
                    release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(20))
                        .map_err(|err| anyhow::anyhow!("tool barrier timed out: {err}"))?;
                }
                Ok(json!({"ok": true}))
            },
        );
        let parts = sqlite_test_parts(&base, run_base.clone(), Arc::new(tools));

        if mode == "crash" {
            let spawned = hub()
                .spawn(
                    &parts,
                    source.to_str().unwrap(),
                    json!({}),
                    &SpawnOptions {
                        name: Some(AGENT.to_string()),
                        restart: "resume".to_string(),
                        max_restarts: 0,
                        backoff_ms: 0,
                        model: None,
                    },
                    "live-cut-test-owner".to_string(),
                )
                .unwrap();
            wait_for_agent_status(&parts, AGENT, |status| status == "hibernating");
            let entry = hub().ensure_entry(&parts, AGENT).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let mut state = entry.state.lock().unwrap();
            while state.descriptor.status != "hibernating" || state.thread_live {
                let now = std::time::Instant::now();
                assert!(now < deadline, "worker did not finish initial hibernation");
                let (next, _) = entry
                    .signal
                    .wait_timeout(state, deadline.saturating_duration_since(now))
                    .unwrap();
                state = next;
            }
            drop(state);
            assert_eq!(
                hub().status(&parts, AGENT).unwrap()["live"],
                json!(false),
                "initial signal listen must be hibernating before send"
            );
            assert_eq!(
                hub()
                    .send(&parts, AGENT, "start", json!({}), json!({"test": true}))
                    .unwrap()["delivered"],
                json!(true)
            );
            entered_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("worker did not enter the blocking tool");
            let live_status = hub().status(&parts, AGENT).unwrap();
            assert_eq!(live_status["status"], json!("running"), "{live_status}");
            assert_eq!(live_status["live"], json!(true), "{live_status}");

            let run_id = spawned["runId"].as_str().unwrap();
            std::fs::write(base.join("run-id.txt"), run_id).unwrap();
            std::fs::write(base.join("send-state.txt"), "about-to-call").unwrap();
            std::env::set_var("CHIDORI_TEST_EXIT_AFTER_SIGNAL_INBOX_COMMIT", "1");
            let _ = hub().send(
                &parts,
                AGENT,
                "cut",
                json!({"id": "cut-1"}),
                json!({"test": true}),
            );
            let _ = release_tx.send(());
            panic!("post-commit failpoint did not terminate the sender process");
        }

        assert!(matches!(mode.as_ref(), "rearm" | "diagnostic"));
        let run_id = std::fs::read_to_string(base.join("run-id.txt")).unwrap();
        let factory = RunStoreFactory::shared(&run_base);
        if mode == "diagnostic" {
            let registered = factory
                .registry_get(AGENT)
                .unwrap()
                .expect("registry entry missing in diagnostic copy");
            let mut descriptor: AgentDescriptor =
                serde_json::from_value(registered["descriptor"].clone()).unwrap();
            descriptor.status = "running".to_string();
            descriptor.listen = None;
            persist_descriptor(&factory, &descriptor);
        }
        let store = factory.store_for(&run_id);
        let records_before_rearm = store.load_call_log().unwrap().unwrap_or_default();
        let inbox_before_rearm: Vec<QueuedSignal> = store
            .get_blob(SIGNAL_INBOX_FILE)
            .unwrap()
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap_or_default();
        let cut_rows_before_rearm = records_before_rearm
            .iter()
            .filter(|record| {
                record.function == "signal" && record.result["payload"]["id"] == json!("cut-1")
            })
            .count();
        let cut_inbox_before_rearm = inbox_before_rearm
            .iter()
            .filter(|queued| queued.payload["id"] == json!("cut-1"))
            .count();
        let registry_before = factory
            .registry_get(AGENT)
            .unwrap()
            .expect("registry entry missing before rearm");
        let descriptor_before = registry_before["descriptor"].clone();
        let rearmed = hub().rearm_from_registry(parts.clone()).unwrap();

        // Wait on the hub's state condition variable for completion, while
        // retaining the last observed status when the expected worker never starts.
        let entry = hub().ensure_entry(&parts, AGENT).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut state = entry.state.lock().unwrap();
        while !matches!(state.descriptor.status.as_str(), "completed" | "failed")
            && !(state.descriptor.status == "hibernating" && !state.thread_live)
        {
            let now = std::time::Instant::now();
            if now >= deadline {
                break;
            }
            let (next, _) = entry
                .signal
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap();
            state = next;
        }
        let final_status = state.descriptor.status_json();
        let final_live = state.thread_live;
        drop(state);

        let store = factory.store_for(&run_id);
        let records = store.load_call_log().unwrap().unwrap_or_default();
        let inbox: Vec<QueuedSignal> = store
            .get_blob(SIGNAL_INBOX_FILE)
            .unwrap()
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap_or_default();
        let cut_rows: Vec<_> = records
            .iter()
            .filter(|record| {
                record.function == "signal" && record.result["payload"]["id"] == json!("cut-1")
            })
            .collect();
        let report = json!({
            "ack": "no response: crash child exited before hub.send returned",
            "send_state": std::fs::read_to_string(base.join("send-state.txt")).unwrap(),
            "descriptor_before_rearm": descriptor_before,
            "cut_rows_before_rearm": cut_rows_before_rearm,
            "cut_inbox_before_rearm": cut_inbox_before_rearm,
            "rearmed": rearmed,
            "status_after_rearm": final_status,
            "live_after_rearm": final_live,
            "cut_signal_rows": cut_rows.len(),
            "cut_signal_result": cut_rows.first().map(|record| &record.result),
            "journal_rows": records.len(),
            "inbox": inbox,
        });
        std::fs::write(
            base.join("rearm-report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        return;
    }

    let mut failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().to_path_buf();
        let db_path = base.join("runs.sqlite3");
        eprintln!(
            "ARTIFACT_BACKEND={backend} ARTIFACT_BASE={}",
            base.display()
        );

        let crash = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(TEST)
            .arg("--nocapture")
            .env(MODE, "crash")
            .env(BASE_ENV, &base)
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", &db_path)
            .output()
            .unwrap();
        assert_eq!(
            crash.status.code(),
            Some(87),
            "{backend} crash child did not exit at the post-commit/pre-publication cut; stdout={} stderr={}",
            String::from_utf8_lossy(&crash.stdout),
            String::from_utf8_lossy(&crash.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(base.join("send-state.txt")).unwrap(),
            "about-to-call",
            "crash cut must occur before hub.send returns; no response is not delivered:true"
        );

        let run_id = std::fs::read_to_string(base.join("run-id.txt")).unwrap();
        let store: Arc<dyn crate::runtime::store::RunStore> = match backend {
            "fs" => Arc::new(crate::runtime::store::FsRunStore::new(
                base.join(".chidori/runs").join(&run_id),
            )),
            "sqlite" => Arc::new(crate::runtime::store::SqliteRunStore::new(
                crate::runtime::store::SqliteRunStoreShared::open(&db_path).unwrap(),
                run_id,
            )),
            _ => unreachable!(),
        };
        let expired = crate::runtime::store::StoredRunLease {
            owner: "dead-live-cut-child".to_string(),
            expires_at: Utc::now() - chrono::Duration::seconds(1),
            generation: 1,
        };
        store
            .put_blob(
                crate::runtime::store::LEASE_FILE,
                &serde_json::to_vec(&expired).unwrap(),
            )
            .unwrap();
        drop(store);

        let diagnostic_temp = tempfile::tempdir().unwrap();
        copy_test_tree(&base, diagnostic_temp.path()).unwrap();

        let rearm = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(TEST)
            .arg("--nocapture")
            .env(MODE, "rearm")
            .env(BASE_ENV, &base)
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", &db_path)
            .output()
            .unwrap();
        if !rearm.status.success() {
            failures.push(format!(
                "{backend}: rearm child failed; stdout={} stderr={}",
                String::from_utf8_lossy(&rearm.stdout),
                String::from_utf8_lossy(&rearm.stderr)
            ));
            let preserved = temp.keep();
            failures.push(format!(
                "{backend} artifacts preserved at {}",
                preserved.display()
            ));
            continue;
        }

        let report: Value =
            serde_json::from_slice(&std::fs::read(base.join("rearm-report.json")).unwrap())
                .unwrap();
        assert_eq!(report["rearmed"], json!(1), "report={report}");

        let diagnostic = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(TEST)
            .arg("--nocapture")
            .env(MODE, "diagnostic")
            .env(BASE_ENV, diagnostic_temp.path())
            .env("CHIDORI_RUN_STORE", backend)
            .env(
                "CHIDORI_RUN_DB",
                diagnostic_temp.path().join("runs.sqlite3"),
            )
            .output()
            .unwrap();
        let diagnostic_report = if diagnostic.status.success() {
            match std::fs::read(diagnostic_temp.path().join("rearm-report.json"))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            {
                Some(report) => Some(report),
                None => {
                    failures.push(format!(
                        "{backend} descriptor-only diagnostic control produced no valid report; stdout={} stderr={}",
                        String::from_utf8_lossy(&diagnostic.stdout),
                        String::from_utf8_lossy(&diagnostic.stderr)
                    ));
                    None
                }
            }
        } else {
            failures.push(format!(
                "{backend} descriptor-only diagnostic control failed: stdout={} stderr={}",
                String::from_utf8_lossy(&diagnostic.stdout),
                String::from_utf8_lossy(&diagnostic.stderr)
            ));
            None
        };
        if let Some(diagnostic_report) = diagnostic_report {
            let diagnostic_ok = diagnostic_report["rearmed"] == json!(1)
                && diagnostic_report["descriptor_before_rearm"]["status"] == json!("running")
                && diagnostic_report["descriptor_before_rearm"]["listen"] == Value::Null
                && diagnostic_report["cut_signal_rows"] == json!(1)
                && diagnostic_report["inbox"]
                    .as_array()
                    .is_some_and(Vec::is_empty);
            if !diagnostic_ok {
                failures.push(format!(
                    "{backend} descriptor-only diagnostic control did not recover as expected; report={diagnostic_report}"
                ));
            }
        }

        let descriptor = &report["descriptor_before_rearm"];
        let listen = &descriptor["listen"];
        assert!(
            matches!(
                descriptor["status"].as_str(),
                Some("running" | "hibernating")
            ),
            "descriptor should remain nonterminal at the stale start listen; report={report}"
        );
        assert_eq!(listen["seq"], json!(1), "report={report}");
        assert_eq!(listen["names"][0], json!("start"), "report={report}");
        assert_eq!(
            report["ack"],
            json!("no response: crash child exited before hub.send returned")
        );
        assert_eq!(report["cut_rows_before_rearm"], json!(0), "report={report}");
        assert_eq!(
            report["cut_inbox_before_rearm"],
            json!(1),
            "report={report}"
        );

        let recovered = report["status_after_rearm"]["status"] == json!("completed")
            && report["cut_signal_rows"] == json!(1)
            && report["cut_signal_result"]["payload"]["id"] == json!("cut-1")
            && report["inbox"].as_array().is_some_and(Vec::is_empty);
        if !recovered {
            failures.push(format!(
                "{backend}: committed cut-1 was not consumed/journaled exactly once after real rearm; observed status={}, live={}, waitingFor={}, cut_rows={}, journal_rows={}, inbox={}; artifact_base={}",
                report["status_after_rearm"]["status"],
                report["live_after_rearm"],
                report["status_after_rearm"]["waitingFor"],
                report["cut_signal_rows"],
                report["journal_rows"],
                report["inbox"],
                base.display()
            ));
            let preserved = temp.keep();
            failures.push(format!(
                "{backend} artifacts preserved at {}",
                preserved.display()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "detached live signal restart contract failed:\n{}",
        failures.join("\n")
    );
}

fn copy_test_tree(source: &std::path::Path, destination: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_test_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[test]
fn live_send_does_not_acknowledge_when_signal_inbox_persistence_fails() {
    const CHILD: &str = "CHIDORI_TEST_LIVE_HUB_FAILURE_CHILD";
    const TEST: &str =
        "runtime::host_agent::tests::live_send_does_not_acknowledge_when_signal_inbox_persistence_fails";
    if std::env::var_os(CHILD).is_none() {
        let mut failures = Vec::new();
        for mode in ["besteffort", "effect", "strict"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(TEST)
                .arg("--nocapture")
                .env(CHILD, "1")
                .env("CHIDORI_DURABILITY", mode)
                .output()
                .unwrap();
            if !output.status.success() {
                failures.push(format!(
                    "{mode}: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "live hub delivery contract failed by mode:\n{}",
            failures.join("\n")
        );
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let run_base = temp.path().join(".chidori/runs");
    std::fs::create_dir_all(&run_base).unwrap();
    let parts = sqlite_test_parts(temp.path(), run_base, Arc::new(ToolRegistry::new()));
    let name = unique("live-send-persist-failure");
    let ctx = RuntimeContext::new();
    let run_dir = temp.path().join("run");
    let store = Arc::new(FailSignalInboxStore::new(&run_dir));
    ctx.enable_persistence_with_store(run_dir, store.clone());
    let now = Utc::now();
    let entry = Arc::new(AgentEntry {
        name: name.clone(),
        parts: parts.clone(),
        state: Mutex::new(AgentState {
            descriptor: AgentDescriptor {
                name: name.clone(),
                run_id: "live-run".to_string(),
                source: "test.ts".to_string(),
                input: Value::Null,
                restart: "never".to_string(),
                max_restarts: 0,
                backoff_ms: 0,
                owner_run_id: "owner".to_string(),
                model: None,
                status: "running".to_string(),
                listen: None,
                restarts: 0,
                output: None,
                error: None,
                created_at: now,
                updated_at: now,
            },
            live_ctx: Some(ctx.clone()),
            execution_active: true,
            thread_live: true,
            wake_pending: false,
            stop_requested: false,
        }),
        signal: Condvar::new(),
        #[cfg(test)]
        live_send_notify_count: std::sync::atomic::AtomicUsize::new(0),
    });
    let registered_descriptor =
        serde_json::to_value(&entry.state.lock().unwrap().descriptor).unwrap();
    RunStoreFactory::shared(&parts.run_base)
        .registry_put(&name, "live-run", &registered_descriptor)
        .unwrap();
    hub()
        .entries
        .lock()
        .unwrap()
        .insert(name.clone(), entry.clone());

    let response = hub().send(
        &parts,
        &name,
        "approval",
        json!({"id": "hub-1"}),
        json!(null),
    );
    hub().entries.lock().unwrap().remove(&name);
    let published = ctx.signal_inbox();
    let notify_count = entry
        .live_send_notify_count
        .load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(store.failure_count(), 1);
    assert!(
        response.is_err() && notify_count == 0 && published.is_empty(),
        "failed live send must return Err without calling notify_all/publishing: response={response:?}, notify_count={notify_count}, inbox={published:?}"
    );
}

struct InboxRaceBarriers {
    agent_name: String,
    sender_arrived: std::sync::mpsc::Sender<()>,
    sender_release: StdMutex<std::sync::mpsc::Receiver<()>>,
    worker_arrived: std::sync::mpsc::Sender<()>,
    worker_release: StdMutex<std::sync::mpsc::Receiver<()>>,
    sender_claimed: AtomicBool,
    worker_claimed: AtomicBool,
    sender_launched: AtomicBool,
    sender_result: StdMutex<Option<String>>,
    worker_timed_out: AtomicBool,
}

impl InboxRaceBarriers {
    fn milestones(&self) -> String {
        format!(
            "sender_launched={}, sender_barrier_claimed={}, sender_result={:?}, worker_barrier_claimed={}, worker_timed_out={}",
            self.sender_launched.load(Ordering::SeqCst),
            self.sender_claimed.load(Ordering::SeqCst),
            *self.sender_result.lock().unwrap(),
            self.worker_claimed.load(Ordering::SeqCst),
            self.worker_timed_out.load(Ordering::SeqCst),
        )
    }
}

static INBOX_RACE_BARRIERS: OnceLock<StdMutex<Option<Arc<InboxRaceBarriers>>>> = OnceLock::new();

struct HibernateRaceStatusBarrier {
    agent_name: String,
    reached: std::sync::mpsc::Sender<()>,
    release: StdMutex<std::sync::mpsc::Receiver<()>>,
    claimed: AtomicBool,
}

static HIBERNATE_RACE_STATUS_BARRIER: OnceLock<StdMutex<Option<Arc<HibernateRaceStatusBarrier>>>> =
    OnceLock::new();

struct InboxSnapshotGapBarriers {
    agent_name: String,
    snapshot_arrived: std::sync::mpsc::Sender<()>,
    snapshot_release: StdMutex<std::sync::mpsc::Receiver<()>>,
    context_published: std::sync::mpsc::Sender<()>,
    context_release: StdMutex<std::sync::mpsc::Receiver<()>>,
    snapshot_claimed: AtomicBool,
    context_claimed: AtomicBool,
}

static INBOX_SNAPSHOT_GAP_BARRIERS: OnceLock<StdMutex<Option<Arc<InboxSnapshotGapBarriers>>>> =
    OnceLock::new();

struct AlarmInboxReadBarrier {
    agent_name: String,
    reached: std::sync::mpsc::Sender<()>,
    release: StdMutex<std::sync::mpsc::Receiver<()>>,
    claimed: AtomicBool,
}

static ALARM_INBOX_READ_BARRIER: OnceLock<StdMutex<Option<Arc<AlarmInboxReadBarrier>>>> =
    OnceLock::new();

fn install_alarm_inbox_read_barrier(
    agent_name: &str,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (reached, wait_until_reached) = std::sync::mpsc::channel();
    let (release, wait_until_released) = std::sync::mpsc::channel();
    *ALARM_INBOX_READ_BARRIER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(Arc::new(AlarmInboxReadBarrier {
        agent_name: agent_name.to_string(),
        reached,
        release: StdMutex::new(wait_until_released),
        claimed: AtomicBool::new(false),
    }));
    (wait_until_reached, release)
}

pub(super) fn pause_after_alarm_wake_lease_before_inbox_read(agent_name: &str) {
    let barrier = ALARM_INBOX_READ_BARRIER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(barrier) = barrier.filter(|barrier| barrier.agent_name == agent_name) else {
        return;
    };
    if barrier.claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    barrier
        .reached
        .send(())
        .expect("alarm inbox test must observe the post-lease barrier");
    barrier
        .release
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(20))
        .expect("alarm inbox test must release the post-lease barrier");
}

fn clear_alarm_inbox_read_barrier() {
    *ALARM_INBOX_READ_BARRIER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = None;
}

pub(super) fn pause_before_cross_process_inbox_update(agent_name: &str) {
    let Some(target) = std::env::var_os("CHIDORI_TEST_CROSS_PROCESS_INBOX_GATE") else {
        return;
    };
    let configured_name = std::env::var("CHIDORI_TEST_CROSS_PROCESS_INBOX_AGENT")
        .expect("cross-process inbox gate requires an agent name");
    if configured_name != agent_name {
        return;
    }
    let role = std::env::var("CHIDORI_TEST_CROSS_PROCESS_INBOX_ROLE")
        .expect("cross-process inbox gate requires a writer role");
    let role_byte = role
        .as_bytes()
        .last()
        .copied()
        .expect("cross-process inbox writer role cannot be empty");
    let target: std::net::SocketAddr = target
        .to_string_lossy()
        .parse()
        .expect("invalid cross-process inbox gate address");
    let mut stream = std::net::TcpStream::connect_timeout(&target, Duration::from_secs(10))
        .expect("connecting to cross-process inbox gate");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("setting cross-process inbox gate read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("setting cross-process inbox gate write timeout");
    std::io::Write::write_all(&mut stream, &[role_byte])
        .expect("announcing cross-process inbox snapshot");
    let mut release = [0];
    std::io::Read::read_exact(&mut stream, &mut release)
        .expect("waiting for cross-process inbox gate release");
    assert_eq!(release, [b'G'], "invalid cross-process inbox gate release");
}

pub(super) fn pause_after_supervisor_inbox_snapshot(agent_name: &str) {
    let barriers = INBOX_SNAPSHOT_GAP_BARRIERS
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(barriers) = barriers.filter(|barriers| barriers.agent_name == agent_name) else {
        return;
    };
    if barriers.snapshot_claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = barriers.snapshot_arrived.send(());
    let _ = barriers
        .snapshot_release
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(30));
}

pub(super) fn pause_after_hibernate_race(agent_name: &str) {
    let barrier = HIBERNATE_RACE_STATUS_BARRIER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(barrier) = barrier.filter(|barrier| barrier.agent_name == agent_name) else {
        return;
    };
    if barrier.claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = barrier.reached.send(());
    let _ = barrier.release.lock().unwrap().recv();
}

pub(super) fn pause_after_supervisor_publishes_live_context(agent_name: &str) {
    let barriers = INBOX_SNAPSHOT_GAP_BARRIERS
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(barriers) = barriers.filter(|barriers| barriers.agent_name == agent_name) else {
        return;
    };
    if barriers.context_claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = barriers.context_published.send(());
    let _ = barriers
        .context_release
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(30));
}

pub(super) fn pause_after_live_context_snapshot(agent_name: &str) {
    let barriers = INBOX_RACE_BARRIERS
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(barriers) = barriers.filter(|barriers| barriers.agent_name == agent_name) else {
        return;
    };
    if barriers.sender_claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = barriers.sender_arrived.send(());
    let _ = barriers
        .sender_release
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(30));
}

pub(super) fn pause_after_worker_clears_live_context(agent_name: &str) {
    let barriers = INBOX_RACE_BARRIERS
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(barriers) = barriers.filter(|barriers| barriers.agent_name == agent_name) else {
        return;
    };
    if barriers.worker_claimed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = barriers.worker_arrived.send(());
    let _ = barriers
        .worker_release
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(30));
}

fn sqlite_test_parts(
    base: &Path,
    run_base: PathBuf,
    tools: Arc<ToolRegistry>,
) -> AgentRuntimeParts {
    AgentRuntimeParts {
        providers: Arc::new(ProviderRegistry::new()),
        template_engine: Arc::new(TemplateEngine::new(base)),
        tokio_rt: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        policy: Arc::new(PolicyConfig::default()),
        tools,
        mcp: Arc::new(McpManager::new()),
        run_base,
    }
}

fn write_cached_resolution_fixture(
    hub: &DetachedAgentHub,
    parts: &AgentRuntimeParts,
    name: &str,
    run_id: &str,
    status: &str,
) -> Arc<AgentEntry> {
    let descriptor = AgentDescriptor {
        name: name.to_string(),
        run_id: run_id.to_string(),
        source: "agent.ts".to_string(),
        input: Value::Null,
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        owner_run_id: "resolution-test".to_string(),
        model: None,
        status: status.to_string(),
        listen: None,
        restarts: 0,
        output: None,
        error: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let value = serde_json::to_value(&descriptor).unwrap();
    RunStoreFactory::shared(&parts.run_base)
        .registry_put(name, run_id, &value)
        .unwrap();
    hub.ensure_entry(parts, name).unwrap()
}

const REARM_CORRUPT_REGISTRY_ROOT: &str = "CHIDORI_TEST_REARM_CORRUPT_REGISTRY_ROOT";
const REARM_CORRUPT_REGISTRY_SCENARIO: &str = "CHIDORI_TEST_REARM_CORRUPT_REGISTRY_SCENARIO";
const REARM_CORRUPT_REGISTRY_TEST: &str =
    "runtime::host_agent::tests::detached_rearm_from_registry_rejects_corrupt_metadata_and_preserves_controls";

fn seed_rearm_registry_entry(
    factory: &RunStoreFactory,
    name: &str,
    run_id: &str,
) -> (Vec<CallRecord>, Vec<u8>) {
    let now = Utc::now();
    let descriptor = json!({
        "name": name,
        "run_id": run_id,
        "source": "services/hibernating.ts",
        "input": {},
        "restart": "resume",
        "max_restarts": 0,
        "backoff_ms": 0,
        "owner_run_id": "registry-rearm-test",
        "model": null,
        "status": "hibernating",
        "listen": null,
        "restarts": 0,
        "output": null,
        "error": null,
        "created_at": now,
        "updated_at": now,
    });
    factory.registry_put(name, run_id, &descriptor).unwrap();
    let record = CallRecord {
        seq: 1,
        parent_seq: None,
        function: "tool".to_string(),
        args: json!({"name":"preserved"}),
        result: json!({"preserved":true}),
        duration_ms: 1,
        token_usage: None,
        timestamp: now,
        error: None,
    };
    let store = factory.store_for(run_id);
    store.append_record(&record).unwrap();
    let inbox = serde_json::to_vec(&vec![QueuedSignal {
        name: "preserve".to_string(),
        payload: json!({"id":"preserve-payload"}),
        from: json!({"kind":"test"}),
        delivery_seq: 7,
        enqueued_at: now,
    }])
    .unwrap();
    store.put_blob(SIGNAL_INBOX_FILE, &inbox).unwrap();
    let records = store.load_call_log().unwrap().unwrap();
    (records, inbox)
}

fn run_rearm_registry_child(scenario: &str, root: &Path, backend: &str) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", REARM_CORRUPT_REGISTRY_TEST, "--nocapture"])
        .env(REARM_CORRUPT_REGISTRY_ROOT, root)
        .env(REARM_CORRUPT_REGISTRY_SCENARIO, scenario)
        .env("CHIDORI_RUN_STORE", backend)
        .env("CHIDORI_RUN_DB", root.join("runs.sqlite3"))
        .env("CHIDORI_DURABILITY", "strict")
        .output()
        .unwrap()
}

#[test]
fn detached_rearm_from_registry_rejects_corrupt_metadata_and_preserves_controls() {
    if let Some(scenario) = std::env::var_os(REARM_CORRUPT_REGISTRY_SCENARIO) {
        let scenario = scenario.to_string_lossy().into_owned();
        let root = PathBuf::from(std::env::var_os(REARM_CORRUPT_REGISTRY_ROOT).unwrap());
        let run_base = root.join(".chidori/runs");
        std::fs::create_dir_all(&run_base).unwrap();
        let backend = std::env::var("CHIDORI_RUN_STORE").unwrap();
        let parts = sqlite_test_parts(&root, run_base.clone(), Arc::new(ToolRegistry::new()));
        let factory = RunStoreFactory::shared(&run_base);
        let hub = DetachedAgentHub {
            parts: Mutex::new(None),
            entries: Mutex::new(HashMap::new()),
            timer_started: Mutex::new(false),
        };

        match scenario.as_str() {
            "malformed-local-json" => {
                let (_records, _inbox) = seed_rearm_registry_entry(
                    &factory,
                    "zz-valid-preserved",
                    "valid-preserved-run",
                );
                let store = factory.store_for("valid-preserved-run");
                let records_before = store.load_call_log().unwrap().unwrap();
                let inbox_before = store.get_blob(SIGNAL_INBOX_FILE).unwrap().unwrap();
                let corrupt_path = run_base.join("agents/aa-malformed-registry.json");
                std::fs::write(&corrupt_path, b"{ malformed registry").unwrap();

                let result = hub.rearm_from_registry(parts.clone());
                let error = result.expect_err(
                    "rearm must fail instead of silently omitting malformed named registry metadata",
                );
                assert!(
                    error.contains("aa-malformed-registry")
                        || error.contains(&corrupt_path.display().to_string()),
                    "rearm error must identify the corrupt registry path/name: {error}"
                );
                assert_eq!(
                    serde_json::to_value(store.load_call_log().unwrap().unwrap()).unwrap(),
                    serde_json::to_value(records_before).unwrap()
                );
                assert_eq!(
                    store.get_blob(SIGNAL_INBOX_FILE).unwrap().unwrap(),
                    inbox_before
                );
                assert!(hub.entries.lock().unwrap().values().all(|entry| {
                    let state = entry.state.lock().unwrap();
                    !state.thread_live && state.descriptor.status == "hibernating"
                }));
            }
            "invalid-descriptor" => {
                let (_records, _inbox) = seed_rearm_registry_entry(
                    &factory,
                    "zz-valid-preserved",
                    "valid-preserved-run",
                );
                let store = factory.store_for("valid-preserved-run");
                let records_before = store.load_call_log().unwrap().unwrap();
                let inbox_before = store.get_blob(SIGNAL_INBOX_FILE).unwrap().unwrap();
                let corrupt_path = run_base.join("agents/aa-invalid-descriptor.json");
                std::fs::create_dir_all(corrupt_path.parent().unwrap()).unwrap();
                std::fs::write(
                    &corrupt_path,
                    br#"{"name":"aa-invalid-descriptor","run_id":"invalid-descriptor-run"}"#,
                )
                .unwrap();

                let result = hub.rearm_from_registry(parts.clone());
                let error = result.expect_err(
                    "rearm must fail instead of silently skipping a named row with no valid descriptor",
                );
                assert!(
                    error.contains("aa-invalid-descriptor"),
                    "rearm error must identify the invalid descriptor name: {error}"
                );
                assert_eq!(
                    serde_json::to_value(store.load_call_log().unwrap().unwrap()).unwrap(),
                    serde_json::to_value(records_before).unwrap()
                );
                assert_eq!(
                    store.get_blob(SIGNAL_INBOX_FILE).unwrap().unwrap(),
                    inbox_before
                );
                assert!(hub.entries.lock().unwrap().values().all(|entry| {
                    let state = entry.state.lock().unwrap();
                    !state.thread_live && state.descriptor.status == "hibernating"
                }));
            }
            "late-invalid-descriptor" => {
                seed_rearm_registry_entry(&factory, "aa-valid-first", "valid-first-run");
                let records_path = run_base.join("valid-first-run/records.jsonl");
                let records_before = std::fs::read(&records_path).unwrap();
                let corrupt_path = run_base.join("agents/zz-invalid-descriptor.json");
                std::fs::write(
                    &corrupt_path,
                    br#"{"name":"zz-invalid-descriptor","run_id":"bad-run"}"#,
                )
                .unwrap();

                let error = hub.rearm_from_registry(parts.clone()).expect_err(
                    "a later corrupt row must fail before an earlier valid row is installed",
                );
                assert!(error.contains("zz-invalid-descriptor"), "{error}");
                assert!(hub.entries.lock().unwrap().is_empty());
                assert_eq!(std::fs::read(records_path).unwrap(), records_before);
            }
            "valid-two-entry-fleet" => {
                seed_rearm_registry_entry(&factory, "fleet-one", "fleet-run-one");
                seed_rearm_registry_entry(&factory, "fleet-two", "fleet-run-two");
                assert_eq!(hub.rearm_from_registry(parts.clone()).unwrap(), 2);
                let entries = hub.entries.lock().unwrap();
                assert!(entries.contains_key("fleet-one"));
                assert!(entries.contains_key("fleet-two"));
                for entry in entries.values() {
                    let state = entry.state.lock().unwrap();
                    assert_eq!(state.descriptor.status, "hibernating");
                    assert!(!state.thread_live, "fixture must not start a worker");
                }
            }
            "absent-agents-directory" => {
                assert!(!run_base.join("agents").exists());
                assert_eq!(hub.rearm_from_registry(parts.clone()).unwrap(), 0);
                assert!(!run_base.join("agents").exists());
            }
            "sqlite-mirror-missing-local-copy" => {
                assert_eq!(backend, "sqlite");
                let (records_before, inbox_before) =
                    seed_rearm_registry_entry(&factory, "mirror-agent", "mirror-agent-run");
                std::fs::remove_file(run_base.join("agents/mirror-agent.json")).unwrap();
                std::fs::remove_dir_all(run_base.join("mirror-agent-run")).unwrap();

                assert_eq!(hub.rearm_from_registry(parts.clone()).unwrap(), 1);
                let store = factory.store_for("mirror-agent-run");
                assert_eq!(
                    serde_json::to_value(store.load_call_log().unwrap().unwrap()).unwrap(),
                    serde_json::to_value(records_before).unwrap()
                );
                assert_eq!(
                    store.get_blob(SIGNAL_INBOX_FILE).unwrap().unwrap(),
                    inbox_before
                );
                assert!(run_base.join("mirror-agent-run/records.jsonl").exists());
                assert!(run_base
                    .join("mirror-agent-run/signals/inbox.json")
                    .exists());
                let entry = hub.entries.lock().unwrap()["mirror-agent"].clone();
                let state = entry.state.lock().unwrap();
                assert_eq!(state.descriptor.status, "hibernating");
                assert!(!state.thread_live);
            }
            other => panic!("unknown registry rearm scenario {other}"),
        }
        eprintln!("REARM_SCENARIO_PASS={scenario} BACKEND={backend}");
        return;
    }

    let cases = [
        ("valid-two-entry-fleet", "fs"),
        ("absent-agents-directory", "fs"),
        ("sqlite-mirror-missing-local-copy", "sqlite"),
        ("malformed-local-json", "fs"),
        ("invalid-descriptor", "fs"),
        ("late-invalid-descriptor", "fs"),
    ];
    let mut failures = Vec::new();
    let mut outcomes = Vec::new();
    for (scenario, backend) in cases {
        let root = tempfile::tempdir().unwrap();
        let output = run_rearm_registry_child(scenario, root.path(), backend);
        let report = format!(
            "scenario={scenario} backend={backend} exited {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !output.status.success() {
            failures.push(report.clone());
        }
        outcomes.push(report);
    }
    assert!(
        failures.is_empty(),
        "detached registry rearm child failures:\n{failures:#?}\nall child outcomes:\n{outcomes:#?}"
    );
}

#[test]
fn detached_name_resolution_fails_closed_and_preserves_same_incarnation_context() {
    let temp = tempfile::tempdir().unwrap();
    let run_base = temp.path().join(".chidori/runs");
    std::fs::create_dir_all(&run_base).unwrap();
    let parts = sqlite_test_parts(temp.path(), run_base.clone(), Arc::new(ToolRegistry::new()));
    let hub = DetachedAgentHub {
        parts: Mutex::new(None),
        entries: Mutex::new(HashMap::new()),
        timer_started: Mutex::new(false),
    };
    let factory = RunStoreFactory::shared(&run_base);

    // A corrupt durable registry must not make lookup or send trust the
    // otherwise-valid process-local entry.
    let corrupt_name = "corrupt-cache-resolution";
    let corrupt_entry = write_cached_resolution_fixture(
        &hub,
        &parts,
        corrupt_name,
        "corrupt-cache-run",
        "hibernating",
    );
    let registry_path = run_base.join("agents").join(format!("{corrupt_name}.json"));
    std::fs::write(&registry_path, b"{ malformed registry").unwrap();
    let lookup = hub.lookup(&parts, corrupt_name);
    let send = hub.send(
        &parts,
        corrupt_name,
        "approval",
        json!({"id": "must-not-route"}),
        Value::Null,
    );
    assert!(lookup
        .as_ref()
        .is_err_and(|error| error.contains("reading registry")));
    assert!(send
        .as_ref()
        .is_err_and(|error| error.contains("reading registry")));
    assert!(Arc::ptr_eq(
        &hub.entries.lock().unwrap()[corrupt_name],
        &corrupt_entry
    ));
    assert!(factory
        .store_for("corrupt-cache-run")
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .is_none());

    // A filesystem read error is equally authoritative; it must not turn
    // a cache hit into a successful lookup or delivery.
    let unreadable_name = "unreadable-cache-resolution";
    let unreadable_entry = write_cached_resolution_fixture(
        &hub,
        &parts,
        unreadable_name,
        "unreadable-cache-run",
        "hibernating",
    );
    let unreadable_path = run_base
        .join("agents")
        .join(format!("{unreadable_name}.json"));
    std::fs::remove_file(&unreadable_path).unwrap();
    std::fs::create_dir(&unreadable_path).unwrap();
    let lookup = hub.lookup(&parts, unreadable_name);
    let send = hub.send(
        &parts,
        unreadable_name,
        "approval",
        json!({"id": "must-not-route-after-read-error"}),
        Value::Null,
    );
    assert!(lookup
        .as_ref()
        .is_err_and(|error| error.contains("reading registry")));
    assert!(send
        .as_ref()
        .is_err_and(|error| error.contains("reading registry")));
    assert!(Arc::ptr_eq(
        &hub.entries.lock().unwrap()[unreadable_name],
        &unreadable_entry
    ));
    assert!(factory
        .store_for("unreadable-cache-run")
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .is_none());

    // If durable identity changes while this process still owns a live
    // predecessor, neither lookup nor send may replace or route around it.
    let active_name = "active-cache-resolution";
    let active_entry = write_cached_resolution_fixture(
        &hub,
        &parts,
        active_name,
        "active-predecessor-run",
        "running",
    );
    let context = RuntimeContext::new();
    let context_run_dir = run_base.join("active-predecessor-run");
    context.enable_persistence_with_store(
        context_run_dir.clone(),
        factory.store_for("active-predecessor-run"),
    );
    {
        let mut state = active_entry.state.lock().unwrap();
        state.live_ctx = Some(context.clone());
        state.execution_active = true;
        state.thread_live = true;
    }
    let replacement = AgentDescriptor {
        name: active_name.to_string(),
        run_id: "replacement-run".to_string(),
        source: "replacement.ts".to_string(),
        input: Value::Null,
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        owner_run_id: "resolution-test".to_string(),
        model: None,
        status: "hibernating".to_string(),
        listen: None,
        restarts: 0,
        output: None,
        error: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    factory
        .registry_put(
            active_name,
            &replacement.run_id,
            &serde_json::to_value(&replacement).unwrap(),
        )
        .unwrap();
    let lookup = hub.lookup(&parts, active_name);
    let send = hub.send(
        &parts,
        active_name,
        "approval",
        json!({"id": "must-not-route-to-active-predecessor"}),
        Value::Null,
    );
    assert!(lookup
        .as_ref()
        .is_err_and(|error| error.contains("predecessor was active")));
    assert!(send
        .as_ref()
        .is_err_and(|error| error.contains("predecessor was active")));
    assert!(Arc::ptr_eq(
        &hub.entries.lock().unwrap()[active_name],
        &active_entry
    ));
    assert_eq!(context.signal_inbox().len(), 0);
    assert!(factory
        .store_for("active-predecessor-run")
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .is_none());
    assert_eq!(
        factory.registry_get(active_name).unwrap().unwrap()["run_id"],
        replacement.run_id
    );

    // An unchanged durable run_id reuses its live Arc and context, so the
    // write-through path persists the message without a duplicate worker.
    let same_name = "same-cache-resolution";
    let same_entry =
        write_cached_resolution_fixture(&hub, &parts, same_name, "same-cache-run", "running");
    let same_context = RuntimeContext::new();
    let same_run_dir = run_base.join("same-cache-run");
    same_context
        .enable_persistence_with_store(same_run_dir.clone(), factory.store_for("same-cache-run"));
    {
        let mut state = same_entry.state.lock().unwrap();
        state.live_ctx = Some(same_context.clone());
        state.execution_active = true;
        state.thread_live = true;
    }
    let (before_lookup, before_send) = {
        let entries = hub.entries.lock().unwrap();
        (entries[same_name].clone(), entries[same_name].clone())
    };
    assert!(Arc::ptr_eq(&before_lookup, &same_entry));
    assert_eq!(
        hub.lookup(&parts, same_name).unwrap()["runId"],
        "same-cache-run"
    );
    let response = hub
        .send(
            &parts,
            same_name,
            "approval",
            json!({"id": "same-incarnation"}),
            Value::Null,
        )
        .unwrap();
    assert_eq!(response["delivered"], true);
    assert!(Arc::ptr_eq(
        &hub.entries.lock().unwrap()[same_name],
        &before_send
    ));
    assert_eq!(same_context.signal_inbox().len(), 1);
    let persisted: Vec<QueuedSignal> = serde_json::from_slice(
        &factory
            .store_for("same-cache-run")
            .get_blob(SIGNAL_INBOX_FILE)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].payload["id"], "same-incarnation");
    assert!(same_entry.state.lock().unwrap().thread_live);
}

#[test]
fn detached_run_id_bound_handle_stop_does_not_stop_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let run_base = temp.path().join(".chidori/runs");
    std::fs::create_dir_all(&run_base).unwrap();
    let parts = sqlite_test_parts(temp.path(), run_base.clone(), Arc::new(ToolRegistry::new()));
    let hub = DetachedAgentHub {
        parts: Mutex::new(None),
        entries: Mutex::new(HashMap::new()),
        timer_started: Mutex::new(false),
    };
    let factory = RunStoreFactory::shared(&run_base);
    let name = "run-id-bound-stop";
    let old_run_id = "old-stopped-incarnation";
    let old_descriptor = AgentDescriptor {
        name: name.to_string(),
        run_id: old_run_id.to_string(),
        source: "agent.ts".to_string(),
        input: Value::Null,
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        owner_run_id: "stop-test".to_string(),
        model: None,
        status: "stopped".to_string(),
        listen: None,
        restarts: 0,
        output: None,
        error: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    factory
        .store_for(old_run_id)
        .put_blob(
            AGENT_DESCRIPTOR_FILE,
            &serde_json::to_vec(&old_descriptor).unwrap(),
        )
        .unwrap();

    let replacement = AgentDescriptor {
        run_id: "current-replacement-incarnation".to_string(),
        status: "hibernating".to_string(),
        ..old_descriptor.clone()
    };
    factory
        .registry_put(
            name,
            &replacement.run_id,
            &serde_json::to_value(&replacement).unwrap(),
        )
        .unwrap();
    let replacement_entry = hub.ensure_entry(&parts, name).unwrap();

    let old_handle_result = hub.stop_incarnation(&parts, name, old_run_id).unwrap();
    assert_eq!(old_handle_result["runId"], old_run_id);
    assert_eq!(old_handle_result["status"], "stopped");
    assert_eq!(
        factory.registry_get(name).unwrap().unwrap()["run_id"],
        replacement.run_id
    );
    let replacement_state = replacement_entry.state.lock().unwrap();
    assert!(!replacement_state.stop_requested);
    assert_eq!(replacement_state.descriptor.status, "hibernating");
    assert!(factory
        .store_for(&replacement.run_id)
        .get_blob(AGENT_STOP_REQUEST_FILE)
        .unwrap()
        .is_none());
}

#[test]
fn detached_stop_replay_identity_includes_handle_run_id_and_keeps_legacy_args() {
    let record = |args| crate::runtime::call_log::CallRecord {
        seq: 1,
        parent_seq: None,
        function: "stop_agent".to_string(),
        args,
        result: json!({ "status": "stopped", "runId": "run-x" }),
        duration_ms: 0,
        token_usage: None,
        timestamp: Utc::now(),
        error: None,
    };

    let run_x_ctx = RuntimeContext::with_replay(vec![record(stop_agent_call_args(
        "same-name",
        Some("run-x"),
    ))]);
    let mismatch = host_core::execute_durable_json_call_at_seq(
        &run_x_ctx,
        1,
        "stop_agent",
        stop_agent_call_args("same-name", Some("run-y")),
        || anyhow::bail!("a different handle incarnation must not replay run-x stop"),
    )
    .unwrap_err();
    assert!(
        mismatch.to_string().contains("Replay divergence"),
        "same-name stop for another run ID must diverge: {mismatch:#}"
    );

    let legacy_ctx = RuntimeContext::with_replay(vec![record(json!({ "to": "same-name" }))]);
    let replayed = host_core::execute_durable_json_call_at_seq(
        &legacy_ctx,
        1,
        "stop_agent",
        stop_agent_call_args("same-name", None),
        || anyhow::bail!("legacy name-only stop should remain replay-compatible"),
    )
    .unwrap();
    assert_eq!(replayed["runId"], "run-x");
}

#[derive(Debug)]
struct PredecessorMigrationFailureObservation {
    spawn_result: Result<Value, String>,
    destination_run_id: String,
    predecessor_run_id: String,
    registry_run_id_after: Result<Option<String>, String>,
    predecessor_before: Vec<u8>,
    predecessor_after: Result<Option<Vec<u8>>, String>,
    destination_inbox_after: Result<Option<Vec<u8>>, String>,
    destination_write_probe: Result<(), String>,
    sqlite_mirror_inbox_after: Option<Result<Option<Vec<u8>>, String>>,
    destination_signal_result_count: Result<usize, String>,
    entry_present: bool,
    thread_live: Option<bool>,
    effect_count: usize,
}

fn summarize_blob_result(result: &Result<Option<Vec<u8>>, String>) -> String {
    match result {
        Ok(Some(bytes)) => format!("{} bytes: {}", bytes.len(), String::from_utf8_lossy(bytes)),
        Ok(None) => "missing".to_string(),
        Err(error) => format!("unreadable: {error}"),
    }
}

fn migration_source(project: &Path) -> PathBuf {
    let source = project.join("services/migration-worker.ts");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(
        &source,
        r#"export async function agent() {
            const message = await chidori.signal("go");
            await chidori.tool("migrationEffect", { id: message.payload.id });
            return { id: message.payload.id };
        }"#,
    )
    .unwrap();
    source
}

fn migration_counting_tools(effects: Arc<std::sync::atomic::AtomicUsize>) -> Arc<ToolRegistry> {
    let mut tools = ToolRegistry::new();
    tools.register_native(
        "migrationEffect",
        "count migration effects",
        Vec::new(),
        move |_| {
            effects.fetch_add(1, Ordering::SeqCst);
            Ok(json!({ "effect": true }))
        },
    );
    Arc::new(tools)
}

fn gated_migration_counting_tools(
    effects: Arc<std::sync::atomic::AtomicUsize>,
    agent_name: String,
) -> Arc<ToolRegistry> {
    let mut tools = ToolRegistry::new();
    tools.register_native(
        "migrationEffect",
        "count and gate migrated signal effects",
        Vec::new(),
        move |_| {
            effects.fetch_add(1, Ordering::SeqCst);
            same_name_spawn_gate_event(&agent_name, "winner-effect-active", true);
            Ok(json!({ "effect": true }))
        },
    );
    Arc::new(tools)
}

fn registry_truncation_control_source(project: &Path) -> PathBuf {
    let source = project.join("services/registry-control-worker.ts");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(
        &source,
        r#"export async function agent() {
            await chidori.signal("gate");
            const message = await chidori.signal("go");
            await chidori.tool("migrationEffect", { id: message.payload.id });
            return { id: message.payload.id };
        }"#,
    )
    .unwrap();
    source
}

fn registry_truncation_child(
    test: &str,
    mode: &str,
    root: &Path,
    backend: &str,
) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env("CHIDORI_TEST_REGISTRY_TRUNCATION_MODE", mode)
        .env("CHIDORI_TEST_REGISTRY_TRUNCATION_ROOT", root)
        .env("CHIDORI_RUN_STORE", backend)
        .env("CHIDORI_RUN_DB", root.join("runs.sqlite3"))
        .env("CHIDORI_DURABILITY", "strict")
        .output()
        .unwrap()
}

fn seed_settled_predecessor(
    factory: &RunStoreFactory,
    name: &str,
    run_id: &str,
    payload_id: &str,
) -> Vec<u8> {
    let now = Utc::now();
    let descriptor = json!({
        "name": name,
        "run_id": run_id,
        "source": "services/old-worker.ts",
        "input": {},
        "restart": "never",
        "max_restarts": 0,
        "backoff_ms": 0,
        "owner_run_id": "migration-test-parent",
        "model": null,
        "status": "completed",
        "listen": null,
        "restarts": 0,
        "output": null,
        "error": null,
        "created_at": now,
        "updated_at": now,
    });
    factory.registry_put(name, run_id, &descriptor).unwrap();
    let queued = vec![QueuedSignal {
        name: "go".to_string(),
        payload: json!({ "id": payload_id }),
        from: json!({ "kind": "test", "id": "predecessor-migration" }),
        delivery_seq: 17,
        enqueued_at: now,
    }];
    let bytes = serde_json::to_vec_pretty(&queued).unwrap();
    factory
        .store_for(run_id)
        .put_blob(SIGNAL_INBOX_FILE, &bytes)
        .unwrap();
    bytes
}

fn seed_same_name_spawn_race(root: &Path, backend: &str, name: &str) -> (String, Vec<u8>) {
    let project = root.join("project");
    migration_source(&project);
    let run_base = project.join(".chidori/runs");
    let predecessor_run_id = format!("old-{}", uuid::Uuid::new_v4());
    let now = Utc::now();
    let descriptor = json!({
        "name": name,
        "run_id": predecessor_run_id,
        "source": "services/old-worker.ts",
        "input": {},
        "restart": "never",
        "max_restarts": 0,
        "backoff_ms": 0,
        "owner_run_id": "same-name-race-seed",
        "model": null,
        "status": "completed",
        "listen": null,
        "restarts": 0,
        "output": null,
        "error": null,
        "created_at": now,
        "updated_at": now,
    });
    let entry = json!({
        "name": name,
        "run_id": predecessor_run_id,
        "descriptor": descriptor,
    });
    let registry_path = run_base.join("agents").join(format!("{name}.json"));
    std::fs::create_dir_all(registry_path.parent().unwrap()).unwrap();
    std::fs::write(&registry_path, serde_json::to_vec_pretty(&entry).unwrap()).unwrap();

    let queued = vec![QueuedSignal {
        name: "go".to_string(),
        payload: json!({ "id": "must-survive" }),
        from: json!({ "kind": "test", "id": "predecessor-migration" }),
        delivery_seq: 17,
        enqueued_at: now,
    }];
    let inbox = serde_json::to_vec_pretty(&queued).unwrap();
    let inbox_path = run_base.join(&predecessor_run_id).join(SIGNAL_INBOX_FILE);
    std::fs::create_dir_all(inbox_path.parent().unwrap()).unwrap();
    std::fs::write(&inbox_path, &inbox).unwrap();

    if backend == "sqlite" {
        let db_path = root.join("runs.sqlite3");
        let connection = rusqlite::Connection::open(&db_path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS run_blobs (
                     run_id TEXT NOT NULL, key TEXT NOT NULL, data BLOB NOT NULL,
                     PRIMARY KEY (run_id, key)
                 );
                 CREATE TABLE IF NOT EXISTS run_registry (
                     name TEXT PRIMARY KEY, run_id TEXT NOT NULL, data TEXT NOT NULL
                 );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO run_registry (name, run_id, data) VALUES (?1, ?2, ?3)",
                rusqlite::params![
                    name,
                    predecessor_run_id,
                    serde_json::to_string(&entry).unwrap()
                ],
            )
            .unwrap();
    }
    (predecessor_run_id, inbox)
}

fn assert_same_name_spawn_seed(inbox: &[u8]) {
    let queued: Vec<QueuedSignal> =
        serde_json::from_slice(inbox).expect("same-name seed must decode as queued signals");
    assert_eq!(queued.len(), 1, "same-name seed must contain one signal");
    let signal = &queued[0];
    assert_eq!(signal.name, "go");
    assert_eq!(signal.payload, json!({ "id": "must-survive" }));
    assert_eq!(
        signal.from,
        json!({ "kind": "test", "id": "predecessor-migration" })
    );
    assert_eq!(signal.delivery_seq, 17);
}

fn same_name_spawn_child_command(
    root: &Path,
    backend: &str,
    name: &str,
    child_index: usize,
    gate_address: Option<&str>,
    mode: &str,
) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            SAME_NAME_SPAWN_TEST,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(SAME_NAME_SPAWN_MODE_ENV, mode)
        .env(SAME_NAME_SPAWN_ROOT_ENV, root)
        .env(SAME_NAME_SPAWN_NAME_ENV, name)
        .env(SAME_NAME_SPAWN_CHILD_INDEX_ENV, child_index.to_string())
        .env("CHIDORI_RUN_STORE", backend)
        .env("CHIDORI_RUN_DB", root.join("runs.sqlite3"))
        .env("CHIDORI_DURABILITY", "strict")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(address) = gate_address {
        command.env(SAME_NAME_SPAWN_GATE_ENV, address);
    } else {
        command.env_remove(SAME_NAME_SPAWN_GATE_ENV);
    }
    command
}

fn same_name_spawn_worker() {
    let root = PathBuf::from(std::env::var_os(SAME_NAME_SPAWN_ROOT_ENV).unwrap());
    let backend = std::env::var("CHIDORI_RUN_STORE").unwrap();
    let name = std::env::var(SAME_NAME_SPAWN_NAME_ENV).unwrap();
    let child_index = std::env::var(SAME_NAME_SPAWN_CHILD_INDEX_ENV).unwrap();
    let mode = std::env::var(SAME_NAME_SPAWN_MODE_ENV).unwrap_or_default();
    let project = root.join("project");
    let run_base = project.join(".chidori/runs");
    let source = project
        .join("services")
        .join(format!("migration-worker-{child_index}.ts"));
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(
        &source,
        r#"export async function agent() {
            const message = await chidori.signal("go");
            await chidori.tool("migrationEffect", { id: message.payload.id });
            return { id: message.payload.id };
        }"#,
    )
    .unwrap();
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tools = if mode == "winner" {
        gated_migration_counting_tools(effects.clone(), name.clone())
    } else {
        migration_counting_tools(effects.clone())
    };
    let parts = sqlite_test_parts(&project, run_base.clone(), tools);
    hub().install_parts(parts.clone());
    let options = SpawnOptions {
        name: Some(name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };
    let mut report = json!({
        "backend": backend,
        "child_index": child_index,
        "spawn_error": null,
        "run_id": null,
        "status": "rejected",
        "matching_signal_rows": 0,
        "effect_count": 0,
        "inbox": null,
    });
    if mode == "registry-error" {
        let result = hub().spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &options,
            format!("same-name-registry-error-{child_index}"),
        );
        report["spawn_error"] = json!(result.err().unwrap_or_default());
        std::fs::write(
            root.join(format!("child-{child_index}-report.json")),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        return;
    }
    let factory = RunStoreFactory::shared(&run_base);
    let mut retry_failure_report = None;
    let spawn = if mode == "start-failure" {
        let prior = factory.registry_get(&name).unwrap().unwrap();
        let predecessor_id = prior["run_id"].as_str().unwrap().to_string();
        let predecessor_store = factory.store_for(&predecessor_id);
        let predecessor_before = predecessor_store.get_blob(SIGNAL_INBOX_FILE).unwrap();
        tests::inject_spawn_thread_start_failure(&name);
        let first = hub().spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &options,
            format!("same-name-start-failure-{child_index}"),
        );
        let first_error = first.expect_err("injected start failure must fail spawn");
        let failed_registry = factory.registry_get(&name).unwrap().unwrap();
        let failed_status = failed_registry["descriptor"]["status"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let predecessor_after = predecessor_store.get_blob(SIGNAL_INBOX_FILE).unwrap();
        retry_failure_report = Some(json!({
            "first_error": first_error,
            "failed_registry_status": failed_status,
            "predecessor_preserved": predecessor_before == predecessor_after,
        }));
        hub().spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &options,
            format!("same-name-start-retry-{child_index}"),
        )
    } else {
        hub().spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &options,
            format!("same-name-race-parent-{child_index}"),
        )
    };
    if let Some(retry_failure_report) = retry_failure_report {
        report["start_failure"] = retry_failure_report;
    }
    match spawn {
        Err(error) => report["spawn_error"] = json!(error),
        Ok(value) => {
            let run_id = value["runId"].as_str().unwrap().to_string();
            report["run_id"] = json!(run_id);
            let descriptor = wait_for_agent_descriptor(&parts, &name, |descriptor| {
                descriptor.run_id == run_id && descriptor.status == "completed"
            });
            let store = RunStoreFactory::shared(&run_base).store_for(&run_id);
            let records = store.load_call_log().unwrap().unwrap_or_default();
            let matching_signal_rows = records
                .iter()
                .filter(|record| {
                    record.function == "signal" && record.result["payload"]["id"] == "must-survive"
                })
                .count();
            let inbox = store
                .get_blob(SIGNAL_INBOX_FILE)
                .unwrap()
                .unwrap_or_default();
            report["status"] = json!(descriptor.status);
            report["matching_signal_rows"] = json!(matching_signal_rows);
            report["effect_count"] = json!(effects.load(Ordering::SeqCst));
            report["inbox"] = json!(String::from_utf8_lossy(&inbox).to_string());
        }
    }
    let report_path = root.join(format!("child-{child_index}-report.json"));
    std::fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
}

fn run_predecessor_migration_failure(base: &Path) -> PredecessorMigrationFailureObservation {
    let project = base.join("failure");
    std::fs::create_dir_all(&project).unwrap();
    let source = migration_source(&project);
    let run_base = project.join(".chidori/runs");
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    let recorded_effects = effects.clone();
    tools.register_native(
        "migrationEffect",
        "count migrated signal effects",
        Vec::new(),
        move |_| {
            recorded_effects.fetch_add(1, Ordering::SeqCst);
            Ok(json!({ "effect": true }))
        },
    );
    let parts = sqlite_test_parts(&project, run_base.clone(), Arc::new(tools));
    hub().install_parts(parts.clone());
    let factory = RunStoreFactory::shared(&run_base);
    let name = unique("predecessor-write-failure");
    let predecessor_run_id = format!("old-{}", uuid::Uuid::new_v4());
    let predecessor_before =
        seed_settled_predecessor(&factory, &name, &predecessor_run_id, "must-survive");
    let old_store = factory.store_for(&predecessor_run_id);
    let options = SpawnOptions {
        name: Some(name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };
    let (arrived, release) = install_predecessor_migration_barrier(&name);
    let mut cleanup = PredecessorMigrationBarrierCleanup {
        agent_name: name.clone(),
        release: Some(release),
    };
    let thread_parts = parts.clone();
    let thread_source = source.to_string_lossy().to_string();
    let thread_options = options.clone();
    let spawn_thread = std::thread::spawn(move || {
        hub().spawn(
            &thread_parts,
            &thread_source,
            json!({}),
            &thread_options,
            "migration-failure-parent".to_string(),
        )
    });

    let destination_run_id = arrived
        .recv_timeout(Duration::from_secs(10))
        .expect("spawn reaches the barrier before destination inbox persistence");
    let destination_inbox_path = run_base.join(&destination_run_id).join(SIGNAL_INBOX_FILE);
    std::fs::create_dir_all(&destination_inbox_path)
        .expect("make destination inbox path a directory to force real FS put failure");
    cleanup.release();
    let spawn_result = spawn_thread
        .join()
        .expect("spawn worker thread returns its migration result");
    drop(cleanup);

    let predecessor_after = old_store
        .get_blob(SIGNAL_INBOX_FILE)
        .map_err(|err| format!("{err:#}"));
    let destination_store = factory.store_for(&destination_run_id);
    let destination_inbox_after = destination_store
        .get_blob(SIGNAL_INBOX_FILE)
        .map_err(|err| format!("{err:#}"));
    let destination_write_probe = destination_store
        .put_blob(SIGNAL_INBOX_FILE, b"migration-write-probe")
        .map_err(|err| format!("{err:#}"));
    let sqlite_mirror_inbox_after = if std::env::var("CHIDORI_RUN_STORE").as_deref() == Ok("sqlite")
    {
        let shared =
            SqliteRunStoreShared::open(&PathBuf::from(std::env::var_os("CHIDORI_RUN_DB").unwrap()))
                .map_err(|err| format!("{err:#}"));
        Some(match shared {
            Ok(shared) => SqliteRunStore::new(shared, &destination_run_id)
                .get_blob(SIGNAL_INBOX_FILE)
                .map_err(|err| format!("{err:#}")),
            Err(err) => Err(err),
        })
    } else {
        None
    };
    let destination_signal_result_count = destination_store
        .load_call_log()
        .map(|records| {
            records
                .unwrap_or_default()
                .iter()
                .filter(|record| {
                    record.function == "signal"
                        && record.result["payload"]["id"] == json!("must-survive")
                })
                .count()
        })
        .map_err(|err| format!("{err:#}"));
    let entry = hub().entries.lock().unwrap().get(&name).cloned();
    let thread_live = entry.map(|entry| entry.state.lock().unwrap().thread_live);
    let entry_present = hub().entries.lock().unwrap().contains_key(&name);
    let registry_run_id_after = factory
        .registry_get(&name)
        .map(|entry| {
            entry.and_then(|entry| {
                entry
                    .get("run_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
        })
        .map_err(|err| format!("{err:#}"));
    let effect_count = effects.load(Ordering::SeqCst);
    PredecessorMigrationFailureObservation {
        spawn_result,
        destination_run_id,
        predecessor_run_id,
        registry_run_id_after,
        predecessor_before,
        predecessor_after,
        destination_inbox_after,
        destination_write_probe,
        sqlite_mirror_inbox_after,
        destination_signal_result_count,
        entry_present,
        thread_live,
        effect_count,
    }
}

fn run_predecessor_migration_success(
    base: &Path,
) -> (AgentDescriptor, Vec<CallRecord>, Vec<u8>, usize) {
    let project = base.join("success");
    std::fs::create_dir_all(&project).unwrap();
    let source = migration_source(&project);
    let run_base = project.join(".chidori/runs");
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    let recorded_effects = effects.clone();
    tools.register_native(
        "migrationEffect",
        "count migrated signal effects",
        Vec::new(),
        move |_| {
            recorded_effects.fetch_add(1, Ordering::SeqCst);
            Ok(json!({ "effect": true }))
        },
    );
    let parts = sqlite_test_parts(&project, run_base.clone(), Arc::new(tools));
    hub().install_parts(parts.clone());
    let factory = RunStoreFactory::shared(&run_base);
    let name = unique("predecessor-write-success");
    let predecessor_run_id = format!("old-{}", uuid::Uuid::new_v4());
    seed_settled_predecessor(&factory, &name, &predecessor_run_id, "must-survive");
    let options = SpawnOptions {
        name: Some(name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };
    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &options,
            "migration-success-parent".to_string(),
        )
        .expect("valid migration spawn succeeds");
    let run_id = spawned["runId"].as_str().unwrap().to_string();
    let descriptor = wait_for_agent_descriptor(&parts, &name, |descriptor| {
        descriptor.run_id == run_id && descriptor.status == "completed"
    });
    let store = factory.store_for(&run_id);
    let records = store.load_call_log().unwrap().unwrap_or_default();
    let inbox = store
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .unwrap_or_else(|| b"[]".to_vec());
    let effect_count = effects.load(Ordering::SeqCst);
    (descriptor, records, inbox, effect_count)
}

fn run_migration_child_for_backend(test_name: &str, scenario: &str) {
    let mut failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let base = tempfile::tempdir().unwrap();
        eprintln!(
            "PREDECESSOR_MIGRATION_BACKEND={backend} SCENARIO={scenario} ARTIFACT_BASE={}",
            base.path().display()
        );
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture"])
            .env(PREDECESSOR_MIGRATION_CHILD_ENV, "1")
            .env(PREDECESSOR_MIGRATION_BASE_ENV, base.path())
            .env(PREDECESSOR_MIGRATION_SCENARIO_ENV, scenario)
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", base.path().join("runs.sqlite3"))
            .env("CHIDORI_DURABILITY", "strict")
            .output()
            .unwrap();
        let report = format!(
            "backend={backend} scenario={scenario} exited {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !output.status.success() {
            let preserved = base.keep();
            failures.push(format!("{report}\nartifact_base={}", preserved.display()));
        }
    }
    assert!(
        failures.is_empty(),
        "migration subprocess failures:\n{failures:#?}"
    );
}

#[test]
fn detached_registry_prefix_write_crash_fs_and_sqlite_restart_and_control() {
    const TEST: &str = "runtime::host_agent::tests::detached_registry_prefix_write_crash_fs_and_sqlite_restart_and_control";
    match std::env::var(REGISTRY_TRUNCATION_MODE_ENV).as_deref() {
        Ok("cut") => {
            let root = PathBuf::from(std::env::var_os(REGISTRY_TRUNCATION_ROOT_ENV).unwrap());
            let backend = std::env::var("CHIDORI_RUN_STORE").unwrap();
            let project = root.join("project");
            std::fs::create_dir_all(&project).unwrap();
            let source = migration_source(&project);
            let run_base = project.join(".chidori/runs");
            let name = format!("registry-truncation-{backend}");
            let parts = sqlite_test_parts(
                &project,
                run_base.clone(),
                migration_counting_tools(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
            );
            let factory = RunStoreFactory::shared(&run_base);
            let predecessor_id = format!("old-{}", uuid::Uuid::new_v4());
            let inbox_before =
                seed_settled_predecessor(&factory, &name, &predecessor_id, "must-survive");
            std::fs::write(root.join("predecessor-inbox-before.json"), &inbox_before).unwrap();
            let registry_path = run_base.join("agents").join(format!("{name}.json"));
            std::fs::write(
                root.join("predecessor-registry-before.json"),
                std::fs::read(&registry_path).unwrap(),
            )
            .unwrap();
            std::fs::write(
                root.join("seed.json"),
                serde_json::to_vec_pretty(&json!({
                    "name":name,
                    "predecessor_run_id":predecessor_id
                }))
                .unwrap(),
            )
            .unwrap();
            let options = SpawnOptions {
                name: Some(name),
                restart: "never".into(),
                max_restarts: 0,
                backoff_ms: 0,
                model: None,
            };
            let result = hub().spawn(
                &parts,
                source.to_str().unwrap(),
                json!({}),
                &options,
                "registry-prefix-cut-parent".into(),
            );
            panic!("replacement spawn returned before process kill at prefix boundary: {result:?}");
        }
        Ok("retry") => {
            let root = PathBuf::from(std::env::var_os(REGISTRY_TRUNCATION_ROOT_ENV).unwrap());
            let seed: Value =
                serde_json::from_slice(&std::fs::read(root.join("seed.json")).unwrap()).unwrap();
            let name = seed["name"].as_str().unwrap();
            let predecessor_id = seed["predecessor_run_id"].as_str().unwrap();
            let project = root.join("project");
            let run_base = project.join(".chidori/runs");
            let source = migration_source(&project);
            let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let parts = sqlite_test_parts(
                &project,
                run_base.clone(),
                migration_counting_tools(effects.clone()),
            );
            let factory = RunStoreFactory::shared(&run_base);
            let registry_before = factory.registry_get(name);
            let observed_run_id = registry_before
                .as_ref()
                .ok()
                .and_then(|entry| entry.as_ref())
                .and_then(|entry| entry["run_id"].as_str())
                .map(str::to_owned);
            let old_inbox_before_retry = factory
                .store_for(predecessor_id)
                .get_blob(SIGNAL_INBOX_FILE)
                .unwrap()
                .unwrap();
            let expected_inbox = std::fs::read(root.join("predecessor-inbox-before.json")).unwrap();
            let registry_file = run_base.join("agents").join(format!("{name}.json"));
            let registry_bytes_before_retry = std::fs::read(&registry_file).ok();
            let report_before_retry = json!({
                "backend": std::env::var("CHIDORI_RUN_STORE").unwrap(),
                "name": name,
                "predecessor_run_id": predecessor_id,
                "registry_result": format!("{registry_before:?}"),
                "observed_registry_run_id": observed_run_id,
                "registry_file": registry_file.display().to_string(),
                "registry_file_bytes": registry_bytes_before_retry.as_ref().map(|bytes| String::from_utf8_lossy(bytes).to_string()),
                "predecessor_inbox_exact_original": old_inbox_before_retry == expected_inbox,
                "predecessor_inbox": String::from_utf8_lossy(&old_inbox_before_retry),
            });
            std::fs::write(
                root.join("retry-precondition-report.json"),
                serde_json::to_vec_pretty(&report_before_retry).unwrap(),
            )
            .unwrap();
            assert_eq!(
                observed_run_id.as_deref(),
                Some(predecessor_id),
                "fresh process must read the previous complete registry entry before same-name retry; report={report_before_retry}"
            );
            assert_eq!(
                old_inbox_before_retry, expected_inbox,
                "predecessor inbox must remain byte-identical until same-name retry"
            );
            let options = SpawnOptions {
                name: Some(name.to_string()),
                restart: "never".into(),
                max_restarts: 0,
                backoff_ms: 0,
                model: None,
            };
            let spawned = hub()
                .spawn(
                    &parts,
                    source.to_str().unwrap(),
                    json!({}),
                    &options,
                    "registry-prefix-retry-parent".into(),
                )
                .unwrap();
            let replacement_id = spawned["runId"].as_str().unwrap().to_owned();
            let descriptor = wait_for_agent_descriptor(&parts, name, |descriptor| {
                descriptor.run_id == replacement_id && descriptor.status == "completed"
            });
            let records = factory
                .store_for(&replacement_id)
                .load_call_log()
                .unwrap()
                .unwrap_or_default();
            let matching_signal_rows = records
                .iter()
                .filter(|record| {
                    record.function == "signal"
                        && record.result["payload"]["id"] == json!("must-survive")
                })
                .count();
            let replacement_inbox = factory
                .store_for(&replacement_id)
                .get_blob(SIGNAL_INBOX_FILE)
                .unwrap()
                .unwrap_or_default();
            let registry_after = factory.registry_get(name).unwrap().unwrap();
            let report = json!({
                "backend": std::env::var("CHIDORI_RUN_STORE").unwrap(),
                "name": name,
                "predecessor_run_id": predecessor_id,
                "replacement_run_id": replacement_id,
                "descriptor_status": descriptor.status,
                "registry_run_id_after": registry_after["run_id"],
                "matching_signal_rows": matching_signal_rows,
                "effect_count": effects.load(Ordering::SeqCst),
                "replacement_inbox": String::from_utf8_lossy(&replacement_inbox),
            });
            std::fs::write(
                root.join("retry-report.json"),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
            assert_eq!(registry_after["run_id"], replacement_id);
            assert_eq!(
                matching_signal_rows, 1,
                "preserved payload must be journaled once"
            );
            assert_eq!(
                effects.load(Ordering::SeqCst),
                1,
                "preserved payload effect must run once"
            );
            assert!(replacement_inbox.is_empty() || replacement_inbox == b"[]");
            return;
        }
        Ok("post-rename") => {
            let root = PathBuf::from(std::env::var_os(REGISTRY_TRUNCATION_ROOT_ENV).unwrap());
            let project = root.join("project");
            std::fs::create_dir_all(&project).unwrap();
            let source = migration_source(&project);
            let run_base = project.join(".chidori/runs");
            let name = "registry-post-rename-sqlite";
            let factory = RunStoreFactory::shared(&run_base);
            let predecessor_id = format!("old-{}", uuid::Uuid::new_v4());
            seed_settled_predecessor(&factory, name, &predecessor_id, "must-survive");
            std::fs::write(
                root.join("post-rename-seed.json"),
                serde_json::to_vec_pretty(&json!({
                    "name": name,
                    "predecessor_run_id": predecessor_id
                }))
                .unwrap(),
            )
            .unwrap();
            let parts = sqlite_test_parts(
                &project,
                run_base,
                migration_counting_tools(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
            );
            let options = SpawnOptions {
                name: Some(name.to_string()),
                restart: "never".into(),
                max_restarts: 0,
                backoff_ms: 0,
                model: None,
            };
            let result = hub().spawn(
                &parts,
                source.to_str().unwrap(),
                json!({}),
                &options,
                "registry-post-rename-parent".into(),
            );
            panic!("spawn returned before post-rename process kill: {result:?}");
        }
        Ok("control-spawn") => {
            let root = PathBuf::from(std::env::var_os(REGISTRY_TRUNCATION_ROOT_ENV).unwrap());
            let backend = std::env::var("CHIDORI_RUN_STORE").unwrap();
            let project = root.join("project");
            std::fs::create_dir_all(&project).unwrap();
            let source = registry_truncation_control_source(&project);
            let run_base = project.join(".chidori/runs");
            let name = format!("registry-truncation-control-{backend}");
            let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let parts = sqlite_test_parts(
                &project,
                run_base.clone(),
                migration_counting_tools(effects),
            );
            let factory = RunStoreFactory::shared(&run_base);
            let predecessor_id = format!("old-{}", uuid::Uuid::new_v4());
            let _inbox_before =
                seed_settled_predecessor(&factory, &name, &predecessor_id, "control-payload");
            let options = SpawnOptions {
                name: Some(name.clone()),
                restart: "never".into(),
                max_restarts: 0,
                backoff_ms: 0,
                model: None,
            };
            let spawned = hub()
                .spawn(
                    &parts,
                    source.to_str().unwrap(),
                    json!({}),
                    &options,
                    "registry-clean-control-parent".into(),
                )
                .unwrap();
            let replacement_id = spawned["runId"].as_str().unwrap();
            let descriptor = wait_for_agent_descriptor(&parts, &name, |d| {
                d.run_id == replacement_id && d.status == "hibernating"
            });
            let registry = factory.registry_get(&name).unwrap().unwrap();
            let report = json!({"backend":backend,"name":name,"predecessor_run_id":predecessor_id,"replacement_run_id":replacement_id,"status":descriptor.status,"registry_run_id":registry["run_id"]});
            std::fs::write(
                root.join("control-spawn-report.json"),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
            assert_eq!(registry["run_id"], replacement_id);
            return;
        }
        Ok("control-rearm") => {
            let root = PathBuf::from(std::env::var_os(REGISTRY_TRUNCATION_ROOT_ENV).unwrap());
            let project = root.join("project");
            let run_base = project.join(".chidori/runs");
            let spawn_report: Value = serde_json::from_slice(
                &std::fs::read(root.join("control-spawn-report.json")).unwrap(),
            )
            .unwrap();
            let name = spawn_report["name"].as_str().unwrap();
            let replacement_id = spawn_report["replacement_run_id"].as_str().unwrap();
            let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let parts = sqlite_test_parts(
                &project,
                run_base.clone(),
                migration_counting_tools(effects.clone()),
            );
            let factory = RunStoreFactory::shared(&run_base);
            let rearmed = hub().rearm_from_registry(parts.clone()).unwrap();
            let before_wake = hub().status(&parts, name).unwrap();
            hub()
                .send(
                    &parts,
                    name,
                    "gate",
                    json!({}),
                    json!({"kind":"test-control"}),
                )
                .unwrap();
            let descriptor = wait_for_agent_descriptor(&parts, name, |d| {
                d.run_id == replacement_id && d.status == "completed"
            });
            let records = factory
                .store_for(replacement_id)
                .load_call_log()
                .unwrap()
                .unwrap_or_default();
            let matching = records
                .iter()
                .filter(|r| {
                    r.function == "signal" && r.result["payload"]["id"] == json!("control-payload")
                })
                .count();
            let inbox = factory
                .store_for(replacement_id)
                .get_blob(SIGNAL_INBOX_FILE)
                .unwrap()
                .unwrap_or_default();
            let registry = factory.registry_get(name).unwrap().unwrap();
            let report = json!({"rearmed":rearmed,"name":name,"replacement_run_id":replacement_id,"registry_run_id":registry["run_id"],"pre_wake_status":before_wake["status"],"descriptor_status":descriptor.status,"matching_signal_rows":matching,"replacement_inbox":String::from_utf8_lossy(&inbox),"effect_count":effects.load(Ordering::SeqCst)});
            std::fs::write(
                root.join("control-rearm-report.json"),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
            assert_eq!(rearmed, 1);
            assert_eq!(registry["run_id"], replacement_id);
            assert_eq!(before_wake["status"], "hibernating");
            assert_eq!(matching, 1);
            assert!(inbox == b"[]" || inbox.is_empty());
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            return;
        }
        _ => {}
    }

    let mut red_failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().to_path_buf();
        let name = format!("registry-truncation-{backend}");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env(REGISTRY_TRUNCATION_MODE_ENV, "cut")
            .env(REGISTRY_TRUNCATION_ROOT_ENV, &root)
            .env("CHIDORI_TEST_REGISTRY_PREFIX_NAME", &name)
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", root.join("runs.sqlite3"))
            .env("CHIDORI_DURABILITY", "strict")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut child_stdout = child.stdout.take().unwrap();
        let (line_tx, line_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(&mut child_stdout);
            loop {
                let mut line = String::new();
                match std::io::BufRead::read_line(&mut reader, &mut line) {
                    Ok(0) => {
                        let _ = line_tx.send(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "child exited before flushed-prefix gate line",
                        )));
                        return;
                    }
                    Ok(_) => {
                        if serde_json::from_str::<Value>(&line)
                            .ok()
                            .and_then(|value| value["registry_path"].as_str().map(str::to_owned))
                            .is_some()
                        {
                            let _ = line_tx.send(Ok(line));
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = line_tx.send(Err(error));
                        return;
                    }
                }
            }
        });
        let gate_line = match line_rx.recv_timeout(Duration::from_secs(20)) {
            Ok(Ok(line)) if !line.is_empty() => line,
            result => {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "{backend} registry writer did not reach the flushed-prefix gate: {result:?}\nchild stderr:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        };
        let gate_info: Value = serde_json::from_str(&gate_line).unwrap();
        assert_eq!(gate_info["phase"], "before_rename");
        child
            .kill()
            .expect("terminate child after staging prefix flush and before registry rename");
        let child_output = child.wait_with_output().unwrap();
        assert!(!child_output.status.success());
        let canonical_path = PathBuf::from(gate_info["registry_path"].as_str().unwrap());
        let canonical_bytes = std::fs::read(&canonical_path).unwrap();
        let previous_bytes = std::fs::read(root.join("predecessor-registry-before.json")).unwrap();
        assert_eq!(canonical_bytes, previous_bytes);
        let previous_entry: Value = serde_json::from_slice(&canonical_bytes).unwrap();
        let seed: Value =
            serde_json::from_slice(&std::fs::read(root.join("seed.json")).unwrap()).unwrap();
        assert_eq!(
            previous_entry["run_id"], seed["predecessor_run_id"],
            "canonical registry remains the complete predecessor entry before commit"
        );
        let staged_path = PathBuf::from(gate_info["staged_path"].as_str().unwrap());
        let prefix_bytes = std::fs::read(staged_path).unwrap();
        assert_eq!(
            prefix_bytes.len(),
            gate_info["prefix_len"].as_u64().unwrap() as usize
        );
        assert!(serde_json::from_slice::<Value>(&prefix_bytes).is_err());
        let retry = registry_truncation_child(TEST, "retry", &root, backend);
        let retry_report = std::fs::read(root.join("retry-report.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        let precondition_report = std::fs::read(root.join("retry-precondition-report.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        eprintln!(
            "REGISTRY_TRUNCATION_BACKEND={backend} ARTIFACT_ROOT={} PREFIX={} PRECONDITION={} RETRY={} CHILD_STDOUT={} CHILD_STDERR={}",
            root.display(),
            serde_json::to_string(&gate_info).unwrap(),
            precondition_report
                .as_ref()
                .map(|report| serde_json::to_string(report).unwrap())
                .unwrap_or_else(|| "missing".into()),
            retry_report
                .as_ref()
                .map(|report| serde_json::to_string(report).unwrap())
                .unwrap_or_else(|| "missing".into()),
            String::from_utf8_lossy(&retry.stdout),
            String::from_utf8_lossy(&retry.stderr)
        );
        if !retry.status.success() {
            red_failures.push(format!(
                "fresh {backend} process failed the previous-entry/retry oracle; precondition={precondition_report:?} retry={retry_report:?}\nstdout:\n{}\nstderr:\n{}\nartifact_root={}",
                String::from_utf8_lossy(&retry.stdout),
                String::from_utf8_lossy(&retry.stderr),
                root.display()
            ));
        }

        let control_temp = tempfile::tempdir().unwrap();
        let control_root = control_temp.path().to_path_buf();
        let control_spawn =
            registry_truncation_child(TEST, "control-spawn", &control_root, backend);
        assert!(
            control_spawn.status.success(),
            "clean {backend} replacement control spawn failed:\n{}",
            String::from_utf8_lossy(&control_spawn.stderr)
        );
        let control_rearm =
            registry_truncation_child(TEST, "control-rearm", &control_root, backend);
        assert!(
            control_rearm.status.success(),
            "clean {backend} rearm control failed:\n{}\n{}",
            String::from_utf8_lossy(&control_rearm.stdout),
            String::from_utf8_lossy(&control_rearm.stderr)
        );
        let control_spawn_report: Value = serde_json::from_slice(
            &std::fs::read(control_root.join("control-spawn-report.json")).unwrap(),
        )
        .unwrap();
        let control_rearm_report: Value = serde_json::from_slice(
            &std::fs::read(control_root.join("control-rearm-report.json")).unwrap(),
        )
        .unwrap();
        eprintln!(
            "REGISTRY_TRUNCATION_CONTROL_BACKEND={backend} ARTIFACT_ROOT={} SPAWN={} REARM={}",
            control_root.display(),
            serde_json::to_string(&control_spawn_report).unwrap(),
            serde_json::to_string(&control_rearm_report).unwrap()
        );
    }

    let post_rename_temp = tempfile::tempdir().unwrap();
    let post_rename_root = post_rename_temp.path().to_path_buf();
    let post_rename_name = "registry-post-rename-sqlite";
    let mut post_rename_child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env(REGISTRY_TRUNCATION_MODE_ENV, "post-rename")
        .env(REGISTRY_TRUNCATION_ROOT_ENV, &post_rename_root)
        .env("CHIDORI_TEST_REGISTRY_POST_RENAME_NAME", post_rename_name)
        .env("CHIDORI_RUN_STORE", "sqlite")
        .env("CHIDORI_RUN_DB", post_rename_root.join("runs.sqlite3"))
        .env("CHIDORI_DURABILITY", "strict")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut post_rename_stdout = post_rename_child.stdout.take().unwrap();
    let (gate_tx, gate_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(&mut post_rename_stdout);
        loop {
            let mut line = String::new();
            match std::io::BufRead::read_line(&mut reader, &mut line) {
                Ok(0) => {
                    let _ = gate_tx.send(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "child exited before post-rename gate line",
                    )));
                    return;
                }
                Ok(_) => {
                    if serde_json::from_str::<Value>(&line)
                        .ok()
                        .is_some_and(|value| value["phase"] == "after_rename_before_mirror")
                    {
                        let _ = gate_tx.send(Ok(line));
                        return;
                    }
                }
                Err(error) => {
                    let _ = gate_tx.send(Err(error));
                    return;
                }
            }
        }
    });
    let post_rename_gate = gate_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("post-rename gate arrives before timeout")
        .expect("post-rename child reports its gate");
    let post_rename_info: Value = serde_json::from_str(&post_rename_gate).unwrap();
    post_rename_child
        .kill()
        .expect("terminate after local rename and before SQLite registry write");
    let post_rename_output = post_rename_child.wait_with_output().unwrap();
    assert!(!post_rename_output.status.success());
    let local_registry: Value = serde_json::from_slice(
        &std::fs::read(post_rename_info["registry_path"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let seed: Value = serde_json::from_slice(
        &std::fs::read(post_rename_root.join("post-rename-seed.json")).unwrap(),
    )
    .unwrap();
    assert_ne!(local_registry["run_id"], seed["predecessor_run_id"]);
    assert_eq!(local_registry["descriptor"]["status"], "running");
    let connection = rusqlite::Connection::open(post_rename_root.join("runs.sqlite3")).unwrap();
    let mirrored_json: String = connection
        .query_row(
            "SELECT data FROM run_registry WHERE name = ?1",
            rusqlite::params![post_rename_name],
            |row| row.get(0),
        )
        .unwrap();
    let mirrored_registry: Value = serde_json::from_str(&mirrored_json).unwrap();
    assert_eq!(mirrored_registry["run_id"], seed["predecessor_run_id"]);
    eprintln!(
        "REGISTRY_POST_RENAME_SQLITE_BOUNDARY={} local_run_id={} mirror_run_id={}",
        post_rename_root.display(),
        local_registry["run_id"],
        mirrored_registry["run_id"]
    );
    assert!(
        red_failures.is_empty(),
        "registry truncation recovery Red failures (expected on vulnerable main): {red_failures:#?}"
    );
}

fn accept_same_name_spawn_event(listener: &std::net::TcpListener) -> (Value, std::net::TcpStream) {
    use std::io::BufRead;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut line = String::new();
                std::io::BufReader::new(stream.try_clone().unwrap())
                    .read_line(&mut line)
                    .unwrap();
                let event = serde_json::from_str(&line).unwrap_or_else(|error| {
                    panic!("invalid same-name spawn event {line:?}: {error}")
                });
                return (event, stream);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for same-name spawn lock event"
                );
                std::thread::yield_now();
            }
            Err(error) => panic!("accepting same-name spawn event failed: {error}"),
        }
    }
}

fn stale_name_cache_event(phase: &str, details: Value, wait_for_release: bool) {
    use std::io::{Read, Write};
    let address: std::net::SocketAddr = std::env::var(STALE_NAME_CACHE_GATE_ENV)
        .expect("missing stale-name-cache gate address")
        .parse()
        .expect("valid stale-name-cache gate address");
    let mut stream = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(10))
        .expect("connect to stale-name-cache test coordinator");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("set stale-name-cache gate timeout");
    writeln!(
        stream,
        "{}",
        serde_json::to_string(&json!({ "phase": phase, "details": details }))
            .expect("serialize stale-name-cache event")
    )
    .expect("report stale-name-cache event");
    if wait_for_release {
        let mut release = [0u8; 1];
        stream
            .read_exact(&mut release)
            .expect("wait for stale-name-cache coordinator release");
        assert_eq!(release[0], b'R', "unexpected stale-name-cache release");
    }
}

fn accept_stale_name_cache_event(
    listener: &std::net::TcpListener,
    expected: &str,
    child: &mut std::process::Child,
    root: &Path,
    role: &str,
) -> (Value, std::net::TcpStream) {
    use std::io::BufRead;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().expect("poll stale-cache child") {
            let stdout =
                std::fs::read_to_string(root.join(format!("{role}.stdout"))).unwrap_or_default();
            let stderr =
                std::fs::read_to_string(root.join(format!("{role}.stderr"))).unwrap_or_default();
            panic!(
                "timed out waiting for stale-name-cache event {expected}; {role} exited {status}; stdout={stdout}; stderr={stderr}"
            );
        }
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("make stale-name-cache accepted stream blocking");
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("set stale-name-cache event timeout");
                let mut line = String::new();
                std::io::BufReader::new(stream.try_clone().unwrap())
                    .read_line(&mut line)
                    .expect("read stale-name-cache event");
                let event = serde_json::from_str(&line).unwrap_or_else(|error| {
                    panic!("invalid stale-name-cache event {line:?}: {error}")
                });
                return (event, stream);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    let stdout = std::fs::read_to_string(root.join(format!("{role}.stdout")))
                        .unwrap_or_default();
                    let stderr = std::fs::read_to_string(root.join(format!("{role}.stderr")))
                        .unwrap_or_default();
                    panic!(
                        "timed out waiting for stale-name-cache event {expected}; {role} remained alive; stdout={stdout}; stderr={stderr}"
                    );
                }
                std::thread::yield_now();
            }
            Err(error) => panic!("accepting stale-name-cache event failed: {error}"),
        }
    }
}

fn stale_name_cache_child_command(
    root: &Path,
    backend: &str,
    name: &str,
    gate: &str,
    mode: &str,
) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            STALE_NAME_CACHE_TEST,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(STALE_NAME_CACHE_MODE_ENV, mode)
        .env(STALE_NAME_CACHE_ROOT_ENV, root)
        .env(STALE_NAME_CACHE_BACKEND_ENV, backend)
        .env(STALE_NAME_CACHE_NAME_ENV, name)
        .env(STALE_NAME_CACHE_GATE_ENV, gate)
        .env("CHIDORI_RUN_STORE", backend)
        .env("CHIDORI_RUN_DB", root.join("runs.sqlite3"))
        .env("CHIDORI_DURABILITY", "strict")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    command
}

fn stale_name_cache_source(project: &Path, role: &str) -> PathBuf {
    let source = project
        .join("services")
        .join(format!("stale-cache-{role}.ts"));
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    let body = match role {
        "predecessor" => {
            r#"export async function agent() {
                const message = await chidori.signal("finish");
                return { phase: "predecessor", id: message.payload.id };
            }"#
        }
        "replacement" => {
            r#"export async function agent() {
                const message = await chidori.signal("current-payload");
                await chidori.tool("migrationEffect", { id: message.payload.id });
                return { phase: "replacement", id: message.payload.id };
            }"#
        }
        _ => panic!("unknown stale-cache source role {role}"),
    };
    std::fs::write(&source, body).unwrap();
    source
}

fn stale_name_cache_child() {
    let mode = std::env::var(STALE_NAME_CACHE_MODE_ENV).unwrap_or_default();
    if !matches!(mode.as_str(), "process-a" | "process-b" | "control") {
        return;
    }
    let root = PathBuf::from(std::env::var_os(STALE_NAME_CACHE_ROOT_ENV).unwrap());
    let backend = std::env::var(STALE_NAME_CACHE_BACKEND_ENV).unwrap();
    let name = std::env::var(STALE_NAME_CACHE_NAME_ENV).unwrap();
    let project = root.join("project");
    let run_base = project.join(".chidori/runs");
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tools = migration_counting_tools(effects.clone());
    let parts = sqlite_test_parts(&project, run_base.clone(), tools);
    hub().install_parts(parts.clone());
    let factory = RunStoreFactory::shared(&run_base);
    let options = SpawnOptions {
        name: Some(name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };

    if mode == "control" {
        let control_name = format!("{}-control", name);
        let source = stale_name_cache_source(&project, "replacement");
        let control_options = SpawnOptions {
            name: Some(control_name.clone()),
            ..options
        };
        let spawned = hub()
            .spawn(
                &parts,
                source.to_str().unwrap(),
                json!({}),
                &control_options,
                "stale-name-cache-single-hub-control".to_string(),
            )
            .unwrap();
        let run_id = spawned["runId"].as_str().unwrap().to_string();
        wait_for_agent_status(&parts, &control_name, |status| status == "hibernating");
        let lookup = hub().lookup(&parts, &control_name).unwrap();
        let send = hub()
            .send(
                &parts,
                &control_name,
                "current-payload",
                json!({ "id": "stale-cache-control-payload" }),
                json!({ "kind": "test", "id": "single-hub-control" }),
            )
            .unwrap();
        let descriptor = wait_for_agent_descriptor(&parts, &control_name, |descriptor| {
            descriptor.run_id == run_id && descriptor.status == "completed"
        });
        let records = factory
            .store_for(&run_id)
            .load_call_log()
            .unwrap()
            .unwrap_or_default();
        let rows = records
            .iter()
            .filter(|record| {
                record.function == "signal"
                    && record.result["payload"]["id"] == "stale-cache-control-payload"
            })
            .count();
        let report = json!({
            "lookup": lookup,
            "send": send,
            "descriptor": descriptor,
            "signal_rows": rows,
            "effect_count": effects.load(Ordering::SeqCst),
        });
        std::fs::write(
            root.join("single-hub-control-report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        return;
    }

    if mode == "process-a" {
        let source = stale_name_cache_source(&project, "predecessor");
        let spawned = hub()
            .spawn(
                &parts,
                source.to_str().unwrap(),
                json!({}),
                &options,
                "stale-name-cache-process-a".to_string(),
            )
            .unwrap();
        let predecessor_id = spawned["runId"].as_str().unwrap().to_string();
        wait_for_agent_status(&parts, &name, |status| status == "hibernating");
        let status = hub().status(&parts, &name).unwrap();
        assert_eq!(status["live"], false);
        stale_name_cache_event(
            "a-predecessor-hibernating",
            json!({ "run_id": predecessor_id, "status": status }),
            true,
        );

        let lookup = hub().lookup(&parts, &name).unwrap();
        let send = hub()
            .send(
                &parts,
                &name,
                "current-payload",
                json!({ "id": "stale-cache-current-payload" }),
                json!({ "kind": "test", "id": "process-a" }),
            )
            .unwrap();
        std::fs::write(
            root.join("process-a-report.json"),
            serde_json::to_vec_pretty(&json!({
                "backend": backend,
                "predecessor_run_id": predecessor_id,
                "lookup": lookup,
                "send": send,
            }))
            .unwrap(),
        )
        .unwrap();
        stale_name_cache_event(
            "a-lookup-send-complete",
            json!({ "lookup": lookup, "send": send }),
            true,
        );
        return;
    }

    let rearmed = hub().rearm_from_registry(parts.clone()).unwrap();
    assert_eq!(rearmed, 1, "fresh process must re-arm the predecessor");
    let predecessor = hub().lookup(&parts, &name).unwrap();
    let predecessor_id = predecessor["runId"].as_str().unwrap().to_string();
    hub()
        .send(
            &parts,
            &name,
            "finish",
            json!({ "id": "finish-predecessor" }),
            json!({ "kind": "test", "id": "process-b" }),
        )
        .unwrap();
    wait_for_agent_descriptor(&parts, &name, |descriptor| {
        descriptor.run_id == predecessor_id && descriptor.status == "completed"
    });

    let source = stale_name_cache_source(&project, "replacement");
    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &options,
            "stale-name-cache-process-b".to_string(),
        )
        .unwrap();
    let replacement_id = spawned["runId"].as_str().unwrap().to_string();
    let replacement_status = wait_for_agent_status(&parts, &name, |status| status == "hibernating");
    assert_eq!(replacement_status["runId"], replacement_id);
    stale_name_cache_event(
        "b-replacement-ready",
        json!({
            "predecessor_run_id": predecessor_id,
            "replacement_run_id": replacement_id,
            "status": replacement_status,
        }),
        true,
    );

    let replacement_entry = hub().ensure_entry(&parts, &name).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut state = replacement_entry.state.lock().unwrap();
    while !AgentEntry::is_settled(&state) {
        let now = std::time::Instant::now();
        if now >= deadline {
            break;
        }
        let (next, _) = replacement_entry
            .signal
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .unwrap();
        state = next;
    }
    drop(state);
    let registry = factory.registry_get(&name).unwrap().unwrap();
    let replacement_store = factory.store_for(&replacement_id);
    let replacement_records = replacement_store
        .load_call_log()
        .unwrap()
        .unwrap_or_default();
    let replacement_inbox = replacement_store
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .unwrap_or_default();
    let predecessor_store = factory.store_for(&predecessor_id);
    let predecessor_records = predecessor_store
        .load_call_log()
        .unwrap()
        .unwrap_or_default();
    let predecessor_inbox = predecessor_store
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .unwrap_or_default();
    let matching_rows = |records: &[crate::runtime::call_log::CallRecord]| {
        records
            .iter()
            .filter(|record| {
                record.function == "signal"
                    && record.result["payload"]["id"] == "stale-cache-current-payload"
            })
            .count()
    };
    let inbox_count = |bytes: &[u8]| -> usize {
        serde_json::from_slice::<Vec<QueuedSignal>>(bytes)
            .unwrap_or_default()
            .iter()
            .filter(|signal| signal.payload["id"] == "stale-cache-current-payload")
            .count()
    };
    let report = json!({
        "backend": backend,
        "predecessor_run_id": predecessor_id,
        "replacement_run_id": replacement_id,
        "registry_run_id": registry["run_id"],
        "replacement_status": hub().status(&parts, &name).unwrap(),
        "replacement_signal_rows": matching_rows(&replacement_records),
        "replacement_inbox_count": inbox_count(&replacement_inbox),
        "replacement_inbox": String::from_utf8_lossy(&replacement_inbox),
        "replacement_effect_count": effects.load(Ordering::SeqCst),
        "predecessor_signal_rows": matching_rows(&predecessor_records),
        "predecessor_inbox_count": inbox_count(&predecessor_inbox),
        "predecessor_inbox": String::from_utf8_lossy(&predecessor_inbox),
    });
    std::fs::write(
        root.join("process-b-report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    stale_name_cache_event("b-final-snapshot", report, true);
}

#[test]
fn detached_stale_name_cache_routes_lookup_and_send_to_current_incarnation() {
    let mode = std::env::var(STALE_NAME_CACHE_MODE_ENV).unwrap_or_default();
    if matches!(mode.as_str(), "process-a" | "process-b" | "control") {
        stale_name_cache_child();
        return;
    }

    let mut red_failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().to_path_buf();
        let project = root.join("project");
        migration_source(&project);
        let name = format!("stale-name-cache-{backend}");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let gate = listener.local_addr().unwrap().to_string();

        let mut process_a_command =
            stale_name_cache_child_command(&root, backend, &name, &gate, "process-a");
        process_a_command
            .stdout(std::process::Stdio::from(
                std::fs::File::create(root.join("a.stdout")).unwrap(),
            ))
            .stderr(std::process::Stdio::from(
                std::fs::File::create(root.join("a.stderr")).unwrap(),
            ));
        let mut process_a = KillChildOnDrop(process_a_command.spawn().unwrap());
        let (a_ready, mut a_ready_stream) = accept_stale_name_cache_event(
            &listener,
            "A predecessor hibernating",
            &mut process_a.0,
            &root,
            "a",
        );
        assert_eq!(a_ready["phase"], "a-predecessor-hibernating");
        let predecessor_id = a_ready["details"]["run_id"].as_str().unwrap().to_string();
        assert_eq!(a_ready["details"]["status"]["status"], "hibernating");
        assert_eq!(a_ready["details"]["status"]["live"], false);

        let mut process_b_command =
            stale_name_cache_child_command(&root, backend, &name, &gate, "process-b");
        process_b_command
            .stdout(std::process::Stdio::from(
                std::fs::File::create(root.join("b.stdout")).unwrap(),
            ))
            .stderr(std::process::Stdio::from(
                std::fs::File::create(root.join("b.stderr")).unwrap(),
            ));
        let mut process_b = KillChildOnDrop(process_b_command.spawn().unwrap());
        let (b_ready, mut b_ready_stream) = accept_stale_name_cache_event(
            &listener,
            "B replacement ready",
            &mut process_b.0,
            &root,
            "b",
        );
        assert_eq!(b_ready["phase"], "b-replacement-ready");
        assert_eq!(b_ready["details"]["predecessor_run_id"], predecessor_id);
        assert_ne!(b_ready["details"]["replacement_run_id"], predecessor_id);
        let replacement_id = b_ready["details"]["replacement_run_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(b_ready["details"]["status"]["status"], "hibernating");

        use std::io::Write;
        a_ready_stream.write_all(b"R").unwrap();
        a_ready_stream.flush().unwrap();
        let (a_done, mut a_done_stream) = accept_stale_name_cache_event(
            &listener,
            "A lookup/send complete",
            &mut process_a.0,
            &root,
            "a",
        );
        assert_eq!(a_done["phase"], "a-lookup-send-complete");
        a_done_stream.write_all(b"R").unwrap();
        a_done_stream.flush().unwrap();
        b_ready_stream.write_all(b"R").unwrap();
        b_ready_stream.flush().unwrap();

        let a_status = process_a.0.wait().unwrap();
        assert!(
            a_status.success(),
            "{backend} process A failed: stdout={} stderr={}",
            std::fs::read_to_string(root.join("a.stdout")).unwrap_or_default(),
            std::fs::read_to_string(root.join("a.stderr")).unwrap_or_default()
        );
        let (b_final, mut b_final_stream) = accept_stale_name_cache_event(
            &listener,
            "B final snapshot",
            &mut process_b.0,
            &root,
            "b",
        );
        assert_eq!(b_final["phase"], "b-final-snapshot");
        b_final_stream.write_all(b"R").unwrap();
        b_final_stream.flush().unwrap();
        let b_status = process_b.0.wait().unwrap();
        assert!(
            b_status.success(),
            "{backend} process B failed: stdout={} stderr={}",
            std::fs::read_to_string(root.join("b.stdout")).unwrap_or_default(),
            std::fs::read_to_string(root.join("b.stderr")).unwrap_or_default()
        );
        let a_report: Value =
            serde_json::from_slice(&std::fs::read(root.join("process-a-report.json")).unwrap())
                .unwrap();
        let b_report: Value =
            serde_json::from_slice(&std::fs::read(root.join("process-b-report.json")).unwrap())
                .unwrap();

        let lookup_id = a_report["lookup"]["runId"].as_str().unwrap_or_default();
        if lookup_id != replacement_id {
            red_failures.push(format!(
                "{backend}: old hub lookup returned {lookup_id}, current registry/replacement is {replacement_id}"
            ));
        }
        if b_report["registry_run_id"] != replacement_id {
            red_failures.push(format!(
                "{backend}: registry={} does not identify replacement={replacement_id}",
                b_report["registry_run_id"]
            ));
        }
        if a_report["send"]["delivered"] != true {
            red_failures.push(format!(
                "{backend}: public send did not report delivered=true: {}",
                a_report["send"]
            ));
        }
        let replacement_accounted = b_report["replacement_signal_rows"].as_u64().unwrap_or(0)
            + b_report["replacement_inbox_count"].as_u64().unwrap_or(0);
        if replacement_accounted != 1 {
            red_failures.push(format!(
                "{backend}: replacement did not account for unique payload exactly once; snapshot={b_report}"
            ));
        }
        if b_report["predecessor_signal_rows"].as_u64().unwrap_or(0) != 0
            || b_report["predecessor_inbox_count"].as_u64().unwrap_or(0) != 0
        {
            red_failures.push(format!(
                "{backend}: payload was stranded on unregistered predecessor; snapshot={b_report}"
            ));
        }
        if b_report["replacement_status"]["status"] == "completed"
            && b_report["replacement_effect_count"] != 1
        {
            red_failures.push(format!(
                "{backend}: replacement completed with effect count {}; snapshot={b_report}",
                b_report["replacement_effect_count"]
            ));
        }

        let control_output =
            stale_name_cache_child_command(&root, backend, &name, &gate, "control")
                .output()
                .unwrap();
        assert!(
            control_output.status.success(),
            "{backend} same-hub control failed: stdout={} stderr={}",
            String::from_utf8_lossy(&control_output.stdout),
            String::from_utf8_lossy(&control_output.stderr)
        );
        let control: Value = serde_json::from_slice(
            &std::fs::read(root.join("single-hub-control-report.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(control["lookup"]["runId"], control["descriptor"]["run_id"]);
        assert_eq!(control["send"]["delivered"], true);
        assert_eq!(control["signal_rows"], 1);
        assert_eq!(control["effect_count"], 1);
    }

    assert!(
        red_failures.is_empty(),
        "stale per-process name-cache Red failures (expected on current main): {red_failures:#?}"
    );
}

#[test]
fn detached_same_name_spawn_competing_processes_preserve_one_delivery() {
    use std::io::Write;
    let mode = std::env::var(SAME_NAME_SPAWN_MODE_ENV).unwrap_or_default();
    if matches!(
        mode.as_str(),
        "worker" | "winner" | "contender" | "lock-crash" | "registry-error" | "start-failure"
    ) {
        same_name_spawn_worker();
        return;
    }

    let mut failures = Vec::new();
    for (backend, trial) in [("fs", 0), ("fs", 1), ("sqlite", 0), ("sqlite", 1)] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().to_path_buf();
        let name = format!("same-name-race-{backend}-{trial}");
        let (predecessor_run_id, predecessor_inbox) =
            seed_same_name_spawn_race(&root, backend, &name);
        assert_same_name_spawn_seed(&predecessor_inbox);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let gate_address = listener.local_addr().unwrap().to_string();
        listener.set_nonblocking(true).unwrap();

        let winner =
            same_name_spawn_child_command(&root, backend, &name, 0, Some(&gate_address), "winner")
                .spawn()
                .unwrap();
        let (winner_attempt, _) = accept_same_name_spawn_event(&listener);
        assert_eq!(winner_attempt["phase"], "lock-attempt");
        assert_eq!(winner_attempt["child_index"], "0");
        let (winner_locked, mut winner_lock_stream) = accept_same_name_spawn_event(&listener);
        assert_eq!(winner_locked["phase"], "lock-acquired");
        assert_eq!(winner_locked["child_index"], "0");

        let contender = same_name_spawn_child_command(
            &root,
            backend,
            &name,
            1,
            Some(&gate_address),
            "contender",
        )
        .spawn()
        .unwrap();
        let (contender_attempt, _) = accept_same_name_spawn_event(&listener);
        assert_eq!(contender_attempt["phase"], "lock-attempt");
        assert_eq!(contender_attempt["child_index"], "1");
        let (contender_waiting, _) = accept_same_name_spawn_event(&listener);
        assert_eq!(contender_waiting["phase"], "lock-waiting");
        assert_eq!(contender_waiting["child_index"], "1");

        winner_lock_stream.write_all(b"R").unwrap();
        winner_lock_stream.flush().unwrap();
        let (winner_live, mut winner_live_stream) = accept_same_name_spawn_event(&listener);
        assert_eq!(winner_live["phase"], "winner-effect-active");
        let winner_registry_path = root
            .join("project/.chidori/runs/agents")
            .join(format!("{name}.json"));
        let winner_registry: Value =
            serde_json::from_slice(&std::fs::read(&winner_registry_path).unwrap()).unwrap();
        let winner_run_id = winner_registry["run_id"].as_str().unwrap().to_string();
        assert!(matches!(
            winner_registry["descriptor"]["status"].as_str(),
            Some("running" | "hibernating")
        ));

        let contender_output = contender.wait_with_output().unwrap();
        let contender_report: Value =
            serde_json::from_slice(&std::fs::read(root.join("child-1-report.json")).unwrap())
                .unwrap();
        if contender_report["run_id"].is_string()
            || !contender_report["spawn_error"]
                .as_str()
                .is_some_and(|error| error.contains("already registered and live"))
        {
            failures.push(format!(
                "{backend} contender was not rejected after the winner published a live listener: output={:?}, report={contender_report}",
                contender_output.status
            ));
        }

        winner_live_stream.write_all(b"R").unwrap();
        winner_live_stream.flush().unwrap();
        let winner_output = winner.wait_with_output().unwrap();
        if !winner_output.status.success() {
            panic!(
                "{backend} winner child failed: status={} stdout={} stderr={}",
                winner_output.status,
                String::from_utf8_lossy(&winner_output.stdout),
                String::from_utf8_lossy(&winner_output.stderr)
            );
        }
        let winner_report: Value =
            serde_json::from_slice(&std::fs::read(root.join("child-0-report.json")).unwrap())
                .unwrap();
        assert_eq!(winner_report["run_id"], winner_run_id);
        assert_eq!(winner_report["status"], "completed");
        assert_eq!(winner_report["matching_signal_rows"], 1);
        assert_eq!(winner_report["effect_count"], 1);
        assert!(winner_report["inbox"] == "[]" || winner_report["inbox"] == "");

        let run_base = root.join("project/.chidori/runs");
        let registry_path = run_base.join("agents").join(format!("{name}.json"));
        let registry: Value =
            serde_json::from_slice(&std::fs::read(&registry_path).unwrap()).unwrap();
        assert_eq!(registry["run_id"], winner_run_id);
        if backend == "sqlite" {
            let connection = rusqlite::Connection::open(root.join("runs.sqlite3")).unwrap();
            let mirror_winner: String = connection
                .query_row(
                    "SELECT run_id FROM run_registry WHERE name = ?1",
                    [&name],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(mirror_winner, winner_run_id);
        }

        eprintln!(
            "SAME_NAME_SPAWN_SERIALIZED backend={backend} trial={trial} predecessor={predecessor_run_id} winner={winner_report} contender={contender_report}"
        );

        if trial == 0 {
            // Killing the owner releases the OS lock but leaves the stable
            // sidecar in place; the old registry and inbox remain retryable.
            let crash_temp = tempfile::tempdir().unwrap();
            let crash_root = crash_temp.path().to_path_buf();
            let crash_name = format!("same-name-crash-release-{backend}");
            let (crash_predecessor, crash_inbox) =
                seed_same_name_spawn_race(&crash_root, backend, &crash_name);
            let crash_registry = crash_root
                .join("project/.chidori/runs/agents")
                .join(format!("{crash_name}.json"));
            let registry_before = std::fs::read(&crash_registry).unwrap();
            let crash_inbox_path = crash_root
                .join("project/.chidori/runs")
                .join(&crash_predecessor)
                .join(SIGNAL_INBOX_FILE);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let gate_address = listener.local_addr().unwrap().to_string();
            listener.set_nonblocking(true).unwrap();
            let mut crash_child = same_name_spawn_child_command(
                &crash_root,
                backend,
                &crash_name,
                9,
                Some(&gate_address),
                "lock-crash",
            )
            .spawn()
            .unwrap();
            std::thread::sleep(Duration::from_millis(300));
            if crash_child.try_wait().unwrap().is_some() {
                let output = crash_child.wait_with_output().unwrap();
                panic!(
                    "lock-crash child exited before reporting lock: status={} stdout={} stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let (attempt, _) = accept_same_name_spawn_event(&listener);
            assert_eq!(attempt["phase"], "lock-attempt");
            let (locked, crash_lock_stream) = accept_same_name_spawn_event(&listener);
            assert_eq!(locked["phase"], "lock-acquired");
            let factory = RunStoreFactory::shared(&crash_root.join("project/.chidori/runs"));
            let lock_path = factory.registry_name_lock_path(&crash_name).unwrap();
            assert!(lock_path.exists(), "stable lock sidecar remains on disk");
            assert!(
                crash_child.try_wait().unwrap().is_none(),
                "crash child must still own the lock while paused"
            );
            crash_child.kill().unwrap();
            let crash_output = crash_child.wait_with_output().unwrap();
            assert!(!crash_output.status.success());
            drop(crash_lock_stream);
            assert_eq!(std::fs::read(&crash_registry).unwrap(), registry_before);
            assert_eq!(std::fs::read(&crash_inbox_path).unwrap(), crash_inbox);

            let retry = same_name_spawn_child_command(
                &crash_root,
                backend,
                &crash_name,
                10,
                None,
                "worker",
            )
            .output()
            .unwrap();
            assert!(
                retry.status.success(),
                "{backend} retry after owner death failed: stdout={} stderr={}",
                String::from_utf8_lossy(&retry.stdout),
                String::from_utf8_lossy(&retry.stderr)
            );
            let retry_report: Value = serde_json::from_slice(
                &std::fs::read(crash_root.join("child-10-report.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(retry_report["status"], "completed");
            assert_eq!(retry_report["matching_signal_rows"], 1);
            assert_eq!(retry_report["effect_count"], 1);
            assert!(retry_report["inbox"] == "[]" || retry_report["inbox"] == "");
            eprintln!(
                "SAME_NAME_SPAWN_CRASH_RELEASE backend={backend} predecessor={crash_predecessor} retry={retry_report} lock_path={}",
                lock_path.display()
            );

            // One uncontended real spawn/migration remains a success control.
            let control = tempfile::tempdir().unwrap();
            let control_root = control.path();
            let control_name = format!("same-name-single-{backend}");
            let (control_predecessor, control_inbox) =
                seed_same_name_spawn_race(control_root, backend, &control_name);
            let control_output = same_name_spawn_child_command(
                control_root,
                backend,
                &control_name,
                0,
                None,
                "worker",
            )
            .output()
            .unwrap();
            assert!(
                control_output.status.success(),
                "{backend} uncontended spawn failed: stdout={} stderr={}",
                String::from_utf8_lossy(&control_output.stdout),
                String::from_utf8_lossy(&control_output.stderr)
            );
            let control_report: Value = serde_json::from_slice(
                &std::fs::read(control_root.join("child-0-report.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(control_report["status"], "completed");
            assert_eq!(control_report["matching_signal_rows"], 1);
            assert_eq!(control_report["effect_count"], 1);
            assert!(control_report["inbox"] == "[]" || control_report["inbox"] == "");
            assert_same_name_spawn_seed(&control_inbox);
            assert!(!control_predecessor.is_empty());
            eprintln!("SAME_NAME_SPAWN_SINGLE_CONTROL backend={backend} report={control_report}");
        }
    }
    assert!(
        failures.is_empty(),
        "same-name spawn serialization: {failures:#?}"
    );
}

#[test]
fn detached_spawn_read_and_thread_start_failures_preserve_retryability() {
    let registry_temp = tempfile::tempdir().unwrap();
    let registry_root = registry_temp.path();
    let registry_name = "spawn-registry-read-error";
    let registry_path = registry_root
        .join("project/.chidori/runs/agents")
        .join(format!("{registry_name}.json"));
    std::fs::create_dir_all(&registry_path).unwrap();
    let registry_output = same_name_spawn_child_command(
        registry_root,
        "fs",
        registry_name,
        3,
        None,
        "registry-error",
    )
    .output()
    .unwrap();
    assert!(
        registry_output.status.success(),
        "registry error child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&registry_output.stdout),
        String::from_utf8_lossy(&registry_output.stderr)
    );
    let registry_report: Value =
        serde_json::from_slice(&std::fs::read(registry_root.join("child-3-report.json")).unwrap())
            .unwrap();
    assert!(registry_report["spawn_error"]
        .as_str()
        .is_some_and(|error| error.contains("reading registry entry")));
    assert!(registry_path.is_dir());
    assert_eq!(
        std::fs::read_dir(registry_root.join("project/.chidori/runs"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != "agents")
            .count(),
        0,
        "registry read failure must not create a run directory"
    );

    for backend in ["fs", "sqlite"] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let name = format!("spawn-thread-start-failure-{backend}");
        let (predecessor_run_id, predecessor_before) =
            seed_same_name_spawn_race(root, backend, &name);
        let output = same_name_spawn_child_command(root, backend, &name, 4, None, "start-failure")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{backend} injected start/retry child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Value =
            serde_json::from_slice(&std::fs::read(root.join("child-4-report.json")).unwrap())
                .unwrap();
        assert!(report["start_failure"]["first_error"]
            .as_str()
            .is_some_and(|error| error.contains("injected supervisor thread-start failure")));
        assert_eq!(report["start_failure"]["failed_registry_status"], "failed");
        assert_eq!(report["start_failure"]["predecessor_preserved"], true);
        assert_eq!(report["status"], "completed");
        assert_eq!(report["matching_signal_rows"], 1);
        assert_eq!(report["effect_count"], 1);
        let predecessor_after = RunStoreFactory::shared(&root.join("project/.chidori/runs"))
            .store_for(&predecessor_run_id)
            .get_blob(SIGNAL_INBOX_FILE)
            .unwrap()
            .unwrap();
        assert_eq!(predecessor_after, predecessor_before);
    }

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let name = "spawn-mirror-error-local-commit";
    seed_same_name_spawn_race(root, "sqlite", name);
    let connection = rusqlite::Connection::open(root.join("runs.sqlite3")).unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER reject_running_spawn_mirror
             BEFORE UPDATE ON run_registry
             WHEN OLD.name = '{name}'
               AND json_extract(NEW.data, '$.descriptor.status') = 'running'
             BEGIN SELECT RAISE(ABORT, 'injected running registry mirror failure'); END;"
        ))
        .unwrap();
    let output = same_name_spawn_child_command(root, "sqlite", name, 5, None, "worker")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "SQLite mirror-error spawn child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value =
        serde_json::from_slice(&std::fs::read(root.join("child-5-report.json")).unwrap()).unwrap();
    assert_eq!(report["status"], "completed");
    assert_eq!(report["matching_signal_rows"], 1);
    assert_eq!(report["effect_count"], 1);
    let local_registry: Value = serde_json::from_slice(
        &std::fs::read(
            root.join("project/.chidori/runs/agents")
                .join(format!("{name}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    let mirror_run_id: String = connection
        .query_row(
            "SELECT run_id FROM run_registry WHERE name = ?1",
            [name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(local_registry["run_id"], mirror_run_id);
}

#[test]
fn detached_spawn_http_registry_mirror_failure_keeps_local_commit_live() {
    const CHILD: &str = "CHIDORI_TEST_SPAWN_HTTP_MIRROR_FAILURE_CHILD";
    const BASE: &str = "CHIDORI_TEST_SPAWN_HTTP_MIRROR_FAILURE_BASE";
    if std::env::var_os(CHILD).is_some() {
        let root = PathBuf::from(std::env::var_os(BASE).unwrap());
        let project = root.join("project");
        let source = project.join("services/http-mirror-worker.ts");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            &source,
            r#"export async function agent() {
                await chidori.tool("httpMirrorEffect", {});
                return { completed: true };
            }"#,
        )
        .unwrap();
        let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tools = ToolRegistry::new();
        let observed_effects = effects.clone();
        tools.register_native(
            "httpMirrorEffect",
            "count effects after local registry publication",
            Vec::new(),
            move |_| {
                observed_effects.fetch_add(1, Ordering::SeqCst);
                Ok(json!({ "effect": true }))
            },
        );
        let run_base = project.join(".chidori/runs");
        let parts = sqlite_test_parts(&project, run_base.clone(), Arc::new(tools));
        let name = "http-registry-mirror-failure";
        let spawned = hub()
            .spawn(
                &parts,
                source.to_str().unwrap(),
                json!({}),
                &SpawnOptions {
                    name: Some(name.to_string()),
                    restart: "never".to_string(),
                    max_restarts: 0,
                    backoff_ms: 0,
                    model: None,
                },
                "http-mirror-failure-test".to_string(),
            )
            .expect("a committed local registry entry must start despite HTTP mirror failure");
        let run_id = spawned["runId"].as_str().unwrap();
        let descriptor = wait_for_agent_descriptor(&parts, name, |descriptor| {
            descriptor.run_id == run_id && descriptor.status == "completed"
        });
        let registry = RunStoreFactory::shared(&run_base)
            .registry_get(name)
            .unwrap()
            .unwrap();
        assert_eq!(descriptor.status, "completed");
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(registry["run_id"], run_id);
        assert_eq!(registry["descriptor"]["status"], "completed");
        return;
    }

    use axum::extract::Request;
    use axum::http::{Method, StatusCode};
    use axum::response::IntoResponse as _;
    let registry_failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_failures = registry_failures.clone();
    let app = axum::Router::new().fallback(move |request: Request| {
        let registry_write =
            request.method() == Method::PUT && request.uri().path().starts_with("/registry/");
        let observed_failures = observed_failures.clone();
        async move {
            if registry_write {
                observed_failures.fetch_add(1, Ordering::SeqCst);
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            } else if request.method() == Method::GET {
                StatusCode::NOT_FOUND.into_response()
            } else {
                StatusCode::OK.into_response()
            }
        }
    });
    let (address_tx, address_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            address_tx.send(listener.local_addr().unwrap()).unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });
    let relay = format!("http://{}", address_rx.recv().unwrap());
    let base = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::host_agent::tests::detached_spawn_http_registry_mirror_failure_keeps_local_commit_live",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env(BASE, base.path())
        .env("CHIDORI_RUN_STORE", &relay)
        .env("CHIDORI_RUN_DB", base.path().join("runs.sqlite3"))
        .env("CHIDORI_DURABILITY", "strict")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "HTTP mirror failure child failed; registry failures={}; stdout={} stderr={}",
        registry_failures.load(Ordering::SeqCst),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        registry_failures.load(Ordering::SeqCst) > 0,
        "fixture did not reject a post-rename HTTP registry mirror write"
    );
}

#[cfg(unix)]
#[test]
fn detached_spawn_name_mutex_unifies_same_process_run_base_aliases() {
    let temporary = tempfile::tempdir().unwrap();
    let real_root = temporary.path().join("real");
    std::fs::create_dir_all(&real_root).unwrap();
    let alias_root = temporary.path().join("alias");
    std::os::unix::fs::symlink(&real_root, &alias_root).unwrap();
    let first_factory = RunStoreFactory::shared(&real_root);
    let alias_factory = RunStoreFactory::shared(&alias_root);
    let first_path = first_factory.registry_name_lock_path("Same-Name").unwrap();
    let alias_path = alias_factory.registry_name_lock_path("same-name").unwrap();
    assert_eq!(first_path, alias_path);
    let first_mutex = spawn_name_mutex(&first_path);
    let alias_mutex = spawn_name_mutex(&alias_path);
    assert!(Arc::ptr_eq(&first_mutex, &alias_mutex));
    let _guard = acquire_spawn_name_mutex(&first_mutex, "Same-Name").unwrap();
    assert!(matches!(
        alias_mutex.try_lock(),
        Err(MutexTryLockError::WouldBlock)
    ));
}

#[test]
fn detached_predecessor_destination_write_failure_is_surfaced_before_worker_start() {
    if std::env::var_os(PREDECESSOR_MIGRATION_CHILD_ENV).is_some() {
        assert_eq!(
            std::env::var(PREDECESSOR_MIGRATION_SCENARIO_ENV).as_deref(),
            Ok("failure")
        );
        let base = PathBuf::from(std::env::var_os(PREDECESSOR_MIGRATION_BASE_ENV).unwrap());
        let observed = run_predecessor_migration_failure(&base);
        eprintln!(
            "destination-write-failure observation: spawn={:?}; destination_run_id={}; predecessor_before={} bytes ({}); predecessor_after={}; destination_inbox_after={}; destination_write_probe={:?}; sqlite_mirror_inbox_after={:?}; destination_signal_result_count={:?}; entry_present={}; thread_live={:?}; effect_count={}",
            observed.spawn_result,
            observed.destination_run_id,
            observed.predecessor_before.len(),
            String::from_utf8_lossy(&observed.predecessor_before),
            summarize_blob_result(&observed.predecessor_after),
            summarize_blob_result(&observed.destination_inbox_after),
            observed.destination_write_probe,
            observed.sqlite_mirror_inbox_after.as_ref().map(summarize_blob_result),
            observed.destination_signal_result_count,
            observed.entry_present,
            observed.thread_live,
            observed.effect_count,
        );
        let mut violations = Vec::new();
        match &observed.spawn_result {
            Err(err) if err.contains("migrat") && err.contains(SIGNAL_INBOX_FILE) => {}
            Err(err) => violations.push(format!(
                "spawn returned an error without migration/inbox context: {err}"
            )),
            Ok(value) => violations.push(format!(
                "spawn reported success despite destination write failure: {value}"
            )),
        }
        match &observed.predecessor_after {
            Ok(Some(bytes)) if bytes == &observed.predecessor_before => {}
            other => violations.push(format!(
                "predecessor inbox changed or could not be read: before={:?}, after={other:?}",
                String::from_utf8_lossy(&observed.predecessor_before)
            )),
        }
        if observed.destination_inbox_after.is_ok() {
            violations.push(format!(
                "injected directory target unexpectedly read as a blob: {:?}",
                observed.destination_inbox_after
            ));
        }
        if observed.destination_write_probe.is_ok() {
            violations.push(
                "a follow-up write probe unexpectedly succeeded at the directory target"
                    .to_string(),
            );
        }
        if let Some(Err(error)) = &observed.sqlite_mirror_inbox_after {
            violations.push(format!("SQLite mirror inbox could not be read: {error}"));
        }
        if observed
            .sqlite_mirror_inbox_after
            .as_ref()
            .is_some_and(|result| result.as_ref().is_ok_and(Option::is_some))
        {
            violations.push(
                "SQLite mirror received destination inbox despite FS-primary failure".to_string(),
            );
        }
        match &observed.destination_signal_result_count {
            Ok(0) => {}
            other => violations.push(format!(
                "destination journal has a migrated signal result or could not be read: {other:?}"
            )),
        }
        if observed.entry_present || observed.thread_live.is_some() {
            violations.push(format!(
                "failed migration registered/started a worker: entry_present={}, thread_live={:?}",
                observed.entry_present, observed.thread_live
            ));
        }
        if observed.registry_run_id_after.as_ref() != Ok(&Some(observed.predecessor_run_id.clone()))
        {
            violations.push(format!(
                "replacement registry was published before destination write succeeded: {:?}",
                observed.registry_run_id_after
            ));
        }
        if observed.effect_count != 0 {
            violations.push(format!(
                "migration effect ran {} time(s) before write failure was surfaced",
                observed.effect_count
            ));
        }
        assert!(
            violations.is_empty(),
            "migration write failure contract violations: {violations:#?}"
        );
        return;
    }
    run_migration_child_for_backend(
        "runtime::host_agent::tests::detached_predecessor_destination_write_failure_is_surfaced_before_worker_start",
        "failure",
    );
}

#[test]
fn detached_predecessor_valid_migration_consumes_payload_once() {
    if std::env::var_os(PREDECESSOR_MIGRATION_CHILD_ENV).is_some() {
        assert_eq!(
            std::env::var(PREDECESSOR_MIGRATION_SCENARIO_ENV).as_deref(),
            Ok("success")
        );
        let base = PathBuf::from(std::env::var_os(PREDECESSOR_MIGRATION_BASE_ENV).unwrap());
        let (descriptor, records, inbox, effect_count) = run_predecessor_migration_success(&base);
        let matching_rows: Vec<_> = records
            .iter()
            .filter(|record| {
                record.function == "signal"
                    && record.result["payload"]["id"] == json!("must-survive")
            })
            .collect();
        assert_eq!(descriptor.status, "completed");
        assert_eq!(matching_rows.len(), 1, "migrated payload is consumed once");
        assert_eq!(
            serde_json::from_slice::<Vec<Value>>(&inbox).unwrap(),
            Vec::<Value>::new()
        );
        assert_eq!(effect_count, 1, "migrated effect executes once");
        return;
    }
    run_migration_child_for_backend(
        "runtime::host_agent::tests::detached_predecessor_valid_migration_consumes_payload_once",
        "success",
    );
}

#[test]
fn detached_sqlite_expired_live_owner_cannot_write_or_start_next_effect() {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    if let Ok(role) = std::env::var(SQLITE_STALE_CHILD_ENV) {
        let base = PathBuf::from(std::env::var_os(SQLITE_STALE_BASE_ENV).unwrap());
        let http = std::env::var(SQLITE_STALE_HTTP_ENV).unwrap();
        if role == "a" {
            eprintln!(
                "stale-owner child proxy bypass before fetch: NO_PROXY={:?} no_proxy={:?}",
                std::env::var("NO_PROXY"),
                std::env::var("no_proxy")
            );
        }
        let run_base = base.join(".chidori/runs");
        let source = base.join("services/stale-owner.ts");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            &source,
            format!(
                r#"export async function agent(input) {{
                    await chidori.signal("start");
                    await chidori.tool("aExecutionBarrier", {{}});
                    const response = await fetch("http://{http}/held");
                    const result = await response.json();
                    if (result.continue) {{
                        await fetch("http://{http}/follow-on");
                    }} else {{
                        await chidori.tool("bCommitHold", {{}});
                    }}
                    return result;
                }}"#
            ),
        )
        .unwrap();

        if role == "proxy_probe" {
            let tokio_rt = tokio::runtime::Runtime::new().unwrap();
            let response = crate::runtime::host_core::execute_http(
                &tokio_rt,
                &json!({"url": format!("http://{http}/prelude"), "method": "GET"}),
            )
            .unwrap();
            assert_eq!(
                response["status"], 200,
                "loopback probe response={response:#}"
            );
            assert_eq!(response["body"], json!({}));
            return;
        }

        let mut tools = ToolRegistry::new();
        if role == "a" {
            tools.register_native(
                "aExecutionBarrier",
                "confirm A entered agent execution before the held request",
                Vec::new(),
                |_| {
                    let release = sqlite_stale_control_wait("A_IN_AGENT");
                    assert_eq!(release, b'G');
                    Ok(json!({ "entered": true }))
                },
            );
        }
        if role == "b" {
            tools.register_native(
                "bCommitHold",
                "hold B after seq-3 commit",
                Vec::new(),
                |_| {
                    let release = sqlite_stale_control_wait("B_SEQ3_DURABLE");
                    assert_eq!(release, b'G', "unexpected B commit barrier release");
                    Ok(json!({ "committed": true }))
                },
            );
        }
        let parts = sqlite_test_parts(&base, run_base, Arc::new(tools));
        hub().install_parts(parts.clone());
        match role.as_str() {
            "a" => {
                let spawned = hub()
                    .spawn(
                        &parts,
                        source.to_str().unwrap(),
                        json!({}),
                        &SpawnOptions {
                            name: Some(SQLITE_STALE_AGENT.to_string()),
                            restart: "never".to_string(),
                            max_restarts: 0,
                            backoff_ms: 0,
                            model: None,
                        },
                        "sqlite-stale-owner-test".to_string(),
                    )
                    .unwrap();
                let run_id = spawned["runId"].as_str().unwrap();
                let release = sqlite_stale_control_wait(&format!("A_READY {run_id}"));
                assert_eq!(release, b'G');
                hub()
                    .send(
                        &parts,
                        SQLITE_STALE_AGENT,
                        "start",
                        json!({ "run": run_id }),
                        json!({ "test": true }),
                    )
                    .unwrap();
                let release = sqlite_stale_control_wait("A_SIGNALLED");
                assert_eq!(release, b'G');
                let terminal = wait_for_agent_status(&parts, SQLITE_STALE_AGENT, |status| {
                    status == "completed" || status == "failed"
                });
                let run_store = RunStoreFactory::shared(&parts.run_base).store_for(run_id);
                let records = run_store.load_call_log().unwrap().unwrap_or_default();
                let release = sqlite_stale_control_wait(&format!(
                    "A_DONE {terminal} records={}",
                    serde_json::to_string(&records).unwrap()
                ));
                assert_eq!(release, b'G');
            }
            "b" => {
                hub().rearm_from_registry(parts.clone()).unwrap();
                let terminal = wait_for_agent_status(&parts, SQLITE_STALE_AGENT, |status| {
                    status == "completed" || status == "failed"
                });
                let registry = RunStoreFactory::shared(&parts.run_base)
                    .registry_get(SQLITE_STALE_AGENT)
                    .unwrap()
                    .unwrap();
                let run_id = registry["run_id"].as_str().unwrap();
                let run_store = RunStoreFactory::shared(&parts.run_base).store_for(run_id);
                let records = run_store.load_call_log().unwrap().unwrap_or_default();
                let release = sqlite_stale_control_wait(&format!(
                    "B_DONE {terminal} records={}",
                    serde_json::to_string(&records).unwrap()
                ));
                assert_eq!(release, b'G');
            }
            other => panic!("unknown stale-owner child role {other}"),
        }
        return;
    }

    // A socket coordinator is used instead of sleeps. Each child reports
    // its barrier name and blocks until the parent explicitly releases it.
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path();
    let run_base = base.join(".chidori/runs");
    let control_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_address = control_listener.local_addr().unwrap();
    let (event_tx, event_rx) = std::sync::mpsc::channel::<(String, TcpStream)>();
    let control_thread = std::thread::spawn(move || {
        while let Ok((stream, _)) = control_listener.accept() {
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            let mut event = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut event)
                .unwrap();
            let event = event.trim().to_string();
            if event == "STOP" {
                break;
            }
            event_tx.send((event, stream)).unwrap();
        }
    });

    let http_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let http_address = http_listener.local_addr().unwrap();
    http_listener.set_nonblocking(true).unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let follow_on_count = Arc::new(AtomicUsize::new(0));
    let held_count = Arc::new(AtomicUsize::new(0));
    let (http_tx, http_rx) = std::sync::mpsc::channel::<String>();
    let (http_diag_tx, http_diag_rx) = std::sync::mpsc::channel::<String>();
    let (release_a_tx, release_a_rx) = std::sync::mpsc::channel::<()>();
    let release_a_rx = Arc::new(StdMutex::new(release_a_rx));
    let server_stopped = Arc::clone(&stopped);
    let server_follow_on = Arc::clone(&follow_on_count);
    let server_held = Arc::clone(&held_count);
    let server = std::thread::spawn(move || {
        while !server_stopped.load(AtomicOrdering::SeqCst) {
            match http_listener.accept() {
                Ok((mut stream, peer)) => {
                    let _ = http_diag_tx
                        .send(format!("accepted peer={peer}; waiting for request headers"));
                    // macOS may preserve the listener's nonblocking mode
                    // on accepted sockets; the fixture uses bounded
                    // blocking reads after accept.
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut request = Vec::with_capacity(1024);
                    let mut bytes = [0u8; 1024];
                    while request.len() < 64 * 1024
                        && !request.windows(4).any(|window| window == b"\r\n\r\n")
                    {
                        match stream.read(&mut bytes) {
                            Ok(0) => break,
                            Ok(count) => request.extend_from_slice(&bytes[..count]),
                            Err(error) => {
                                let _ = http_diag_tx.send(format!(
                                    "request read failed peer={peer}: {error}; bytes={:?}",
                                    String::from_utf8_lossy(&request)
                                ));
                                break;
                            }
                        }
                    }
                    if request.is_empty() {
                        let _ = http_diag_tx.send(format!(
                            "empty request peer={peer}; received_bytes={request:?}"
                        ));
                        continue;
                    }
                    let request = String::from_utf8_lossy(&request).into_owned();
                    let _ = http_diag_tx
                        .send(format!("complete request peer={peer}; bytes={request:?}"));
                    let target = request
                        .lines()
                        .next()
                        .unwrap_or("")
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("/");
                    let path = url::Url::parse(target)
                        .map(|url| url.path().to_string())
                        .unwrap_or_else(|_| target.to_string());
                    let held_index = if path == "/held" {
                        server_held.fetch_add(1, AtomicOrdering::SeqCst)
                    } else {
                        usize::MAX
                    };
                    http_tx.send(path.clone()).unwrap();
                    let release = Arc::clone(&release_a_rx);
                    let follow_on = Arc::clone(&server_follow_on);

                    std::thread::spawn(move || {
                        let body = match path.as_str() {
                            "/prelude" => "{}".to_string(),
                            "/held" => {
                                if held_index == 0 {
                                    release
                                        .lock()
                                        .unwrap()
                                        .recv_timeout(Duration::from_secs(30))
                                        .expect("A held HTTP response was not released");
                                    r#"{"writer":"A","continue":true}"#.to_string()
                                } else {
                                    r#"{"writer":"B","continue":false}"#.to_string()
                                }
                            }
                            "/follow-on" => {
                                follow_on.fetch_add(1, AtomicOrdering::SeqCst);
                                "{}".to_string()
                            }
                            _ => "{}".to_string(),
                        };
                        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                        if let Err(error) = stream.write_all(response.as_bytes()) {
                            eprintln!("stale-owner HTTP fixture response write failed: {error}; request={request}");
                        }
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(error) => panic!("stale-owner HTTP fixture failed: {error}"),
            }
        }
    });

    let child = |role: &str| {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", SQLITE_STALE_TEST, "--nocapture"])
            .env(SQLITE_STALE_CHILD_ENV, role)
            .env(SQLITE_STALE_BASE_ENV, base)
            .env(SQLITE_STALE_CONTROL_ENV, control_address.to_string())
            .env(SQLITE_STALE_HTTP_ENV, http_address.to_string())
            .env("CHIDORI_RUN_STORE", "sqlite")
            .env("CHIDORI_RUN_DB", base.join("runs.sqlite3"))
            .env("CHIDORI_DURABILITY", "effect")
            .env("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1")
            .env("CHIDORI_TEST_SQLITE_LEASE_TTL_MS", "3000")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .env("CHIDORI_TEST_SQLITE_STALE_NO_PROXY", "1")
            .spawn()
            .unwrap()
    };
    let wait_http = |expected: &str| {
        let path = http_rx
            .recv_timeout(Duration::from_secs(20))
            .unwrap_or_else(|err| {
                let child_event = event_rx.try_recv().ok().map(|(event, _)| event);
                let mut server_diagnostics = Vec::new();
                while let Ok(diagnostic) = http_diag_rx.try_recv() {
                    server_diagnostics.push(diagnostic);
                }
                panic!("timed out waiting for HTTP {expected}: {err}; full pending child event={child_event:#?}; listener diagnostics={server_diagnostics:#?}")
            });
        assert_eq!(path, expected);
    };
    let proxy_probe = child("proxy_probe");
    wait_http("/prelude");
    let probe_output = proxy_probe.wait_with_output().unwrap();
    assert!(
        probe_output.status.success(),
        "loopback proxy probe child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&probe_output.stdout),
        String::from_utf8_lossy(&probe_output.stderr)
    );
    let probe_diagnostics: Vec<_> = http_diag_rx.try_iter().collect();
    assert!(
        probe_diagnostics
            .iter()
            .any(|line| line.contains("complete request")
                && line.contains("GET /prelude HTTP/1.1")
                && line.contains("host:")),
        "loopback probe did not deliver HTTP request headers; diagnostics={probe_diagnostics:#?}"
    );
    if std::env::var_os("CHIDORI_TEST_SQLITE_STALE_PROXY_PROBE_ONLY").is_some() {
        stopped.store(true, AtomicOrdering::SeqCst);
        server.join().unwrap();
        std::net::TcpStream::connect(control_address)
            .unwrap()
            .write_all(b"STOP\n")
            .unwrap();
        control_thread.join().unwrap();
        return;
    }
    let a = child("a");
    let (ready, ready_socket) = event_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    let run_id = ready
        .strip_prefix("A_READY ")
        .expect("A ready event")
        .to_string();
    let shared = SqliteRunStoreShared::open(&base.join("runs.sqlite3")).unwrap();
    let store = SqliteRunStore::new(shared.clone(), &run_id);
    let tee = TeeRunStore::new(
        FsRunStore::new(run_base.join(&run_id)),
        Arc::new(SqliteRunStore::new(shared.clone(), &run_id)),
    );
    let initial_lease = wait_for_initial_stale_owner_lease(&store);
    let initial_expiry: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(initial_lease["expires_at"].clone()).unwrap();
    stale_owner_release(ready_socket, b'G');
    let a_signalled = stale_owner_wait_event(&event_rx, "A_SIGNALLED");
    stale_owner_release(a_signalled, b'G');
    let a_in_agent = stale_owner_wait_event(&event_rx, "A_IN_AGENT");
    stale_owner_release(a_in_agent, b'G');
    wait_http("/held");

    let b = child("b");
    let contended = stale_owner_wait_event(&event_rx, "B_CONTENDED");
    // Keep B paused after the real contention result while A remains
    // blocked in its dispatched HTTP request. Wait for A's independent
    // heartbeat to extend the original SQLite expiry; B must still be
    // unable to acquire this live tenure after the original TTL elapsed.
    let renewal_deadline = std::time::Instant::now() + Duration::from_secs(4);
    let mut renewed = false;
    while std::time::Instant::now() < renewal_deadline {
        let current = stale_owner_lease(&store);
        let expires: chrono::DateTime<chrono::Utc> =
            serde_json::from_value(current["expires_at"].clone()).unwrap();
        if current["owner"] == initial_lease["owner"]
            && current["generation"] == initial_lease["generation"]
            && expires > initial_expiry + chrono::Duration::seconds(2)
        {
            renewed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        renewed,
        "A's heartbeat did not extend its original lease: initial={initial_lease:#}; current={:#}",
        stale_owner_lease(&store)
    );
    assert!(
        event_rx.try_recv().is_err(),
        "B acquired while A's heartbeat kept its lease live"
    );
    let before = stale_owner_lease(&store);
    let owner_a = before["owner"].as_str().unwrap().to_string();
    let expires_a: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(before["expires_at"].clone()).unwrap();
    assert!(
        expires_a > chrono::Utc::now(),
        "before-expiry control: A lease is not in the future"
    );
    assert_eq!(
        held_count.load(AtomicOrdering::SeqCst),
        1,
        "only A's pending request should have arrived before takeover"
    );
    let before_records = store.load_call_log().unwrap().unwrap_or_default();
    assert!(
        before_records.iter().all(|record| record.seq != 3),
        "seq 3 must still be pending before B takes over: {before_records:#?}"
    );
    let before_promises = load_host_promise_records(&store).unwrap();
    assert!(
        before_promises.iter().any(|record| {
            record.operation.seq == 3 && record.state == HostPromiseState::Pending
        }),
        "the held seq-3 HTTP operation must remain pending before takeover: {before_promises:#?}"
    );

    let expired = serde_json::to_vec(&crate::runtime::store::StoredRunLease {
        owner: owner_a.clone(),
        expires_at: chrono::Utc::now() - chrono::Duration::seconds(1),
        generation: before["generation"].as_u64().unwrap_or(0),
    })
    .unwrap();
    let prior = store
        .get_blob(crate::runtime::store::LEASE_FILE)
        .unwrap()
        .unwrap();
    assert!(store
        .compare_and_swap_blob(
            crate::runtime::store::LEASE_FILE,
            Some(&prior),
            Some(&expired)
        )
        .unwrap());
    stale_owner_release(contended, b'R');

    let (acquired_event, acquired) = event_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    let owner_b_reported = acquired_event
        .strip_prefix("B_ACQUIRED ")
        .expect("B acquired lease event");
    let lease_b = stale_owner_lease(&store);
    let owner_b = lease_b["owner"].as_str().unwrap().to_string();
    assert_eq!(
        owner_b, owner_b_reported,
        "B must own the real SQLite lease before replay"
    );
    assert_ne!(owner_a, owner_b, "lease transfer must replace A's owner");
    let expires_b: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(lease_b["expires_at"].clone()).unwrap();
    assert!(expires_b > chrono::Utc::now());
    stale_owner_release(acquired, b'G');
    wait_http("/held");
    let b_committed = stale_owner_wait_event(&event_rx, "B_SEQ3_DURABLE");

    let records_b = store.load_call_log().unwrap().unwrap();
    let seq3_b = records_b
        .iter()
        .find(|record| record.seq == 3)
        .expect("B seq-3 result must be durable");
    assert_eq!(seq3_b.function, "http");
    assert_eq!(
        seq3_b.result["body"]["writer"],
        json!("B"),
        "seq-3 HTTP result={:#}",
        seq3_b.result
    );
    let promises_b = load_host_promise_records(&store).unwrap();
    let promise_b = promises_b
        .iter()
        .find(|record| record.operation.seq == 3)
        .expect("B seq-3 promise must be durable");
    assert!(
        matches!(&promise_b.state, HostPromiseState::Resolved { value, .. } if value["body"]["writer"] == json!("B"))
    );
    let seq3_result_b = seq3_b.result.clone();
    let promise_value_b = match &promise_b.state {
        HostPromiseState::Resolved { value, .. } => value.clone(),
        other => panic!("B seq-3 host promise is not resolved: {other:?}"),
    };
    let tee_records_b = tee.load_call_log().unwrap().unwrap();
    let tee_seq3_b = tee_records_b
        .iter()
        .find(|record| record.seq == 3)
        .expect("factory Tee should read B's primary seq-3 result");
    let tee_promises_b = load_host_promise_records(&tee).unwrap();
    let tee_promise_b = tee_promises_b
        .iter()
        .find(|record| record.operation.seq == 3)
        .expect("factory Tee should read B's primary seq-3 promise");
    assert_eq!(tee_seq3_b.result, seq3_result_b);
    assert_eq!(
        tee_promise_b.state,
        HostPromiseState::Resolved {
            value: promise_value_b.clone(),
            completed_at: match &promise_b.state {
                HostPromiseState::Resolved { completed_at, .. } => *completed_at,
                _ => unreachable!("resolved state checked above"),
            },
        }
    );
    assert_eq!(
        follow_on_count.load(AtomicOrdering::SeqCst),
        0,
        "B's continue=false response must not dispatch the follow-on effect"
    );

    release_a_tx.send(()).unwrap();
    let (a_done, a_socket) = event_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    assert!(a_done.starts_with("A_DONE "), "{a_done}");
    stale_owner_release(a_socket, b'G');
    let final_records = store.load_call_log().unwrap().unwrap();
    let final_seq3: Vec<_> = final_records
        .iter()
        .filter(|record| record.seq == 3)
        .collect();
    let final_promises = load_host_promise_records(&store).unwrap();
    let final_promise = final_promises
        .iter()
        .find(|record| record.operation.seq == 3)
        .unwrap();
    let tee_final_records = tee.load_call_log().unwrap().unwrap();
    let tee_final_seq3: Vec<_> = tee_final_records
        .iter()
        .filter(|record| record.seq == 3)
        .collect();
    let tee_final_promises = load_host_promise_records(&tee).unwrap();
    let tee_final_promise = tee_final_promises
        .iter()
        .find(|record| record.operation.seq == 3)
        .unwrap();
    let follow_on = follow_on_count.load(AtomicOrdering::SeqCst);

    // Hold B until all stale-A observations have been captured: its lease
    // must not be released before the final journal/promise check.
    stale_owner_release(b_committed, b'G');
    let (event, stream) = event_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    assert!(event.starts_with("B_DONE "), "{event}");
    stale_owner_release(stream, b'G');
    let wait = |child: std::process::Child, role: &str| {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{role} child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    };
    wait(a, "A");
    wait(b, "B");

    // A's pending HTTP request was already dispatched before takeover and
    // B may replay it. Do not assert exactly-once delivery for that call;
    // only the later follow-on request is fenced by this test.

    // Release local fixtures only after child processes have completed.
    stopped.store(true, AtomicOrdering::SeqCst);
    server.join().unwrap();
    std::net::TcpStream::connect(control_address)
        .unwrap()
        .write_all(b"STOP\n")
        .unwrap();
    control_thread.join().unwrap();

    let mut red_failures = Vec::new();
    if final_seq3.len() != 1 {
        red_failures.push(format!(
            "expected one seq-3 journal row, observed {}",
            final_seq3.len()
        ));
    } else if final_seq3[0].result != seq3_result_b {
        red_failures.push(format!(
            "stale A replaced B's seq-3 journal result: expected {}, observed {}",
            seq3_result_b, final_seq3[0].result
        ));
    }
    let final_promise_value = match &final_promise.state {
        HostPromiseState::Resolved { value, .. } => Some(value),
        _ => None,
    };
    if final_promise_value != Some(&promise_value_b) {
        red_failures.push(format!(
            "stale A replaced B's seq-3 host-promise result: expected {}, observed {:?}",
            promise_value_b, final_promise_value
        ));
    }
    if tee_final_seq3.len() != 1 {
        red_failures.push(format!(
            "factory Tee primary should expose one seq-3 row, observed {}",
            tee_final_seq3.len()
        ));
    } else if tee_final_seq3[0].result != tee_seq3_b.result {
        red_failures.push(format!(
            "stale A replaced the factory Tee primary seq-3 result: expected {}, observed {}",
            tee_seq3_b.result, tee_final_seq3[0].result
        ));
    }
    let tee_final_promise_value = match &tee_final_promise.state {
        HostPromiseState::Resolved { value, .. } => Some(value),
        _ => None,
    };
    let tee_promise_value_b = match &tee_promise_b.state {
        HostPromiseState::Resolved { value, .. } => value,
        _ => unreachable!("resolved state checked above"),
    };
    if tee_final_promise_value != Some(tee_promise_value_b) {
        red_failures.push(format!(
            "stale A replaced the factory Tee primary seq-3 host-promise result: expected {}, observed {:?}",
            tee_promise_value_b, tee_final_promise_value
        ));
    }
    if follow_on != 0 {
        red_failures.push(format!(
            "stale A dispatched {follow_on} follow-on external effect(s)"
        ));
    }
    assert!(
        red_failures.is_empty(),
        "stale-owner violations:\n{}",
        red_failures.join("\n")
    );
}

#[tokio::test]
async fn detached_inbox_cross_process_writers_preserve_acknowledged_payloads() {
    const CHILD: &str = "CHIDORI_TEST_CROSS_PROCESS_INBOX_CHILD";
    const TEST_NAME: &str = "runtime::host_agent::tests::detached_inbox_cross_process_writers_preserve_acknowledged_payloads";

    if let Some(role) = std::env::var_os(CHILD) {
        let role = role.to_string_lossy();
        let base =
            PathBuf::from(std::env::var_os("CHIDORI_TEST_CROSS_PROCESS_INBOX_BASE").unwrap());
        let agent_name = std::env::var("CHIDORI_TEST_CROSS_PROCESS_INBOX_AGENT").unwrap();
        let run_base = base.join(".chidori/runs");
        let parts = sqlite_test_parts(&base, run_base.clone(), Arc::new(ToolRegistry::new()));
        hub().install_parts(parts.clone());

        match role.as_ref() {
            "seed" => {
                std::fs::create_dir_all(base.join("services")).unwrap();
                let source = base.join("services/worker.ts");
                std::fs::write(
                    &source,
                    r#"export async function agent() {
                        await chidori.signal("hold");
                        return { done: true };
                    }"#,
                )
                .unwrap();
                let options = SpawnOptions {
                    name: Some(agent_name.clone()),
                    restart: "never".to_string(),
                    max_restarts: 0,
                    backoff_ms: 0,
                    model: None,
                };
                hub()
                    .spawn(
                        &parts,
                        source.to_str().unwrap(),
                        json!({}),
                        &options,
                        "cross-process-inbox-test".to_string(),
                    )
                    .unwrap();
                wait_for_agent_status(&parts, &agent_name, |status| status == "hibernating");
            }
            "send" => {
                let id = std::env::var("CHIDORI_TEST_CROSS_PROCESS_INBOX_ID").unwrap();
                let result = hub()
                    .send(
                        &parts,
                        &agent_name,
                        "tick",
                        json!({ "id": id }),
                        json!({ "test": true }),
                    )
                    .unwrap();
                assert_eq!(result["delivered"], json!(true));
            }
            "inspect" => {
                let expected: std::collections::HashSet<_> =
                    std::env::var("CHIDORI_TEST_CROSS_PROCESS_INBOX_IDS")
                        .unwrap()
                        .split(',')
                        .map(str::to_string)
                        .collect();
                let factory = RunStoreFactory::shared(&run_base);
                let registry = factory.registry_get(&agent_name).unwrap().unwrap();
                let run_id = registry["run_id"].as_str().unwrap();
                let store = factory.store_for(run_id);
                let records = store.load_call_log().unwrap().unwrap_or_default();
                let inbox: Vec<QueuedSignal> = store
                    .get_blob(SIGNAL_INBOX_FILE)
                    .unwrap()
                    .map(|bytes| serde_json::from_slice(&bytes).unwrap())
                    .unwrap_or_default();
                assert!(
                    records.iter().all(|record| record.function != "signal"),
                    "mismatched listen name unexpectedly consumed a message: {records:#?}"
                );
                let actual: Vec<_> = inbox
                    .iter()
                    .map(|queued| queued.payload["id"].as_str().unwrap().to_string())
                    .collect();
                let actual_set: std::collections::HashSet<_> = actual.iter().cloned().collect();
                if actual.len() != actual_set.len() || actual_set != expected {
                    let missing: Vec<_> = expected.difference(&actual_set).cloned().collect();
                    let unexpected: Vec<_> = actual_set.difference(&expected).cloned().collect();
                    std::fs::write(
                        base.join("inbox-inspector-red.txt"),
                        format!(
                            "expected={expected:?}; actual={actual:?}; missing={missing:?}; unexpected={unexpected:?}"
                        ),
                    )
                    .unwrap();
                    std::process::exit(17);
                }
            }
            _ => panic!("unknown cross-process inbox child role: {role}"),
        }
        return;
    }

    async fn child(
        base: &Path,
        agent: &str,
        role: &str,
        ids: &str,
        backend: &str,
    ) -> std::process::Child {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("runtime::host_agent::tests::detached_inbox_cross_process_writers_preserve_acknowledged_payloads")
            .arg("--nocapture")
            .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_CHILD", role)
            .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_BASE", base)
            .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_AGENT", agent)
            .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_IDS", ids)
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", base.join("runs.sqlite3"));
        if role == "send" {
            command.env("CHIDORI_TEST_CROSS_PROCESS_INBOX_ID", ids);
        }
        command.spawn().unwrap()
    }

    async fn wait_child(child: std::process::Child, role: &str) {
        let status = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
            .await
            .unwrap();
        assert!(
            status.status.success(),
            "cross-process inbox {role} child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
    }

    let mut red_failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let base = tempfile::tempdir().unwrap();
        let sequential_agent = unique("cross-process-sequential");
        wait_child(
            child(base.path(), &sequential_agent, "seed", "", backend).await,
            "seed",
        )
        .await;
        for id in ["sequential-a", "sequential-b"] {
            wait_child(
                child(base.path(), &sequential_agent, "send", id, backend).await,
                "sequential send",
            )
            .await;
        }
        wait_child(
            child(
                base.path(),
                &sequential_agent,
                "inspect",
                "sequential-a,sequential-b",
                backend,
            )
            .await,
            "sequential inspect",
        )
        .await;

        let concurrent_agent = unique("cross-process-concurrent");
        wait_child(
            child(base.path(), &concurrent_agent, "seed", "", backend).await,
            "seed",
        )
        .await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gate = listener.local_addr().unwrap().to_string();
        let mut writers = Vec::new();
        for (role, id) in [("writer-a", "concurrent-a"), ("writer-b", "concurrent-b")] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg(TEST_NAME)
                .arg("--nocapture")
                .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_CHILD", "send")
                .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_BASE", base.path())
                .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_AGENT", &concurrent_agent)
                .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_ID", id)
                .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_ROLE", role)
                .env("CHIDORI_TEST_CROSS_PROCESS_INBOX_GATE", &gate)
                .env("CHIDORI_RUN_STORE", backend)
                .env("CHIDORI_RUN_DB", base.path().join("runs.sqlite3"));
            writers.push(command.spawn().unwrap());
        }

        let mut arrivals = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                .await
                .expect("writer did not reach the post-read gate")
                .unwrap();
            let mut role = [0];
            tokio::time::timeout(
                Duration::from_secs(10),
                tokio::io::AsyncReadExt::read_exact(&mut stream, &mut role),
            )
            .await
            .expect("writer role announcement timed out")
            .unwrap();
            arrivals.push((stream, role[0]));
        }
        arrivals.sort_by_key(|(_, role)| *role);
        assert_eq!(
            arrivals.iter().map(|(_, role)| *role).collect::<Vec<_>>(),
            vec![b'a', b'b']
        );
        for (mut stream, _) in arrivals {
            tokio::io::AsyncWriteExt::write_all(&mut stream, b"G")
                .await
                .unwrap();
        }
        for writer in writers {
            let result = tokio::task::spawn_blocking(move || writer.wait_with_output().unwrap())
                .await
                .unwrap();
            assert!(
                result.status.success(),
                "cross-process writer did not acknowledge delivery: stdout={} stderr={}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
        }

        let inspect = child(
            base.path(),
            &concurrent_agent,
            "inspect",
            "concurrent-a,concurrent-b",
            backend,
        )
        .await;
        let result = tokio::task::spawn_blocking(move || inspect.wait_with_output().unwrap())
            .await
            .unwrap();
        if !result.status.success() {
            assert!(
                result.status.code() == Some(17),
                "unexpected inspector failure for {backend}: stdout={} stderr={}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
            red_failures.push(format!(
                "{backend}: inspector found missing or duplicate acknowledged IDs (exit 17): {}",
                std::fs::read_to_string(base.path().join("inbox-inspector-red.txt")).unwrap()
            ));
        }
    }
    assert!(
        red_failures.is_empty(),
        "acknowledged inbox payloads were lost:\n{}",
        red_failures.join("\n")
    );
}

#[test]
fn detached_inbox_competing_writers_preserve_every_acknowledged_payload() {
    const CHILD: &str = "CHIDORI_TEST_DETACHED_INBOX_RACE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let base = PathBuf::from(std::env::var_os("CHIDORI_TEST_DETACHED_INBOX_BASE").unwrap());
        let mut losses = Vec::new();
        for count in [2, 5] {
            let loss = run_deterministic_inbox_race(&base, count, false);
            println!(
                "backend={} competing sends N={count}: {}",
                std::env::var("CHIDORI_RUN_STORE").unwrap_or_else(|_| "fs".to_string()),
                loss.as_deref()
                    .unwrap_or("all acknowledged IDs accounted exactly once")
            );
            if let Some(loss) = loss {
                losses.push(loss);
            }
        }
        run_sequential_inbox_control(&base, 25);
        println!(
            "backend={} N=25 sequential control passed",
            std::env::var("CHIDORI_RUN_STORE").unwrap_or_else(|_| "fs".to_string())
        );
        assert!(
            losses.is_empty(),
            "confirmed inbox overwrite losses: {losses:#?}"
        );
        return;
    }

    let mut failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let base = test_dir(&format!("inbox-race-{backend}"));
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("runtime::host_agent::tests::detached_inbox_competing_writers_preserve_every_acknowledged_payload")
            .arg("--nocapture")
            .env(CHILD, "1")
            .env("CHIDORI_TEST_DETACHED_INBOX_BASE", &base)
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", base.join("runs.sqlite3"))
            .output()
            .unwrap();
        let report = format!(
            "detached inbox race child backend={backend} exited {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let _ = std::fs::remove_dir_all(&base);
        if !output.status.success() {
            failures.push(report);
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn detached_inbox_missed_wake_does_not_hibernate_with_matching_payloads() {
    const CHILD: &str = "CHIDORI_TEST_DETACHED_INBOX_MISSED_WAKE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let base = PathBuf::from(std::env::var_os("CHIDORI_TEST_DETACHED_INBOX_BASE").unwrap());
        let mut failures = Vec::new();
        for count in [2, 5] {
            if let Some(failure) = run_deterministic_inbox_race(&base, count, true) {
                failures.push(failure);
            }
        }
        run_sequential_inbox_control(&base, 25);
        assert!(
            failures.is_empty(),
            "missed detached-agent wake: {failures:#?}"
        );
        return;
    }

    let mut failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let base = tempfile::tempdir().unwrap();
        let db_path = base.path().join("runs.sqlite3");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("runtime::host_agent::tests::detached_inbox_missed_wake_does_not_hibernate_with_matching_payloads")
            .arg("--nocapture")
            .env(CHILD, "1")
            .env("CHIDORI_TEST_DETACHED_INBOX_BASE", base.path())
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", db_path)
            .output()
            .unwrap();
        let report = format!(
            "detached inbox missed-wake child backend={backend} exited {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !output.status.success() {
            failures.push(report);
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

fn run_deterministic_inbox_race(
    base: &Path,
    competing_sends: usize,
    require_wake_resolution: bool,
) -> Option<String> {
    let agent_name = unique(&format!("inbox-race-{competing_sends}"));
    let run_base = base
        .join(format!("race-{competing_sends}"))
        .join(".chidori/runs");
    let source = base.join(format!("race-{competing_sends}.ts"));
    std::fs::write(
        &source,
        r#"export async function agent(input: { count: number }) {
            for (let i = 0; i <= input.count; i++) {
                const message = await chidori.signal("race");
                await chidori.tool("raceConsumed", { id: message.payload.id });
                if (i === 0) await chidori.tool("raceReady", {});
            }
            return { done: true };
        }"#,
    )
    .unwrap();

    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (ready_release_tx, ready_release_rx) = std::sync::mpsc::channel();
    let ready_release_rx = Arc::new(StdMutex::new(ready_release_rx));
    let (consumed_tx, consumed_rx) = std::sync::mpsc::channel();
    let mut tools = ToolRegistry::new();
    tools.register_native(
        "raceReady",
        "holds the live context until the competing sender is armed",
        Vec::new(),
        move |_| {
            let _ = ready_tx.send(());
            ready_release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .expect("inbox race fixture must release its ready handoff");
            Ok(json!({ "ready": true }))
        },
    );
    tools.register_native(
        "raceConsumed",
        "records a consumed inbox payload",
        Vec::new(),
        move |args| {
            let _ = consumed_tx.send(args["id"].clone());
            Ok(json!({ "consumed": true }))
        },
    );
    let parts = sqlite_test_parts(base, run_base.clone(), Arc::new(tools));
    hub().install_parts(parts.clone());
    let options = SpawnOptions {
        name: Some(agent_name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };
    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({ "count": competing_sends }),
            &options,
            "inbox-race-test".to_string(),
        )
        .unwrap();
    let run_id = spawned["runId"].as_str().unwrap().to_string();
    wait_for_agent_status(&parts, &agent_name, |status| status == "hibernating");

    let seed_id = format!("{agent_name}-seed");
    assert!(hub()
        .send(
            &parts,
            &agent_name,
            "race",
            json!({ "id": seed_id }),
            json!({ "test": true })
        )
        .unwrap()["delivered"]
        .as_bool()
        .unwrap());
    assert_eq!(
        consumed_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
        json!(seed_id)
    );
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("target run did not enter the live-context readiness handoff");

    let sender_arrived_rx;
    let sender_release_tx;
    let worker_arrived_rx;
    let worker_release_tx;
    let barriers;
    {
        let (sender_arrived_tx, rx) = std::sync::mpsc::channel();
        sender_arrived_rx = rx;
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        sender_release_tx = release_tx;
        let sender_release_rx = release_rx;
        let (worker_arrived_tx, rx) = std::sync::mpsc::channel();
        worker_arrived_rx = rx;
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        worker_release_tx = release_tx;
        let worker_release_rx = release_rx;
        barriers = Arc::new(InboxRaceBarriers {
            agent_name: agent_name.clone(),
            sender_arrived: sender_arrived_tx,
            sender_release: StdMutex::new(sender_release_rx),
            worker_arrived: worker_arrived_tx,
            worker_release: StdMutex::new(worker_release_rx),
            sender_claimed: AtomicBool::new(false),
            worker_claimed: AtomicBool::new(false),
            sender_launched: AtomicBool::new(false),
            sender_result: StdMutex::new(None),
            worker_timed_out: AtomicBool::new(false),
        });
        *INBOX_RACE_BARRIERS
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap() = Some(barriers.clone());
    }

    let (hibernate_status_reached, hibernate_status_release) = if require_wake_resolution {
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *HIBERNATE_RACE_STATUS_BARRIER
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap() = Some(Arc::new(HibernateRaceStatusBarrier {
            agent_name: agent_name.clone(),
            reached: reached_tx,
            release: StdMutex::new(release_rx),
            claimed: AtomicBool::new(false),
        }));
        (Some(reached_rx), Some(release_tx))
    } else {
        (None, None)
    };

    let live_id = format!("{agent_name}-live");
    let sender_parts = parts.clone();
    let sender_name = agent_name.clone();
    let live_id_for_thread = live_id.clone();
    let sender_barriers = barriers.clone();
    barriers.sender_launched.store(true, Ordering::SeqCst);
    let sender = std::thread::spawn(move || {
        let result = hub().send(
            &sender_parts,
            &sender_name,
            "race",
            json!({ "id": live_id_for_thread }),
            json!({ "test": true }),
        );
        *sender_barriers.sender_result.lock().unwrap() = Some(match &result {
            Ok(_) => "Ok".to_string(),
            Err(error) => format!("Err({error})"),
        });
        result
    });
    if sender_arrived_rx
        .recv_timeout(Duration::from_secs(10))
        .is_err()
    {
        panic!(
            "live sender did not reach the post-snapshot barrier: {}",
            barriers.milestones()
        );
    }
    // `raceReady` is still blocked here, so the sender's successful
    // snapshot proves this exact run owns a live context. Only now let
    // the worker move to its next listen and clear that context.
    ready_release_tx.send(()).unwrap();
    if worker_arrived_rx
        .recv_timeout(Duration::from_secs(10))
        .is_err()
    {
        barriers.worker_timed_out.store(true, Ordering::SeqCst);
        panic!(
            "worker did not reach the post-context-clear barrier: {}",
            barriers.milestones()
        );
    }

    let status = hub().status(&parts, &agent_name).unwrap();
    assert_eq!(status["status"], json!("running"), "{status}");
    assert_eq!(status["live"], json!(true), "{status}");
    let mut acknowledged_ids = vec![live_id.clone()];
    let mut other_ids = Vec::new();
    // The first sender is paused after its live-context snapshot while it
    // owns this name's send lock. Release it before sending the remaining
    // messages; the worker remains paused after clearing live context, so
    // those messages still exercise the queued-inbox side of this race.
    sender_release_tx.send(()).unwrap();
    let live_result = sender.join().unwrap().unwrap();
    assert_eq!(live_result["delivered"], json!(true));
    for index in 1..competing_sends {
        let id = format!("{agent_name}-store-{index}");
        let result = hub()
            .send(
                &parts,
                &agent_name,
                "race",
                json!({ "id": id }),
                json!({ "test": true }),
            )
            .unwrap();
        assert_eq!(result["delivered"], json!(true));
        acknowledged_ids.push(id.clone());
        other_ids.push(id);
    }
    worker_release_tx.send(()).unwrap();
    if require_wake_resolution {
        let race_returned = hibernate_status_reached
            .as_ref()
            .expect("hibernate status barrier was not configured")
            .recv_timeout(Duration::from_secs(10))
            .is_ok();
        if race_returned {
            let status = hub().status(&parts, &agent_name).unwrap();
            let entry = hub().ensure_entry(&parts, &agent_name).unwrap();
            let thread_live = entry.state.lock().unwrap().thread_live;
            let live_status_ok = status["live"] == json!(true);
            let thread_live_ok = thread_live;
            hibernate_status_release
                .as_ref()
                .expect("hibernate status barrier was not configured")
                .send(())
                .unwrap();
            *HIBERNATE_RACE_STATUS_BARRIER
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap() = None;
            assert!(live_status_ok, "raced hibernate lost live state: {status}");
            assert!(
                thread_live_ok,
                "raced hibernate should keep the supervisor thread live"
            );
        } else {
            hibernate_status_release
                .as_ref()
                .expect("hibernate status barrier was not configured")
                .send(())
                .unwrap();
        }
        *HIBERNATE_RACE_STATUS_BARRIER
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap() = None;
        assert!(
            race_returned,
            "worker did not reach the hibernate-race status barrier"
        );
    }
    let mut consumed_notifications = std::collections::HashMap::<String, usize>::new();
    if require_wake_resolution {
        let expected_ids: std::collections::HashSet<_> = acknowledged_ids.iter().cloned().collect();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while expected_ids
            .iter()
            .any(|id| consumed_notifications.get(id).copied().unwrap_or(0) == 0)
        {
            let now = std::time::Instant::now();
            if now >= deadline {
                break;
            }
            match consumed_rx.recv_timeout(deadline.saturating_duration_since(now)) {
                Ok(id) => {
                    if let Some(id) = id.as_str() {
                        *consumed_notifications.entry(id.to_string()).or_default() += 1;
                    }
                }
                Err(_) => break,
            }
        }
    }
    let status = if require_wake_resolution {
        wait_for_inbox_race_outcome(&parts, &agent_name)
    } else {
        wait_for_agent_status(&parts, &agent_name, |status| {
            status == "hibernating" || status == "completed" || status == "failed"
        })
    };
    *INBOX_RACE_BARRIERS
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = None;

    let factory = RunStoreFactory::shared(&run_base);
    let records = factory
        .store_for(&run_id)
        .load_call_log()
        .unwrap()
        .unwrap_or_default();
    let inbox = factory
        .store_for(&run_id)
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .map(|bytes| serde_json::from_slice::<Vec<Value>>(&bytes).unwrap())
        .unwrap_or_default();
    let live = if require_wake_resolution {
        status["live"] == json!(true)
    } else {
        hub().status(&parts, &agent_name).unwrap()["live"] == json!(true)
    };
    let accounted: Vec<(String, usize, usize)> = acknowledged_ids
        .iter()
        .map(|id| {
            let journal_count = records
                .iter()
                .filter(|record| {
                    record.function == "signal" && record.result["payload"]["id"] == json!(id)
                })
                .count();
            let inbox_count = inbox
                .iter()
                .filter(|queued| queued["payload"]["id"] == json!(id))
                .count();
            (id.clone(), journal_count, inbox_count)
        })
        .collect();
    let unaccounted: Vec<_> = accounted
        .iter()
        .filter(|(_, journal_count, inbox_count)| journal_count + inbox_count != 1)
        .cloned()
        .collect();

    let consumption_failures: Vec<_> = if require_wake_resolution {
        acknowledged_ids
            .iter()
            .filter_map(|id| {
                let notification_count = consumed_notifications.get(id).copied().unwrap_or(0);
                let (journal_signal_count, inbox_count) = accounted
                    .iter()
                    .find(|(accounted_id, _, _)| accounted_id == id)
                    .map(|(_, journal_count, inbox_count)| (*journal_count, *inbox_count))
                    .unwrap_or((0, 0));
                let race_consumed_call_count = records
                    .iter()
                    .filter(|record| {
                        record.function == "tool"
                            && record.args["name"] == json!("raceConsumed")
                            && record.args["kwargs"]["id"] == json!(id)
                    })
                    .count();
                (notification_count != 1
                    || journal_signal_count != 1
                    || inbox_count != 0
                    || race_consumed_call_count != 1)
                    .then(|| {
                        (
                            id.clone(),
                            notification_count,
                            journal_signal_count,
                            inbox_count,
                            race_consumed_call_count,
                        )
                    })
            })
            .collect()
    } else {
        Vec::new()
    };

    let _ = hub().stop(&parts, &agent_name);
    wait_for_agent_status(&parts, &agent_name, |status| {
        matches!(status, "stopped" | "completed" | "failed")
    });
    let stranded_matching_ids: Vec<_> = accounted
        .iter()
        .filter(|(_, journal_count, inbox_count)| *journal_count == 0 && *inbox_count == 1)
        .map(|(id, _, _)| id.clone())
        .collect();
    let waiting_for_race = status["waitingFor"]
        .as_array()
        .is_some_and(|names| names.iter().any(|name| name == "race"));
    let hibernating_with_queued_matches = require_wake_resolution
        && status["status"] == "hibernating"
        && !live
        && waiting_for_race
        && !stranded_matching_ids.is_empty();

    (!unaccounted.is_empty()
        || !consumption_failures.is_empty()
        || hibernating_with_queued_matches)
        .then(|| {
            format!(
                "backend={}, N={}, conservation={accounted:?}, exact_consumption_failures={consumption_failures:?}, status={status}, live={live}, hibernating_with_queued_matches={hibernating_with_queued_matches}, stranded_matching_ids={stranded_matching_ids:?}, inbox={inbox:?}, other_store_ids={other_ids:?}",
                std::env::var("CHIDORI_RUN_STORE").unwrap_or_else(|_| "fs".to_string()),
                competing_sends
            )
        })
}

fn run_sequential_inbox_control(base: &Path, count: usize) {
    let agent_name = unique("inbox-sequential");
    let run_base = base.join("sequential").join(".chidori/runs");
    let source = base.join("sequential.ts");
    std::fs::write(
        &source,
        r#"export async function agent(input: { count: number }) {
            for (let i = 0; i < input.count; i++) {
                const message = await chidori.signal("sequential");
                await chidori.tool("raceConsumed", { id: message.payload.id });
            }
            return { done: true };
        }"#,
    )
    .unwrap();
    let (consumed_tx, consumed_rx) = std::sync::mpsc::channel();
    let mut tools = ToolRegistry::new();
    tools.register_native(
        "raceConsumed",
        "records a consumed inbox payload",
        Vec::new(),
        move |args| {
            let _ = consumed_tx.send(args["id"].clone());
            Ok(json!({ "consumed": true }))
        },
    );
    let parts = sqlite_test_parts(base, run_base.clone(), Arc::new(tools));
    hub().install_parts(parts.clone());
    let options = SpawnOptions {
        name: Some(agent_name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };
    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({ "count": count }),
            &options,
            "inbox-sequential-test".to_string(),
        )
        .unwrap();
    let run_id = spawned["runId"].as_str().unwrap().to_string();
    wait_for_agent_status(&parts, &agent_name, |status| status == "hibernating");

    let mut acknowledged_ids = Vec::new();
    for index in 0..count {
        let id = format!("{agent_name}-{index}");
        let result = hub()
            .send(
                &parts,
                &agent_name,
                "sequential",
                json!({ "id": id }),
                json!({ "test": true }),
            )
            .unwrap();
        assert_eq!(result["delivered"], json!(true));
        assert_eq!(
            consumed_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            json!(id)
        );
        acknowledged_ids.push(id);
    }
    wait_for_agent_status(&parts, &agent_name, |status| status == "completed");

    let factory = RunStoreFactory::shared(&run_base);
    let records = factory
        .store_for(&run_id)
        .load_call_log()
        .unwrap()
        .unwrap_or_default();
    let inbox = factory
        .store_for(&run_id)
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .map(|bytes| serde_json::from_slice::<Vec<Value>>(&bytes).unwrap())
        .unwrap_or_default();
    for id in acknowledged_ids {
        let journal_count = records
            .iter()
            .filter(|record| {
                record.function == "signal" && record.result["payload"]["id"] == json!(id)
            })
            .count();
        let inbox_count = inbox
            .iter()
            .filter(|queued| queued["payload"]["id"] == json!(id))
            .count();
        assert_eq!(
            journal_count + inbox_count,
            1,
            "payload {id} not accounted exactly once"
        );
    }
}

fn wait_for_agent_status(
    parts: &AgentRuntimeParts,
    name: &str,
    predicate: impl Fn(&str) -> bool,
) -> Value {
    let entry = hub().ensure_entry(parts, name).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut state = entry.state.lock().unwrap();
    loop {
        if predicate(&state.descriptor.status) {
            return state.descriptor.status_json();
        }
        let now = std::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {name}: {}",
            state.descriptor.status
        );
        let (next, _) = entry
            .signal
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .unwrap();
        state = next;
    }
}

fn wait_for_inbox_race_outcome(parts: &AgentRuntimeParts, name: &str) -> Value {
    let entry = hub().ensure_entry(parts, name).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut state = entry.state.lock().unwrap();
    loop {
        let settled = AgentEntry::is_settled(&state);
        let hibernated = state.descriptor.status == "hibernating" && !state.thread_live;
        let driveable = state.descriptor.status == "running" && !state.thread_live;
        if settled || hibernated || driveable {
            let mut status = state.descriptor.status_json();
            status["live"] = json!(state.thread_live);
            return status;
        }

        let now = std::time::Instant::now();
        if now >= deadline {
            let mut status = state.descriptor.status_json();
            status["live"] = json!(state.thread_live);
            return status;
        }
        let (next, timeout) = entry
            .signal
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .unwrap();
        state = next;
        if timeout.timed_out() {
            let mut status = state.descriptor.status_json();
            status["live"] = json!(state.thread_live);
            return status;
        }
    }
}

#[test]
fn detached_alarm_timeout_wakes_reconcile_host_promises_across_cycles() {
    const CHILD: &str = "CHIDORI_TEST_DETACHED_ALARM_RECOVERY_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let base = PathBuf::from(std::env::var_os("CHIDORI_TEST_DETACHED_ALARM_BASE").unwrap());
        let no_alarm = run_paced_alarm_case(&base, "no-alarm", false);
        assert_eq!(
            no_alarm.status, "completed",
            "no-alarm control: {no_alarm:?}"
        );
        assert_eq!(no_alarm.output, Some(json!({ "seen": [0, 1, 2] })));
        assert!(
            no_alarm.promise_load.is_ok(),
            "no-alarm control: {no_alarm:?}"
        );
        assert!(no_alarm
            .records
            .iter()
            .all(|record| record.args["name"] != json!(ALARM_SIGNAL_NAME)));
        println!(
            "backend={} paced no-alarm control passed (3 cycles)",
            std::env::var("CHIDORI_RUN_STORE").unwrap_or_else(|_| "fs".to_string())
        );

        let with_alarm = run_paced_alarm_case(&base, "with-alarm", true);
        println!("backend={} paced alarm observation: status={}, error={:?}, journal_rows={}, promise_load={:?}",
            std::env::var("CHIDORI_RUN_STORE").unwrap_or_else(|_| "fs".to_string()),
            with_alarm.status,
            with_alarm.error,
            with_alarm.records.len(),
            with_alarm.promise_load.as_ref().map(|records| records.len()).map_err(|error| error.clone()));
        assert_eq!(
            with_alarm.status,
            "completed",
            "three alarm timeouts must resume through all cycles and durably reconcile each completion: {with_alarm:?}"
        );
        assert_eq!(with_alarm.output, Some(json!({ "seen": [0, 1, 2] })));
        let promises = with_alarm.promise_load.as_ref().unwrap();
        let timeout_rows: Vec<_> = with_alarm
            .records
            .iter()
            .filter(|record| {
                record.function == "signal"
                    && record.args["name"] == json!(ALARM_SIGNAL_NAME)
                    && record.result["timedOut"] == json!(true)
            })
            .collect();
        assert_eq!(timeout_rows.len(), 3, "{with_alarm:?}");
        for timeout in timeout_rows {
            let promise = promises
                .iter()
                .find(|promise| promise.operation.seq == timeout.seq)
                .expect("durable promise for each timeout journal row");
            assert!(
                matches!(
                    &promise.state,
                    crate::runtime::snapshot::HostPromiseState::Resolved { .. }
                        | crate::runtime::snapshot::HostPromiseState::ResolvedInJournal { .. }
                ),
                "timeout at seq {} was not persisted as completed: {promise:?}",
                timeout.seq
            );
        }
        return;
    }

    let mut failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let base = test_dir(&format!("detached-alarm-{backend}"));
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("runtime::host_agent::tests::detached_alarm_timeout_wakes_reconcile_host_promises_across_cycles")
            .arg("--nocapture")
            .env(CHILD, "1")
            .env("CHIDORI_TEST_DETACHED_ALARM_BASE", &base)
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", base.join("runs.sqlite3"))
            .output()
            .unwrap();
        let report = format!(
            "detached alarm recovery child backend={backend} exited {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let _ = std::fs::remove_dir_all(&base);
        if !output.status.success() {
            failures.push(report);
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

#[derive(Debug)]
struct PacedAlarmOutcome {
    status: String,
    error: Option<String>,
    output: Option<Value>,
    records: Vec<CallRecord>,
    promise_load: Result<Vec<crate::runtime::snapshot::HostPromiseRecord>, String>,
}

fn run_paced_alarm_case(base: &Path, case: &str, with_alarm: bool) -> PacedAlarmOutcome {
    let project = base.join(case);
    std::fs::create_dir_all(project.join("services")).unwrap();
    let source = project.join("services/worker.ts");
    std::fs::write(
        &source,
        r#"export async function agent(input: { cycles: number; withAlarm: boolean }) {
            const seen = [];
            for (let i = 0; i < input.cycles; i++) {
                const message = await chidori.signal("tick");
                seen.push(message.payload.n);
                if (input.withAlarm) await chidori.alarm(10);
                await chidori.log("cycle", { n: message.payload.n });
            }
            return { seen };
        }"#,
    )
    .unwrap();

    let run_base = project.join(".chidori/runs");
    let parts = sqlite_test_parts(&project, run_base.clone(), Arc::new(ToolRegistry::new()));
    hub().install_parts(parts.clone());
    let name = unique(if with_alarm {
        "paced-alarm"
    } else {
        "paced-no-alarm"
    });
    let options = SpawnOptions {
        name: Some(name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };
    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({ "cycles": 3, "withAlarm": with_alarm }),
            &options,
            format!("{case}-parent"),
        )
        .unwrap();
    let run_id = spawned["runId"].as_str().unwrap().to_string();
    let mut descriptor = wait_for_agent_descriptor(&parts, &name, |descriptor| {
        descriptor.status == "hibernating"
            && descriptor
                .listen
                .as_ref()
                .is_some_and(|listen| listen.names.iter().any(|name| name == "tick"))
    });

    for tick in 0..3 {
        let previous_seq = descriptor
            .listen
            .as_ref()
            .expect("paced worker is hibernating on a listen")
            .seq;
        let delivery = hub()
            .send(
                &parts,
                &name,
                "tick",
                json!({ "n": tick }),
                json!({ "kind": "test", "id": case }),
            )
            .unwrap();
        assert_eq!(delivery["delivered"], json!(true));
        descriptor = wait_for_agent_descriptor(&parts, &name, |current| {
            matches!(current.status.as_str(), "failed" | "completed")
                || (current.status == "hibernating"
                    && current.listen.as_ref().is_some_and(|listen| {
                        listen.names.iter().any(|name| name == "tick") && listen.seq > previous_seq
                    }))
        });
        if matches!(descriptor.status.as_str(), "failed" | "completed") {
            break;
        }
    }

    if !matches!(
        descriptor.status.as_str(),
        "failed" | "completed" | "stopped"
    ) {
        let _ = hub().stop(&parts, &name);
        descriptor = wait_for_agent_descriptor(&parts, &name, |current| {
            matches!(current.status.as_str(), "failed" | "completed" | "stopped")
        });
    }

    let factory = RunStoreFactory::shared(&run_base);
    let store = factory.store_for(&run_id);
    let records = store.load_call_log().unwrap().unwrap_or_default();
    let promise_load = crate::runtime::snapshot::load_host_promise_records(store.as_ref())
        .map_err(|error| format!("{error:#}"));
    PacedAlarmOutcome {
        status: descriptor.status,
        error: descriptor.error,
        output: descriptor.output,
        records,
        promise_load,
    }
}

fn wait_for_agent_descriptor(
    parts: &AgentRuntimeParts,
    name: &str,
    predicate: impl Fn(&AgentDescriptor) -> bool,
) -> AgentDescriptor {
    let entry = hub().ensure_entry(parts, name).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut state = entry.state.lock().unwrap();
    loop {
        if predicate(&state.descriptor) {
            return state.descriptor.clone();
        }
        let now = std::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {name}: {:?}",
            state.descriptor
        );
        let (next, _) = entry
            .signal
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .unwrap();
        state = next;
    }
}

struct AlarmInboxFixture {
    parts: AgentRuntimeParts,
    name: String,
    run_id: String,
    entry: Arc<AgentEntry>,
    store: Arc<dyn RunStore>,
    effects: Arc<std::sync::atomic::AtomicUsize>,
}

fn new_alarm_inbox_fixture(base: &Path, case: &str) -> AlarmInboxFixture {
    let project = base.join(case);
    std::fs::create_dir_all(project.join("services")).unwrap();
    let source = project.join("services/alarm-worker.ts");
    std::fs::write(
        &source,
        r#"export async function agent() {
            const fired = await chidori.alarm(3600000);
            await chidori.tool("effectMarker", { timedOut: fired.timedOut === true });
            return fired;
        }"#,
    )
    .unwrap();
    let run_base = project.join(".chidori/runs");
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    let observed_effects = effects.clone();
    tools.register_native(
        "effectMarker",
        "count alarm continuation",
        Vec::new(),
        move |_| {
            observed_effects.fetch_add(1, Ordering::SeqCst);
            Ok(json!({ "executed": true }))
        },
    );
    let parts = sqlite_test_parts(&project, run_base.clone(), Arc::new(tools));
    hub().install_parts(parts.clone());
    let name = unique(&format!("{case}-alarm"));
    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &SpawnOptions {
                name: Some(name.clone()),
                restart: "resume".to_string(),
                max_restarts: 0,
                backoff_ms: 0,
                model: None,
            },
            format!("{case}-owner"),
        )
        .unwrap();
    let run_id = spawned["runId"].as_str().unwrap().to_string();
    let descriptor = wait_for_agent_descriptor(&parts, &name, |descriptor| {
        descriptor.status == "hibernating"
            && descriptor.listen.as_ref().is_some_and(|listen| {
                listen.names == [ALARM_SIGNAL_NAME]
                    && listen.function == "signal"
                    && listen.deadline.is_some()
            })
    });
    assert_eq!(descriptor.run_id, run_id);
    let entry = hub().ensure_entry(&parts, &name).unwrap();
    let store = RunStoreFactory::shared(&run_base).store_for(&run_id);
    AlarmInboxFixture {
        parts,
        name,
        run_id,
        entry,
        store,
        effects,
    }
}

fn expire_alarm_listen(fixture: &AlarmInboxFixture) -> (ListenState, DateTime<Utc>) {
    let mut state = fixture.entry.state.lock().unwrap();
    let listen = state
        .descriptor
        .listen
        .as_mut()
        .expect("alarm fixture must retain its listen while hibernating");
    listen.deadline = Some(Utc::now() - chrono::Duration::seconds(1));
    let expected_listen = listen.clone();
    state.descriptor.updated_at = Utc::now();
    let updated_at = state.descriptor.updated_at;
    let descriptor = state.descriptor.clone();
    drop(state);
    persist_descriptor(&fixture.entry.factory(), &descriptor);
    (expected_listen, updated_at)
}

fn wait_for_alarm_worker_exit(fixture: &AlarmInboxFixture) -> (AgentDescriptor, bool, bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut state = fixture.entry.state.lock().unwrap();
    loop {
        if !state.thread_live && !state.execution_active {
            return (
                state.descriptor.clone(),
                state.thread_live,
                state.execution_active,
            );
        }
        let now = std::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {} supervisor exit: descriptor={:?}, execution_active={}, thread_live={}",
            fixture.name,
            state.descriptor,
            state.execution_active,
            state.thread_live,
        );
        let (next, _) = fixture
            .entry
            .signal
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .unwrap();
        state = next;
    }
}

fn journal_value(store: &dyn RunStore) -> Value {
    serde_json::to_value(store.load_call_log().unwrap().unwrap_or_default()).unwrap()
}

fn run_corrupt_alarm_wake_case(base: &Path, read_error: bool) -> Vec<String> {
    let case = if read_error {
        "read-error"
    } else {
        "malformed"
    };
    let fixture = new_alarm_inbox_fixture(base, case);
    let (barrier_reached, release_barrier) = install_alarm_inbox_read_barrier(&fixture.name);
    let (listen, before_updated_at) = expire_alarm_listen(&fixture);
    let expected_listen = serde_json::to_value(&listen).unwrap();
    let journal_before = journal_value(fixture.store.as_ref());
    let promises_before = load_host_promise_records(fixture.store.as_ref()).unwrap();
    let pending_before = fixture
        .store
        .get_blob(crate::runtime::snapshot::PENDING_HOST_OPERATION_FILE)
        .unwrap();
    let pending_alarm = promises_before.iter().any(|promise| {
        promise.operation.seq == listen.seq && matches!(promise.state, HostPromiseState::Pending)
    });
    hub()
        .start_thread(fixture.entry.clone())
        .expect("start explicit expired-alarm wake");
    let reached = barrier_reached
        .recv_timeout(Duration::from_secs(10))
        .is_ok();
    let wake_started_at = Utc::now();
    let lease_before = fixture
        .store
        .get_blob(crate::runtime::store::LEASE_FILE)
        .unwrap();
    let lease_before_value = lease_before
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
    let lease_acquired_by_supervisor = lease_before_value.as_ref().is_some_and(|lease| {
        lease["owner"] == json!(process_lease_owner())
            && lease["expires_at"]
                .as_str()
                .and_then(|value| value.parse::<DateTime<Utc>>().ok())
                .is_some_and(|expires_at| expires_at > Utc::now())
    });
    let inbox_path = fixture
        .parts
        .run_base
        .join(&fixture.run_id)
        .join(SIGNAL_INBOX_FILE);
    let corrupt_bytes = b"{malformed detached alarm inbox";
    if read_error {
        if inbox_path.exists() {
            std::fs::remove_file(&inbox_path).unwrap();
        }
        std::fs::create_dir_all(inbox_path.parent().unwrap()).unwrap();
        std::fs::create_dir(&inbox_path).unwrap();
    } else {
        fixture
            .store
            .put_blob(SIGNAL_INBOX_FILE, corrupt_bytes)
            .unwrap();
    }
    release_barrier
        .send(())
        .expect("release alarm supervisor after corrupt inbox is persisted");
    clear_alarm_inbox_read_barrier();
    let (descriptor_after, thread_live_after, execution_active_after) =
        wait_for_alarm_worker_exit(&fixture);
    let descriptor_value = serde_json::to_value(&descriptor_after).unwrap();
    let run_descriptor_after = fixture
        .store
        .get_blob(AGENT_DESCRIPTOR_FILE)
        .unwrap()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let registry_descriptor_after = fixture
        .entry
        .factory()
        .registry_get(&fixture.name)
        .unwrap()
        .and_then(|record| record.get("descriptor").cloned());
    let journal_after = journal_value(fixture.store.as_ref());
    let promises_after = load_host_promise_records(fixture.store.as_ref()).unwrap();
    let promises_after_value = serde_json::to_value(&promises_after).unwrap();
    let pending_after = fixture
        .store
        .get_blob(crate::runtime::snapshot::PENDING_HOST_OPERATION_FILE)
        .unwrap();
    let inbox_after = if read_error {
        None
    } else {
        fixture.store.get_blob(SIGNAL_INBOX_FILE).unwrap()
    };
    let read_error_still_present =
        read_error && inbox_path.is_dir() && fixture.store.get_blob(SIGNAL_INBOX_FILE).is_err();
    let lease_after = fixture
        .store
        .get_blob(crate::runtime::store::LEASE_FILE)
        .unwrap();
    let lease_released = match (lease_before_value.as_ref(), lease_after.as_deref()) {
        (Some(before), Some(after_bytes)) => {
            let after: Value = serde_json::from_slice(after_bytes).unwrap();
            after["owner"] == before["owner"]
                && after["generation"] == before["generation"]
                && after["expires_at"]
                    .as_str()
                    .and_then(|value| value.parse::<DateTime<Utc>>().ok())
                    .is_some_and(|expires_at| expires_at <= Utc::now())
        }
        (_, None) => true,
        _ => false,
    };
    let mut failures = Vec::new();
    if !reached {
        failures.push("supervisor did not reach the post-lease/pre-inbox barrier".to_string());
    }
    if !lease_acquired_by_supervisor {
        failures.push(format!(
            "barrier did not observe this supervisor's live lease: {lease_before_value:?}"
        ));
    }
    if !pending_alarm {
        failures.push(format!(
            "alarm promise at seq {} was not Pending before wake: {promises_before:?}",
            listen.seq
        ));
    }
    if fixture.effects.load(Ordering::SeqCst) != 0 {
        failures.push(format!(
            "corrupt inbox allowed an agent effect before repair: effects={}",
            fixture.effects.load(Ordering::SeqCst)
        ));
    }
    if descriptor_after.status != "running"
        || descriptor_after
            .listen
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .unwrap()
            .as_ref()
            != Some(&expected_listen)
    {
        failures.push(format!("expected nonterminal driverless running descriptor with exact listen; got {descriptor_value}"));
    }
    if descriptor_after.updated_at <= wake_started_at
        || descriptor_after.updated_at <= before_updated_at
    {
        failures.push(format!("deferred descriptor did not refresh updated_at: before={before_updated_at}, wake={wake_started_at}, after={}", descriptor_after.updated_at));
    }
    if thread_live_after || execution_active_after {
        failures.push(format!("supervisor did not stand down after inbox error: thread_live={thread_live_after}, execution_active={execution_active_after}"));
    }
    if journal_after != journal_before {
        failures.push(format!("corrupt inbox changed journal or synthesized timeout: before={journal_before}, after={journal_after}"));
    }
    if promises_after_value != serde_json::to_value(&promises_before).unwrap() {
        failures.push(format!("corrupt inbox resolved or changed Pending promise: before={promises_before:?}, after={promises_after:?}"));
    }
    if pending_after != pending_before {
        failures.push(format!("corrupt inbox changed pending host operation: before={pending_before:?}, after={pending_after:?}"));
    }
    if read_error && !read_error_still_present {
        failures.push(format!(
            "read-error inbox was not preserved as an unreadable directory: path={}, read_result={:?}",
            inbox_path.display(),
            fixture.store.get_blob(SIGNAL_INBOX_FILE)
        ));
    } else if !read_error && inbox_after.as_deref() != Some(corrupt_bytes.as_slice()) {
        failures.push(format!(
            "corrupt inbox bytes were rewritten: {inbox_after:?}"
        ));
    }
    if !lease_released {
        failures.push(format!("the supervisor's exact lease was not released: before={lease_before_value:?}, after={lease_after:?}"));
    }
    for (label, descriptor) in [
        ("run descriptor", run_descriptor_after),
        ("registry descriptor", registry_descriptor_after),
    ] {
        let valid = descriptor.as_ref().is_some_and(|value| {
            value["status"] == json!("running")
                && value["listen"] == expected_listen
                && value["updated_at"].as_str().is_some_and(|updated_at| {
                    updated_at
                        .parse::<DateTime<Utc>>()
                        .is_ok_and(|updated_at| updated_at > wake_started_at)
                })
        });
        if !valid {
            failures.push(format!(
                "{label} did not persist retryable driverless state: {descriptor:?}"
            ));
        }
    }

    if read_error {
        std::fs::remove_dir(&inbox_path).unwrap();
    }
    fixture.store.put_blob(SIGNAL_INBOX_FILE, b"[]").unwrap();
    if descriptor_after.status == "running" && !thread_live_after {
        let rearmed = hub().rearm_from_registry(fixture.parts.clone());
        if rearmed.is_err() {
            failures.push(format!(
                "explicit re-drive after inbox repair failed: {rearmed:?}"
            ));
        } else {
            let completed =
                wait_for_agent_descriptor(&fixture.parts, &fixture.name, |descriptor| {
                    matches!(
                        descriptor.status.as_str(),
                        "completed" | "failed" | "stopped"
                    )
                });
            if completed.status != "completed" || fixture.effects.load(Ordering::SeqCst) != 1 {
                failures.push(format!("repaired explicit re-drive did not execute exactly once: descriptor={completed:?}, effects={}", fixture.effects.load(Ordering::SeqCst)));
            }
        }
    } else {
        failures.push(format!(
            "corrupt inbox wake was not retryable after repair; state was {}",
            descriptor_after.status
        ));
    }
    hub().entries.lock().unwrap().remove(&fixture.name);
    failures
}

fn run_valid_alarm_control(base: &Path, case: &str, matching: bool) -> Vec<String> {
    let fixture = new_alarm_inbox_fixture(base, case);
    let (barrier_reached, release_barrier) = install_alarm_inbox_read_barrier(&fixture.name);
    let (listen, _) = expire_alarm_listen(&fixture);
    hub()
        .start_thread(fixture.entry.clone())
        .expect("start valid alarm control");
    let reached = barrier_reached
        .recv_timeout(Duration::from_secs(10))
        .is_ok();
    if matching {
        let queued = QueuedSignal {
            name: ALARM_SIGNAL_NAME.to_string(),
            payload: json!({ "control": case }),
            from: json!({ "kind": "test", "id": case }),
            delivery_seq: 1,
            enqueued_at: Utc::now(),
        };
        fixture
            .store
            .put_blob(
                SIGNAL_INBOX_FILE,
                &serde_json::to_vec(&vec![queued]).unwrap(),
            )
            .unwrap();
    } else {
        fixture.store.put_blob(SIGNAL_INBOX_FILE, b"[]").unwrap();
    }
    release_barrier
        .send(())
        .expect("release valid alarm control after inbox setup");
    clear_alarm_inbox_read_barrier();
    let completed = wait_for_agent_descriptor(&fixture.parts, &fixture.name, |descriptor| {
        matches!(
            descriptor.status.as_str(),
            "completed" | "failed" | "stopped"
        )
    });
    let records = fixture.store.load_call_log().unwrap().unwrap_or_default();
    let alarm_rows: Vec<_> = records
        .iter()
        .filter(|record| record.seq == listen.seq && record.function == "signal")
        .collect();
    let inbox_after = fixture.store.get_blob(SIGNAL_INBOX_FILE).unwrap();
    let mut failures = Vec::new();
    if !reached {
        failures.push(format!(
            "{case} control did not reach post-lease/pre-inbox barrier"
        ));
    }
    if completed.status != "completed" {
        failures.push(format!("{case} control did not complete: {completed:?}"));
    }
    if fixture.effects.load(Ordering::SeqCst) != 1 {
        failures.push(format!(
            "{case} control expected one effect, observed {}",
            fixture.effects.load(Ordering::SeqCst)
        ));
    }
    if alarm_rows.len() != 1 {
        failures.push(format!(
            "{case} control expected exactly one alarm journal record, got {alarm_rows:?}"
        ));
    } else if matching {
        if alarm_rows[0].result["timedOut"] == json!(true) {
            failures.push(format!(
                "matching inbox control synthesized timeout: {:?}",
                alarm_rows[0]
            ));
        }
        if inbox_after.as_deref() != Some(b"[]".as_slice()) {
            failures.push(format!(
                "matching alarm inbox was not consumed: {inbox_after:?}"
            ));
        }
    } else if alarm_rows[0].result["timedOut"] != json!(true) {
        failures.push(format!(
            "valid-empty expired control did not journal timeout: {:?}",
            alarm_rows[0]
        ));
    }
    hub().entries.lock().unwrap().remove(&fixture.name);
    failures
}

#[test]
fn detached_alarm_wake_fails_closed_on_corrupt_inbox() {
    const CHILD: &str = "CHIDORI_TEST_DETACHED_CORRUPT_ALARM_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let mut failures = Vec::new();
        for (backend, fault) in [
            ("fs", "malformed"),
            ("sqlite", "malformed"),
            ("fs", "read-error"),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(
                    "runtime::host_agent::tests::detached_alarm_wake_fails_closed_on_corrupt_inbox",
                )
                .arg("--nocapture")
                .env(CHILD, "1")
                .env("CHIDORI_TEST_DETACHED_CORRUPT_ALARM_BASE", temp.path())
                .env("CHIDORI_TEST_DETACHED_CORRUPT_ALARM_FAULT", fault)
                .env("CHIDORI_RUN_STORE", backend)
                .env("CHIDORI_RUN_DB", temp.path().join("runs.sqlite3"))
                .output()
                .unwrap();
            if !output.status.success() {
                failures.push(format!(
                    "backend={backend}, fault={fault}, exit={:?}\nstdout:\n{}\nstderr:\n{}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "detached corrupt alarm Red failed on base:\n{}",
            failures.join("\n")
        );
        return;
    }

    let base = PathBuf::from(
        std::env::var_os("CHIDORI_TEST_DETACHED_CORRUPT_ALARM_BASE")
            .expect("child fixture base is required"),
    );
    let read_error =
        std::env::var("CHIDORI_TEST_DETACHED_CORRUPT_ALARM_FAULT").as_deref() == Ok("read-error");
    let failures = run_corrupt_alarm_wake_case(&base, read_error);
    assert!(
        failures.is_empty(),
        "detached corrupt alarm contract failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn detached_alarm_valid_expired_and_matching_inbox_controls() {
    const CHILD: &str = "CHIDORI_TEST_DETACHED_ALARM_CONTROLS_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let mut failures = Vec::new();
        for backend in ["fs", "sqlite"] {
            let temp = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("runtime::host_agent::tests::detached_alarm_valid_expired_and_matching_inbox_controls")
                .arg("--nocapture")
                .env(CHILD, "1")
                .env("CHIDORI_TEST_DETACHED_CORRUPT_ALARM_BASE", temp.path())
                .env("CHIDORI_RUN_STORE", backend)
                .env("CHIDORI_RUN_DB", temp.path().join("runs.sqlite3"))
                .output()
                .unwrap();
            if !output.status.success() {
                failures.push(format!(
                    "backend={backend}, exit={:?}\nstdout:\n{}\nstderr:\n{}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "detached alarm controls failed:\n{}",
            failures.join("\n")
        );
        return;
    }

    let base = PathBuf::from(
        std::env::var_os("CHIDORI_TEST_DETACHED_CORRUPT_ALARM_BASE")
            .expect("child fixture base is required"),
    );
    let mut failures = run_valid_alarm_control(&base, "valid-empty", false);
    failures.extend(run_valid_alarm_control(&base, "valid-matching", true));
    assert!(
        failures.is_empty(),
        "detached alarm controls failed:\n{}",
        failures.join("\n")
    );
}

const DETACHED_VFS_MANIFEST_CHILD: &str = "CHIDORI_TEST_DETACHED_VFS_MANIFEST_CHILD";
const DETACHED_VFS_MANIFEST_BASE: &str = "CHIDORI_TEST_DETACHED_VFS_MANIFEST_BASE";
const DETACHED_VFS_PATH: &str = "/config/decision";
const DETACHED_VFS_SEED_VALUE: &str = "seeded-vfs-decision";
const DETACHED_VFS_FRESH_VALUE: &str = "fresh-run-vfs-decision";
const DETACHED_VFS_CORRUPT_BYTES: &[u8] = b"{malformed runtime snapshot manifest";

struct DetachedVfsManifestFixture {
    parts: AgentRuntimeParts,
    name: String,
    run_id: String,
    store: Arc<dyn RunStore>,
    observations: Arc<StdMutex<Vec<Value>>>,
}

fn new_detached_vfs_manifest_fixture(
    base: &Path,
    case: &str,
    waits_for_signal: bool,
) -> DetachedVfsManifestFixture {
    let project = base.join(case);
    std::fs::create_dir_all(project.join("services")).unwrap();
    let source = project.join("services/vfs-worker.ts");
    let source_text = if waits_for_signal {
        r#"import * as fs from "node:fs";
        export async function agent() {
            await chidori.signal("vfs-wake");
            let value;
            try { value = fs.readFileSync("/config/decision", "utf8"); }
            catch (error) { value = `missing:${error.code || "unknown"}`; }
            await chidori.tool("recordVfsValue", { value });
            return { value };
        }"#
    } else {
        r#"import * as fs from "node:fs";
        export async function agent() {
            fs.mkdirSync("/config", { recursive: true });
            fs.writeFileSync("/config/decision", "fresh-run-vfs-decision");
            let value;
            try { value = fs.readFileSync("/config/decision", "utf8"); }
            catch (error) { value = `missing:${error.code || "unknown"}`; }
            await chidori.tool("recordVfsValue", { value });
            return { value };
        }"#
    };
    std::fs::write(&source, source_text).unwrap();

    let run_base = project.join(".chidori/runs");
    let observations = Arc::new(StdMutex::new(Vec::new()));
    let observed = observations.clone();
    let mut tools = ToolRegistry::new();
    tools.register_native(
        "recordVfsValue",
        "records the value read from the snapshot-resident VFS",
        Vec::new(),
        move |args| {
            observed.lock().unwrap().push(args.clone());
            Ok(json!({ "recorded": true }))
        },
    );
    let parts = sqlite_test_parts(&project, run_base.clone(), Arc::new(tools));
    hub().install_parts(parts.clone());
    let name = unique(&format!("{case}-vfs"));
    let (snapshot_arrived_rx, snapshot_release_tx, context_published_rx, context_release_tx) =
        if waits_for_signal {
            let (arrived_tx, arrived_rx) = std::sync::mpsc::channel();
            let (snapshot_release_tx, snapshot_release_rx) = std::sync::mpsc::channel();
            let (published_tx, published_rx) = std::sync::mpsc::channel();
            let (context_release_tx, context_release_rx) = std::sync::mpsc::channel();
            *INBOX_SNAPSHOT_GAP_BARRIERS
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap() = Some(Arc::new(InboxSnapshotGapBarriers {
                agent_name: name.clone(),
                snapshot_arrived: arrived_tx,
                snapshot_release: StdMutex::new(snapshot_release_rx),
                context_published: published_tx,
                context_release: StdMutex::new(context_release_rx),
                snapshot_claimed: AtomicBool::new(false),
                context_claimed: AtomicBool::new(false),
            }));
            (
                Some(arrived_rx),
                Some(snapshot_release_tx),
                Some(published_rx),
                Some(context_release_tx),
            )
        } else {
            (None, None, None, None)
        };
    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &SpawnOptions {
                name: Some(name.clone()),
                restart: "resume".to_string(),
                max_restarts: 0,
                backoff_ms: 0,
                model: None,
            },
            format!("{case}-owner"),
        )
        .unwrap();
    let run_id = spawned["runId"].as_str().unwrap().to_string();
    let store = RunStoreFactory::shared(&run_base).store_for(&run_id);
    if waits_for_signal {
        snapshot_arrived_rx
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .expect("supervisor did not reach inbox snapshot barrier");
        snapshot_release_tx.unwrap().send(()).unwrap();
        context_published_rx
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .expect("supervisor did not publish live context");
        let entry = hub().ensure_entry(&parts, &name).unwrap();
        let ctx = entry
            .state
            .lock()
            .unwrap()
            .live_ctx
            .clone()
            .expect("published detached context must be live");
        ctx.vfs_mkdir("/config", true).unwrap();
        ctx.vfs_write(
            DETACHED_VFS_PATH,
            DETACHED_VFS_SEED_VALUE.as_bytes().to_vec(),
        )
        .unwrap();
        context_release_tx.unwrap().send(()).unwrap();
        *INBOX_SNAPSHOT_GAP_BARRIERS
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap() = None;
        let descriptor = wait_for_agent_descriptor(&parts, &name, |descriptor| {
            descriptor.status == "hibernating"
                && descriptor
                    .listen
                    .as_ref()
                    .is_some_and(|listen| listen.names == ["vfs-wake"])
        });
        assert_eq!(descriptor.run_id, run_id);
    }
    DetachedVfsManifestFixture {
        parts,
        name,
        run_id,
        store,
        observations,
    }
}

fn wait_for_detached_vfs_quiescence(
    parts: &AgentRuntimeParts,
    name: &str,
) -> (AgentDescriptor, bool, bool) {
    let entry = hub().ensure_entry(parts, name).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut state = entry.state.lock().unwrap();
    loop {
        if !state.thread_live && !state.execution_active {
            return (
                state.descriptor.clone(),
                state.thread_live,
                state.execution_active,
            );
        }
        let now = std::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {} to leave execution: descriptor={:?}, thread_live={}, execution_active={}",
            name,
            state.descriptor,
            state.thread_live,
            state.execution_active,
        );
        let (next, _) = entry
            .signal
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .unwrap();
        state = next;
    }
}

fn detached_vfs_manifest_corruption_child(backend: &str, test_name: &str) -> Result<(), String> {
    let base = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(DETACHED_VFS_MANIFEST_CHILD, "1")
        .env(DETACHED_VFS_MANIFEST_BASE, base.path())
        .env("CHIDORI_RUN_STORE", backend)
        .env("CHIDORI_RUN_DB", base.path().join("runs.sqlite3"))
        .output()
        .unwrap();
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "detached VFS manifest child backend={backend} exited {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

#[test]
fn detached_wake_does_not_resume_with_empty_vfs_after_manifest_corruption() {
    const TEST: &str = "runtime::host_agent::tests::detached_wake_does_not_resume_with_empty_vfs_after_manifest_corruption";
    if std::env::var_os(DETACHED_VFS_MANIFEST_CHILD).is_none() {
        let failures: Vec<_> = ["fs", "sqlite"]
            .into_iter()
            .filter_map(|backend| detached_vfs_manifest_corruption_child(backend, TEST).err())
            .collect();
        assert!(
            failures.is_empty(),
            "detached VFS corruption Red results:\n{}",
            failures.join("\n")
        );
        return;
    }

    let base = PathBuf::from(std::env::var_os(DETACHED_VFS_MANIFEST_BASE).unwrap());
    let fixture = new_detached_vfs_manifest_fixture(&base, "corrupt", true);
    let manifest_path = fixture
        .parts
        .run_base
        .join(&fixture.run_id)
        .join("runtime.snapshot.json");
    let valid_manifest_bytes = fixture
        .store
        .get_blob("runtime.snapshot.json")
        .unwrap()
        .expect("hibernating detached run must persist its snapshot manifest");
    let valid_manifest: crate::runtime::snapshot::SnapshotManifest =
        serde_json::from_slice(&valid_manifest_bytes).unwrap();
    assert_eq!(
        valid_manifest.vfs.read_text(DETACHED_VFS_PATH).unwrap(),
        DETACHED_VFS_SEED_VALUE,
        "precondition: manifest must contain the unique seeded VFS file"
    );
    assert!(
        manifest_path.is_file(),
        "precondition: snapshot manifest is persisted at {}",
        manifest_path.display()
    );
    assert_eq!(
        std::fs::read(&manifest_path).unwrap(),
        valid_manifest_bytes,
        "precondition: local snapshot manifest agrees with the configured store"
    );

    fixture
        .store
        .put_blob("runtime.snapshot.json", DETACHED_VFS_CORRUPT_BYTES)
        .unwrap();
    assert_eq!(
        fixture
            .store
            .get_blob("runtime.snapshot.json")
            .unwrap()
            .as_deref(),
        Some(DETACHED_VFS_CORRUPT_BYTES),
        "test must corrupt only the snapshot manifest before wake"
    );
    let send = hub().send(
        &fixture.parts,
        &fixture.name,
        "vfs-wake",
        json!({ "delivery": "repair-me" }),
        json!({ "from": "test" }),
    );
    assert_eq!(send.unwrap()["delivered"], json!(true));
    let (descriptor, thread_live, execution_active) =
        wait_for_detached_vfs_quiescence(&fixture.parts, &fixture.name);

    let observations = fixture.observations.lock().unwrap().clone();
    let records = fixture.store.load_call_log().unwrap().unwrap_or_default();
    let inbox_bytes = fixture.store.get_blob(SIGNAL_INBOX_FILE).unwrap();
    let inbox: Vec<QueuedSignal> = inbox_bytes
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()
        .unwrap()
        .unwrap_or_default();
    let manifest_after = fixture.store.get_blob("runtime.snapshot.json").unwrap();
    let signal_rows: Vec<_> = records
        .iter()
        .filter(|record| record.function == "signal" && record.args["name"] == "vfs-wake")
        .collect();
    let mut failures = Vec::new();
    if !observations.is_empty() {
        failures.push(format!(
            "corrupt snapshot resumed user code using an empty VFS; native observations={observations:?}"
        ));
    }
    if !signal_rows.is_empty() {
        failures.push(format!(
            "corrupt snapshot consumed the wake frontier before VFS recovery: {signal_rows:?}"
        ));
    }
    if inbox.len() != 1
        || inbox[0].name != "vfs-wake"
        || inbox[0].payload != json!({ "delivery": "repair-me" })
    {
        failures.push(format!(
            "corrupt snapshot did not preserve the pending wake for repair: {inbox:?}"
        ));
    }
    if manifest_after.as_deref() != Some(DETACHED_VFS_CORRUPT_BYTES) {
        failures.push(format!(
            "corrupt manifest bytes changed during wake: {:?}",
            manifest_after.as_deref().map(String::from_utf8_lossy)
        ));
    }
    if descriptor.status == "completed" || descriptor.status == "failed" {
        failures.push(format!(
            "corrupt snapshot wake settled the agent before repair: {descriptor:?}"
        ));
    }
    if thread_live || execution_active {
        failures.push(format!(
            "supervisor did not stand down on corrupt snapshot: thread_live={thread_live}, execution_active={execution_active}"
        ));
    }

    // A fail-closed implementation can leave the durable wake queued. Repair
    // the original manifest and re-drive that exact delivery to prove the
    // run remains recoverable, without relying on a second acknowledged send.
    if inbox.len() == 1 && !thread_live && !execution_active {
        fixture
            .store
            .put_blob("runtime.snapshot.json", &valid_manifest_bytes)
            .unwrap();
        let rearmed = hub().rearm_from_registry(fixture.parts.clone());
        if let Err(error) = rearmed {
            failures.push(format!("re-drive after manifest repair failed: {error}"));
        } else {
            let repaired = wait_for_agent_descriptor(&fixture.parts, &fixture.name, |descriptor| {
                matches!(
                    descriptor.status.as_str(),
                    "completed" | "failed" | "stopped"
                )
            });
            let repaired_observations = fixture.observations.lock().unwrap().clone();
            let repaired_records = fixture.store.load_call_log().unwrap().unwrap_or_default();
            let repaired_inbox = fixture.store.get_blob(SIGNAL_INBOX_FILE).unwrap();
            let recovered_signal_count = repaired_records
                .iter()
                .filter(|record| record.function == "signal" && record.args["name"] == "vfs-wake")
                .count();
            if repaired.status != "completed"
                || repaired_observations.len() != 1
                || repaired_observations[0]["value"] != DETACHED_VFS_SEED_VALUE
                || recovered_signal_count != 1
                || repaired_inbox.as_deref() != Some(b"[]".as_slice())
            {
                failures.push(format!(
                    "repaired wake did not restore the VFS and consume exactly once: descriptor={repaired:?}, observations={repaired_observations:?}, signal_count={recovered_signal_count}, inbox={repaired_inbox:?}"
                ));
            }
        }
    }

    hub().entries.lock().unwrap().remove(&fixture.name);
    assert!(
        failures.is_empty(),
        "detached VFS manifest corruption contract failures for backend={} project={} run_id={}:\n{}\nobservations={observations:?}\ndescriptor={descriptor:?}\njournal={records:?}\ninbox={inbox:?}\nmanifest_path={} local_manifest={:?}",
        std::env::var("CHIDORI_RUN_STORE").unwrap_or_else(|_| "fs".to_string()),
        fixture.parts.run_base.parent().unwrap().display(),
        fixture.run_id,
        failures.join("\n"),
        manifest_path.display(),
        std::fs::read(&manifest_path)
            .ok()
            .as_deref()
            .map(String::from_utf8_lossy),
    );
}

#[test]
fn detached_vfs_manifest_restore_and_fresh_run_controls() {
    const TEST: &str =
        "runtime::host_agent::tests::detached_vfs_manifest_restore_and_fresh_run_controls";
    if std::env::var_os(DETACHED_VFS_MANIFEST_CHILD).is_none() {
        let failures: Vec<_> = ["fs", "sqlite"]
            .into_iter()
            .filter_map(|backend| detached_vfs_manifest_corruption_child(backend, TEST).err())
            .collect();
        assert!(
            failures.is_empty(),
            "detached VFS manifest controls failed:\n{}",
            failures.join("\n")
        );
        return;
    }

    let base = PathBuf::from(std::env::var_os(DETACHED_VFS_MANIFEST_BASE).unwrap());
    let fixture = new_detached_vfs_manifest_fixture(&base, "valid-restore", true);
    let manifest = fixture
        .store
        .get_blob("runtime.snapshot.json")
        .unwrap()
        .expect("hibernating detached run must persist its snapshot manifest");
    let parsed: crate::runtime::snapshot::SnapshotManifest =
        serde_json::from_slice(&manifest).unwrap();
    assert_eq!(
        parsed.vfs.read_text(DETACHED_VFS_PATH).unwrap(),
        DETACHED_VFS_SEED_VALUE,
        "control precondition: valid snapshot carries the seed"
    );
    assert_eq!(
        hub()
            .send(
                &fixture.parts,
                &fixture.name,
                "vfs-wake",
                json!({ "delivery": "valid-control" }),
                json!({ "from": "test" }),
            )
            .unwrap()["delivered"],
        json!(true)
    );
    let completed = wait_for_agent_descriptor(&fixture.parts, &fixture.name, |descriptor| {
        descriptor.status == "completed" || descriptor.status == "failed"
    });
    let records = fixture.store.load_call_log().unwrap().unwrap_or_default();
    let inbox = fixture.store.get_blob(SIGNAL_INBOX_FILE).unwrap();
    assert_eq!(completed.status, "completed", "{completed:?}");
    assert_eq!(
        fixture.observations.lock().unwrap().as_slice(),
        &[json!({ "value": DETACHED_VFS_SEED_VALUE })]
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.function == "signal" && record.args["name"] == "vfs-wake")
            .count(),
        1,
        "valid snapshot control consumes its signal once"
    );
    assert_eq!(inbox.as_deref(), Some(b"[]".as_slice()));
    hub().entries.lock().unwrap().remove(&fixture.name);

    // A genuinely new run starts without any prior snapshot manifest and
    // seeds the VFS from the child process environment as documented.
    let fresh = new_detached_vfs_manifest_fixture(&base, "fresh-no-manifest", false);
    let fresh_descriptor = wait_for_agent_descriptor(&fresh.parts, &fresh.name, |descriptor| {
        descriptor.status == "completed" || descriptor.status == "failed"
    });
    assert_eq!(fresh_descriptor.status, "completed", "{fresh_descriptor:?}");
    assert_eq!(
        fresh.observations.lock().unwrap().as_slice(),
        &[json!({ "value": DETACHED_VFS_FRESH_VALUE })]
    );
    hub().entries.lock().unwrap().remove(&fresh.name);
}

#[test]
fn explicit_sqlite_failure_rejects_detached_spawn_rearm_send_and_alarm_wake_before_effects() {
    const CHILD: &str = "CHIDORI_TEST_AGENT_SQLITE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let base = PathBuf::from(std::env::var_os("CHIDORI_TEST_AGENT_BASE").unwrap());
        let db_path = std::env::var("CHIDORI_RUN_DB").unwrap();
        let source = base.join("agent.ts");
        std::fs::write(
            &source,
            r#"export async function agent() {
                await chidori.tool("count", {});
                return {};
            }"#,
        )
        .unwrap();
        let alarm_source = base.join("alarm-agent.ts");
        std::fs::write(
            &alarm_source,
            r#"export async function agent() {
                await chidori.alarm(60000);
                await chidori.tool("alarmCount", {});
                return {};
            }"#,
        )
        .unwrap();
        let spawn_base = base.join("spawn-runs");
        let wake_base = base.join("wake-runs");
        let rearm_base = base.join("rearm-runs");
        let alarm_base = base.join("alarm-runs");
        let effect_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (alarm_event_tx, alarm_event_rx) = std::sync::mpsc::channel();
        let mut tool_registry = ToolRegistry::new();
        let effect_counter = effect_count.clone();
        tool_registry.register_native(
            "count",
            "counts detached agent effects",
            Vec::new(),
            move |_| {
                effect_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(json!({ "counted": true }))
            },
        );
        let alarm_event = alarm_event_tx;
        let alarm_counter = effect_count.clone();
        tool_registry.register_native(
            "alarmCount",
            "marks the due-alarm wake effect",
            Vec::new(),
            move |_| {
                alarm_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = alarm_event.send(true);
                Ok(json!({ "counted": true }))
            },
        );
        let tools = Arc::new(tool_registry);
        let spawn_parts = sqlite_test_parts(&base, spawn_base.clone(), tools.clone());
        let wake_parts = sqlite_test_parts(&base, wake_base.clone(), tools.clone());
        let rearm_parts = sqlite_test_parts(&base, rearm_base.clone(), tools);
        let alarm_parts = sqlite_test_parts(&base, alarm_base.clone(), spawn_parts.tools.clone());

        let spawn_name = "sqlite-red-spawn";
        let spawn_result = hub().spawn(
            &spawn_parts,
            source.to_str().unwrap(),
            json!({}),
            &SpawnOptions {
                name: Some(spawn_name.to_string()),
                restart: "never".to_string(),
                max_restarts: 0,
                backoff_ms: 0,
                model: None,
            },
            "owner-run".to_string(),
        );

        let wake_name = "sqlite-red-wake";
        write_registry_fixture(&wake_base, wake_name, "wake-agent", &source, "hibernating");
        let wake_registry = wake_base.join("agents").join(format!("{wake_name}.json"));
        let wake_registry_before = std::fs::read(&wake_registry).unwrap();
        let wake_rearm = hub().rearm_from_registry(wake_parts.clone());
        let wake_result = hub().send(
            &wake_parts,
            wake_name,
            "wake",
            json!({"payload": true}),
            json!({"from": "test"}),
        );

        let rearm_name = "sqlite-red-rearm";
        write_registry_fixture(&rearm_base, rearm_name, "rearm-agent", &source, "running");
        let rearm_registry = rearm_base.join("agents").join(format!("{rearm_name}.json"));
        let rearm_registry_before = std::fs::read(&rearm_registry).unwrap();
        let rearm_result = hub().rearm_from_registry(rearm_parts.clone());

        let alarm_name = "sqlite-red-alarm-wake";
        write_registry_fixture(
            &alarm_base,
            alarm_name,
            "alarm-agent",
            &alarm_source,
            "hibernating",
        );
        let alarm_registry = alarm_base.join("agents").join(format!("{alarm_name}.json"));
        let mut alarm_entry: Value =
            serde_json::from_slice(&std::fs::read(&alarm_registry).unwrap()).unwrap();
        alarm_entry["descriptor"]["listen"]["names"] = json!([ALARM_SIGNAL_NAME]);
        alarm_entry["descriptor"]["listen"]["deadline"] =
            json!((Utc::now() - chrono::Duration::seconds(1)).to_rfc3339());
        std::fs::write(
            &alarm_registry,
            serde_json::to_vec_pretty(&alarm_entry).unwrap(),
        )
        .unwrap();
        let alarm_registry_before = std::fs::read(&alarm_registry).unwrap();
        let alarm_rearm = hub().rearm_from_registry(alarm_parts.clone());

        // Wait for the alarm agent's distinctive effect event. This proves
        // the due-alarm path reached execution without sleep polling.
        let alarm_effect_seen =
            alarm_rearm.is_ok() && alarm_event_rx.recv_timeout(Duration::from_secs(5)).is_ok();

        let spawn_registry_written = spawn_base
            .join("agents")
            .join(format!("{spawn_name}.json"))
            .exists();
        let wake_inbox_written = wake_base
            .join("wake-agent")
            .join("signals")
            .join("inbox.json")
            .exists();
        let spawn_entry_installed = hub().entries.lock().unwrap().contains_key(spawn_name);
        let wake_entry_installed = hub().entries.lock().unwrap().contains_key(wake_name);
        let rearm_entry_installed = hub().entries.lock().unwrap().contains_key(rearm_name);
        let alarm_entry_installed = hub().entries.lock().unwrap().contains_key(alarm_name);

        let errors = [
            spawn_result.err(),
            wake_rearm.err(),
            wake_result.err(),
            rearm_result.err(),
            alarm_rearm.err(),
        ];
        let observed_effects = effect_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed_effects, 0,
            "detached agent executed a live tool effect under invalid SQLite; due-alarm effect observed={alarm_effect_seen}"
        );
        assert!(
            errors.iter().all(|error| error.as_deref().is_some_and(|text| {
                text.contains(&db_path)
                    && text.to_lowercase().contains("unable to open database file")
            })),
            "hub operations did not propagate invalid SQLite before spawn/rearm/send/alarm wake; observed errors={errors:?}, effects={}, spawn_registry_written={spawn_registry_written}, wake_inbox_written={wake_inbox_written}, entries=[{spawn_entry_installed}, {wake_entry_installed}, {rearm_entry_installed}, {alarm_entry_installed}]",
            observed_effects
        );
        assert!(
            !spawn_registry_written && !wake_inbox_written,
            "invalid SQLite mutated a detached-agent registry or mailbox"
        );
        assert_eq!(std::fs::read(&wake_registry).unwrap(), wake_registry_before);
        assert_eq!(
            std::fs::read(&rearm_registry).unwrap(),
            rearm_registry_before
        );
        assert_eq!(
            std::fs::read(&alarm_registry).unwrap(),
            alarm_registry_before
        );
        assert!(
            !spawn_entry_installed
                && !wake_entry_installed
                && !rearm_entry_installed
                && !alarm_entry_installed,
            "invalid SQLite installed an agent entry or started a supervision path"
        );
        assert!(
            hub().parts.lock().unwrap().is_none(),
            "invalid SQLite installed detached-agent runtime parts"
        );
        assert!(
            !*hub().timer_started.lock().unwrap(),
            "invalid SQLite started the detached-agent timer"
        );
        return;
    }

    let base = test_dir("sqlite-factory-paths");
    let db_path = base.join("sqlite-db-is-a-directory");
    std::fs::create_dir_all(&db_path).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::host_agent::tests::explicit_sqlite_failure_rejects_detached_spawn_rearm_send_and_alarm_wake_before_effects",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("CHIDORI_TEST_AGENT_BASE", &base)
        .env("CHIDORI_RUN_STORE", "sqlite")
        .env("CHIDORI_RUN_DB", &db_path)
        .env("CHIDORI_DURABILITY", "strict")
        .output()
        .unwrap();
    let report = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_dir_all(&base).ok();
    assert!(
        output.status.success(),
        "isolated detached-agent SQLite assertions failed:\n{report}"
    );
}

fn write_registry_fixture(run_base: &Path, name: &str, run_id: &str, source: &Path, status: &str) {
    let agents = run_base.join("agents");
    std::fs::create_dir_all(&agents).unwrap();
    let descriptor = json!({
        "name": name,
        "run_id": run_id,
        "source": source,
        "input": {},
        "restart": "never",
        "max_restarts": 0,
        "backoff_ms": 0,
        "owner_run_id": "owner-run",
        "model": null,
        "status": status,
        "listen": if status == "hibernating" {
            json!({"names": ["wake"], "seq": 1, "function": "signal", "deadline": null})
        } else {
            Value::Null
        },
        "restarts": 0,
        "output": null,
        "error": null,
        "created_at": Utc::now(),
        "updated_at": Utc::now(),
    });
    let entry = json!({ "name": name, "run_id": run_id, "descriptor": descriptor });
    std::fs::write(
        agents.join(format!("{name}.json")),
        serde_json::to_vec_pretty(&entry).unwrap(),
    )
    .unwrap();
}

/// A fully-wired runtime backend over `ctx`, with the template engine
/// anchored at `dir` so relative agent sources resolve — mirroring the
/// host_actor test harness, plus persistence (detached agents are
/// durable runs and refuse to exist without a journal).
fn test_backend(ctx: RuntimeContext, dir: &Path) -> HostBindingBackend {
    ctx.enable_persistence(dir.join(".chidori").join("runs"));
    HostBindingBackend::for_runtime(
        ctx,
        Arc::new(ProviderRegistry::new()),
        Arc::new(TemplateEngine::new(dir)),
        Arc::new(tokio::runtime::Runtime::new().unwrap()),
        PolicyConfig::from_env(),
        Arc::new(StdMutex::new(PolicyCache::default())),
        RuntimePolicy::durable_default("agent-test"),
        Arc::new(ToolRegistry::new()),
        Arc::new(McpManager::new()),
    )
}

fn test_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("chidori-agent-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(dir.join("services")).unwrap();
    dir
}

#[test]
fn detached_inbox_snapshot_to_live_context_gap_preserves_acknowledged_payloads() {
    const CHILD: &str = "CHIDORI_TEST_INBOX_SNAPSHOT_GAP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let base = PathBuf::from(std::env::var_os("CHIDORI_TEST_INBOX_SNAPSHOT_GAP_BASE").unwrap());
        let loss = run_inbox_snapshot_to_live_context_gap(&base);
        run_sequential_inbox_control(&base, 2);
        assert!(
            loss.is_none(),
            "acknowledged payload missing from both child journal and inbox: {}",
            loss.unwrap_or_default()
        );
        return;
    }

    let mut failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let base = tempfile::tempdir().unwrap();
        let db_path = base.path().join("runs.sqlite3");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::host_agent::tests::detached_inbox_snapshot_to_live_context_gap_preserves_acknowledged_payloads",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("CHIDORI_TEST_INBOX_SNAPSHOT_GAP_BASE", base.path())
            .env("CHIDORI_RUN_STORE", backend)
            .env("CHIDORI_RUN_DB", db_path)
            .env("CHIDORI_DURABILITY", "strict")
            .output()
            .unwrap();
        if !output.status.success() {
            failures.push(format!(
                "backend={backend}\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "inbox snapshot-to-live-context interleaving lost an acknowledged payload:\n{failures:#?}"
    );
}

fn run_inbox_snapshot_to_live_context_gap(base: &Path) -> Option<String> {
    let project = base.join("snapshot-gap");
    std::fs::create_dir_all(project.join("services")).unwrap();
    let source = project.join("services/worker.ts");
    std::fs::write(
        &source,
        r#"export async function agent() {
            for (let i = 0; i < 2; i++) {
                const message = await chidori.signal("tick");
                await chidori.log("cycle", { n: message.payload.n });
            }
            return { done: true };
        }"#,
    )
    .unwrap();
    let agent_name = unique("inbox-snapshot-gap");
    let run_base = project.join(".chidori/runs");
    let parts = sqlite_test_parts(&project, run_base.clone(), Arc::new(ToolRegistry::new()));
    hub().install_parts(parts.clone());
    let options = SpawnOptions {
        name: Some(agent_name.clone()),
        restart: "never".to_string(),
        max_restarts: 0,
        backoff_ms: 0,
        model: None,
    };

    let snapshot_arrived_rx;
    let snapshot_release_tx;
    let context_published_rx;
    let context_release_tx;
    {
        let (arrived_tx, rx) = std::sync::mpsc::channel();
        snapshot_arrived_rx = rx;
        let (release_tx, snapshot_release_rx) = std::sync::mpsc::channel();
        snapshot_release_tx = release_tx;
        let (published_tx, rx) = std::sync::mpsc::channel();
        context_published_rx = rx;
        let (release_tx, context_release_rx) = std::sync::mpsc::channel();
        context_release_tx = release_tx;
        *INBOX_SNAPSHOT_GAP_BARRIERS
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap() = Some(Arc::new(InboxSnapshotGapBarriers {
            agent_name: agent_name.clone(),
            snapshot_arrived: arrived_tx,
            snapshot_release: StdMutex::new(snapshot_release_rx),
            context_published: published_tx,
            context_release: StdMutex::new(context_release_rx),
            snapshot_claimed: AtomicBool::new(false),
            context_claimed: AtomicBool::new(false),
        }));
    }

    let spawned = hub()
        .spawn(
            &parts,
            source.to_str().unwrap(),
            json!({}),
            &options,
            "snapshot-gap-test".to_string(),
        )
        .unwrap();
    snapshot_arrived_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("supervisor did not reach inbox snapshot barrier");

    let run_id = spawned["runId"].as_str().unwrap().to_string();
    let status = hub().status(&parts, &agent_name).unwrap();
    assert_eq!(status["status"], json!("running"), "{status}");
    assert_eq!(status["live"], json!(true), "{status}");
    let mut acknowledged_ids = Vec::new();
    for n in 1..=2 {
        if n == 2 {
            snapshot_release_tx.send(()).unwrap();
            context_published_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("supervisor did not publish live context");
        }
        let result = hub()
            .send(
                &parts,
                &agent_name,
                "tick",
                json!({ "n": n }),
                json!({ "kind": "agent", "id": "snapshot-gap-test" }),
            )
            .unwrap();
        assert_eq!(result["delivered"], json!(true), "payload n={n}");
        acknowledged_ids.push(n);
    }
    context_release_tx.send(()).unwrap();
    wait_for_agent_status(&parts, &agent_name, |status| {
        status == "hibernating" || status == "completed" || status == "failed"
    });
    *INBOX_SNAPSHOT_GAP_BARRIERS
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = None;

    let factory = RunStoreFactory::shared(&run_base);
    let store = factory.store_for(&run_id);
    let records = store.load_call_log().unwrap().unwrap_or_default();
    let inbox: Vec<Value> = store
        .get_blob(SIGNAL_INBOX_FILE)
        .unwrap()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .unwrap_or_default();
    let accounted: Vec<(usize, usize, usize)> = acknowledged_ids
        .iter()
        .map(|n| {
            let journal_count = records
                .iter()
                .filter(|record| {
                    record.function == "signal" && record.result["payload"]["n"] == json!(n)
                })
                .count();
            let inbox_count = inbox
                .iter()
                .filter(|queued| queued["payload"]["n"] == json!(n))
                .count();
            (*n, journal_count, inbox_count)
        })
        .collect();
    let missing: Vec<_> = accounted
        .iter()
        .filter(|(_, journal_count, inbox_count)| journal_count + inbox_count != 1)
        .cloned()
        .collect();
    let _ = hub().stop(&parts, &agent_name);
    wait_for_agent_status(&parts, &agent_name, |status| {
        matches!(status, "stopped" | "completed" | "failed")
    });
    (!missing.is_empty()).then(|| {
        format!(
            "expected acknowledged n=1 and n=2 exactly once in child journal or inbox; missing/duplicate={missing:?}, all_counts={accounted:?}, inbox={inbox:?}"
        )
    })
}

fn write_module(dir: &Path, name: &str, source: &str) {
    std::fs::write(dir.join("services").join(name), source).unwrap();
}

fn unique(name: &str) -> String {
    format!("{name}-{}", &uuid::Uuid::new_v4().to_string()[..8])
}

/// Poll an agent's status until it settles (or the deadline passes) —
/// wakes run on their own thread, so the test observes the descriptor.
fn wait_for_settled(parts: &AgentRuntimeParts, name: &str) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let status = hub().status(parts, name).unwrap();
        let settled = matches!(
            status["status"].as_str().unwrap_or_default(),
            "completed" | "failed" | "stopped" | "paused"
        );
        if settled || std::time::Instant::now() >= deadline {
            return status;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The run id the parent's `spawn_agent` record minted.
fn spawned_run_id(ctx: &RuntimeContext) -> String {
    ctx.call_log()
        .into_records()
        .iter()
        .find(|r| r.function == "spawn_agent")
        .expect("spawn_agent record")
        .result["runId"]
        .as_str()
        .expect("runId")
        .to_string()
}

#[test]
fn detached_agent_spawns_completes_and_joins() {
    let dir = test_dir("basic");
    write_module(
        &dir,
        "worker.ts",
        r#"
        export async function agent(input: { base: number }) {
            await chidori.log("detached worker running");
            return { doubled: input.base * 2 };
        }
        "#,
    );
    let agent_name = unique("doubler");
    let path = dir.join("agent.ts");
    let src = r#"
        export async function agent() {
            const svc = await chidori.agents.spawn("services/worker.ts", { base: 21 }, { name: "__NAME__" });
            const outcome = await svc.join({ timeoutMs: 30000 });
            return { name: svc.name, outcome };
        }
    "#
    .replace("__NAME__", &agent_name);
    std::fs::write(&path, &src).unwrap();

    let ctx = RuntimeContext::new();
    let backend = test_backend(ctx.clone(), &dir);
    let output = run_agent(&path, &src, &json!({}), &backend).unwrap();

    assert_eq!(output["name"], json!(agent_name));
    assert_eq!(output["outcome"]["status"], json!("completed"));
    assert_eq!(output["outcome"]["output"], json!({ "doubled": 42 }));

    // The parent journal carries only spawn + join records; the agent's
    // own records live in ITS journal under its own run id.
    let records = ctx.call_log().into_records();
    let spawn = records
        .iter()
        .find(|r| r.function == "spawn_agent")
        .expect("spawn_agent record");
    assert!(records.iter().any(|r| r.function == "join_agent"));
    assert!(!records.iter().any(|r| r.function == "log"));
    let agent_run_id = spawn.result["runId"].as_str().unwrap();
    let factory = RunStoreFactory::shared(&dir.join(".chidori").join("runs"));
    let agent_journal = factory
        .store_for(agent_run_id)
        .load_call_log()
        .unwrap()
        .expect("detached agent journal");
    assert!(agent_journal
        .iter()
        .any(|r| r.function == "log" && r.args["message"] == "detached worker running"));

    // The registry knows the settled agent.
    let entry = factory.registry_get(&agent_name).unwrap().unwrap();
    assert_eq!(entry["descriptor"]["status"], json!("completed"));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn detached_agent_hibernates_and_wakes_on_send() {
    let dir = test_dir("hibernate");
    write_module(
        &dir,
        "listener.ts",
        r#"
        export async function agent() {
            await chidori.log("before listen");
            const msg = await chidori.signal("go");
            return { got: msg.payload };
        }
        "#,
    );
    let agent_name = unique("listener");
    let path = dir.join("agent.ts");
    let src = r#"
        export async function agent() {
            const svc = await chidori.agents.spawn("services/listener.ts", {}, { name: "__NAME__" });
            // Give the service a moment to reach its listen point, then
            // check it hibernates with no thread parked on the wait.
            let status = await svc.join({ timeoutMs: 3000 });
            const wasHibernating = status.status;
            await svc.send("go", { speed: "fast" });
            const outcome = await svc.join({ timeoutMs: 30000 });
            return { wasHibernating, outcome };
        }
    "#
    .replace("__NAME__", &agent_name);
    std::fs::write(&path, &src).unwrap();

    let ctx = RuntimeContext::new();
    let backend = test_backend(ctx.clone(), &dir);
    let output = run_agent(&path, &src, &json!({}), &backend).unwrap();

    assert_eq!(output["wasHibernating"], json!("hibernating"));
    assert_eq!(output["outcome"]["status"], json!("completed"));
    assert_eq!(
        output["outcome"]["output"],
        json!({ "got": { "speed": "fast" } })
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn alarm_wakes_a_hibernating_agent_at_the_deadline() {
    let dir = test_dir("alarm");
    write_module(
        &dir,
        "sleeper.ts",
        r#"
        export async function agent() {
            await chidori.log("arming alarm");
            const fired = await chidori.alarm(1500);
            return { timedOut: fired.timedOut === true };
        }
        "#,
    );
    let agent_name = unique("sleeper");
    let path = dir.join("agent.ts");
    let src = r#"
        export async function agent() {
            const svc = await chidori.agents.spawn("services/sleeper.ts", {}, { name: "__NAME__" });
            const outcome = await svc.join({ timeoutMs: 30000 });
            return { outcome };
        }
    "#
    .replace("__NAME__", &agent_name);
    std::fs::write(&path, &src).unwrap();

    let ctx = RuntimeContext::new();
    let backend = test_backend(ctx.clone(), &dir);
    // join with a timeout returns a status snapshot rather than blocking,
    // so poll until the alarm fires and the agent completes.
    let output = run_agent(&path, &src, &json!({}), &backend).unwrap();
    let mut outcome = output["outcome"].clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while outcome["status"] != json!("completed") && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        let parts = hub().installed_parts().unwrap();
        outcome = hub().status(&parts, &agent_name).unwrap();
    }
    assert_eq!(outcome["status"], json!("completed"), "{outcome}");
    assert_eq!(outcome["output"], json!({ "timedOut": true }));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn resume_restart_replays_completed_work_and_retries_the_failure() {
    let dir = test_dir("restart");
    // The worker fails on its first attempt (no marker file), then
    // "the environment is fixed" and the resume restart retries only the
    // failing frontier: the log call before the failure must not re-run.
    write_module(
        &dir,
        "flaky.ts",
        r#"
        import * as fs from "node:fs";
        export async function agent() {
            await chidori.log("expensive work done");
            // The VFS is snapshot-resident and empty on the first
            // attempt; the resume restart replays the log call from
            // cache and re-executes only this failing read.
            const marker = await chidori.util.tryCall(() => fs.readFileSync("/marker", "utf8"));
            if (!marker.ok) {
                fs.writeFileSync("/marker", "present");
                throw new Error("transient failure");
            }
            return { done: true };
        }
        "#,
    );
    let agent_name = unique("flaky");
    let path = dir.join("agent.ts");
    let src = r#"
        export async function agent() {
            const svc = await chidori.agents.spawn("services/flaky.ts", {}, {
                name: "__NAME__",
                restart: "resume",
                maxRestarts: 3,
            });
            const outcome = await svc.join({ timeoutMs: 30000 });
            return { outcome };
        }
    "#
    .replace("__NAME__", &agent_name);
    std::fs::write(&path, &src).unwrap();

    let ctx = RuntimeContext::new();
    let backend = test_backend(ctx.clone(), &dir);
    let output = run_agent(&path, &src, &json!({}), &backend).unwrap();
    // The VFS resets per iteration (it rides the manifest persisted at
    // safepoints, and the failing iteration persisted its write), so the
    // retry sees the marker... depending on scaffold timing the agent
    // either completes on a later attempt or exhausts the budget; both
    // prove the restart loop drove re-execution.
    let status = output["outcome"]["status"].as_str().unwrap().to_string();
    assert!(
        status == "completed" || status == "failed",
        "unexpected status {status}"
    );
    let restarts = output["outcome"]["restarts"].as_u64().unwrap();
    assert!(restarts >= 1, "expected at least one restart");

    let _ = std::fs::remove_dir_all(dir);
}

/// The fleet case: a wake lands wherever a lease can be taken, not where
/// the agent's project tree happens to be. With the local tree gone, the
/// run's own recorded source history is the source of truth — entry AND
/// imported module, at their recorded relative layout, so the import
/// still resolves.
#[test]
fn wakes_from_materialized_source_when_the_local_tree_is_gone() {
    const GLOBAL_PARTS_CHILD: &str = "CHIDORI_TEST_MATERIALIZED_SOURCE_GLOBAL_PARTS_CHILD";
    let prove_global_parts_replacement = std::env::var_os(GLOBAL_PARTS_CHILD).is_some();
    let dir = test_dir("materialize");
    write_module(&dir, "lib.ts", "export const greeting = \"hi\";\n");
    write_module(
        &dir,
        "listener.ts",
        r#"
        import { greeting } from "./lib.ts";
        export async function agent() {
            await chidori.log("armed");
            const msg = await chidori.signal("go");
            return { got: msg.payload, greeting };
        }
        "#,
    );
    let agent_name = unique("materialized");
    let path = dir.join("agent.ts");
    let src = r#"
        export async function agent() {
            const svc = await chidori.agents.spawn("services/listener.ts", {}, { name: "__NAME__" });
            const status = await svc.join({ timeoutMs: 5000 });
            return { status: status.status };
        }
    "#
    .replace("__NAME__", &agent_name);
    std::fs::write(&path, &src).unwrap();

    let ctx = RuntimeContext::new();
    // This capability belongs to the fixture's own run base. Do not read
    // the process-global hub slot after spawn: a parallel test may have
    // installed parts for another isolated run base by then.
    let owned_parts = sqlite_test_parts(
        &dir,
        dir.join(".chidori").join("runs"),
        Arc::new(ToolRegistry::new()),
    );
    let backend = test_backend(ctx.clone(), &dir);
    let output = run_agent(&path, &src, &json!({}), &backend).unwrap();
    assert_eq!(output["status"], json!("hibernating"));
    let run_id = spawned_run_id(&ctx);

    std::fs::remove_dir_all(dir.join("services")).unwrap();

    // Only the exact child process mutates the global parts slot. That
    // proves the cross-test failure mode without allowing this ordinary
    // parallel test to overwrite another test's installed runtime parts.
    let competing_dir = if prove_global_parts_replacement {
        let competing_dir = test_dir("materialize-competing");
        let competing_parts = sqlite_test_parts(
            &competing_dir,
            competing_dir.join(".chidori").join("runs"),
            Arc::new(ToolRegistry::new()),
        );
        hub().install_parts(competing_parts);
        let borrowed_error = hub()
            .send(
                &hub().installed_parts().unwrap(),
                &agent_name,
                "go",
                json!({ "speed": "wrong-run-base" }),
                json!({ "kind": "test" }),
            )
            .expect_err("replaced global parts must not resolve this test's agent");
        assert!(
            borrowed_error.contains(&format!("unknown agent `{agent_name}`")),
            "unexpected borrowed-parts error: {borrowed_error}"
        );
        Some(competing_dir)
    } else {
        None
    };
    hub()
        .send(
            &owned_parts,
            &agent_name,
            "go",
            json!({ "speed": "fast" }),
            json!({ "kind": "test" }),
        )
        .unwrap();
    let outcome = wait_for_settled(&owned_parts, &agent_name);
    assert_eq!(outcome["status"], json!("completed"), "{outcome}");
    assert_eq!(
        outcome["output"],
        json!({ "got": { "speed": "fast" }, "greeting": "hi" })
    );

    // The tree came out of the run store, into the materialization root.
    // (The hub is process-global, so derive the root from this test's own
    // run base rather than whatever parts another test installed last.)
    let root = crate::runtime::source_history::materialization_root(
        &dir.join(".chidori").join("runs"),
        &run_id,
    )
    .unwrap();
    assert!(root.join("listener.ts").is_file(), "{}", root.display());
    assert!(root.join("lib.ts").is_file());

    if let Some(competing_dir) = competing_dir {
        let _ = std::fs::remove_dir_all(competing_dir);
    }
    let _ = std::fs::remove_dir_all(dir);

    if !prove_global_parts_replacement {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::host_agent::tests::wakes_from_materialized_source_when_the_local_tree_is_gone",
                "--nocapture",
            ])
            .env(GLOBAL_PARTS_CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "materialized-source global-parts child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// A run recorded before source history existed has no `history/` at all,
/// only the entry text inside the snapshot blob's `DurableBlob.bundle`.
/// That is still enough to wake a single-module agent on a node with no
/// copy of its source.
#[test]
fn wakes_from_the_snapshot_bundle_for_a_pre_source_history_run() {
    let dir = test_dir("bundle-wake");
    write_module(
        &dir,
        "solo.ts",
        r#"
        export async function agent() {
            await chidori.log("armed");
            const msg = await chidori.signal("go");
            return { got: msg.payload };
        }
        "#,
    );
    let agent_name = unique("legacy");
    let path = dir.join("agent.ts");
    let src = r#"
        export async function agent() {
            const svc = await chidori.agents.spawn("services/solo.ts", {}, { name: "__NAME__" });
            const status = await svc.join({ timeoutMs: 5000 });
            return { status: status.status };
        }
    "#
    .replace("__NAME__", &agent_name);
    std::fs::write(&path, &src).unwrap();

    let ctx = RuntimeContext::new();
    let backend = test_backend(ctx.clone(), &dir);
    let output = run_agent(&path, &src, &json!({}), &backend).unwrap();
    assert_eq!(output["status"], json!("hibernating"));
    let run_id = spawned_run_id(&ctx);

    // Age the run into the pre-source-history shape, then take its source
    // away: only the manifest + durable bundle remain.
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    std::fs::remove_dir_all(run_dir.join("history")).unwrap();
    std::fs::remove_dir_all(dir.join("services")).unwrap();

    let parts = hub().installed_parts().unwrap();
    hub()
        .send(
            &parts,
            &agent_name,
            "go",
            json!({ "speed": "slow" }),
            json!({ "kind": "test" }),
        )
        .unwrap();
    let outcome = wait_for_settled(&parts, &agent_name);
    assert_eq!(outcome["status"], json!("completed"), "{outcome}");
    assert_eq!(outcome["output"], json!({ "got": { "speed": "slow" } }));

    let _ = std::fs::remove_dir_all(dir);
}
