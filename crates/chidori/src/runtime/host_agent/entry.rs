//! Shared detached-agent types and the per-agent durable-file helpers
//! (stop requests, signal-inbox load/ack).

pub(crate) const AGENT_THREAD_STACK_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const AGENT_DESCRIPTOR_FILE: &str = "agent.json";
pub(crate) const AGENT_STOP_REQUEST_FILE: &str = "agent.stop-request.json";

use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::mcp::McpManager;
use crate::policy::PolicyConfig;
use crate::providers::ProviderRegistry;
use crate::runtime::snapshot::{QueuedSignal, SIGNAL_INBOX_FILE};
use crate::runtime::store::RunStoreFactory;
use crate::runtime::template::TemplateEngine;
use crate::tools::ToolRegistry;

use super::agent_hub::AgentState;

/// The engine parts a detached-agent supervisor needs to run agent modules
/// outside any spawning run's lifetime. Installed from the first spawning
/// backend, or by the server at boot (`rearm_from_registry`).
#[derive(Clone)]
pub struct AgentRuntimeParts {
    pub providers: Arc<ProviderRegistry>,
    pub template_engine: Arc<TemplateEngine>,
    pub tokio_rt: Arc<tokio::runtime::Runtime>,
    pub policy: Arc<PolicyConfig>,
    pub tools: Arc<ToolRegistry>,
    pub mcp: Arc<McpManager>,
    /// `.chidori/runs` — the base every detached agent's journal lives under.
    pub run_base: PathBuf,
}

/// The durable listen state of a hibernating agent: which names wake it, the
/// pending listen call's seq/function (for the timeout sentinel's synthetic
/// record), and the alarm deadline when the wait carries one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenState {
    pub names: Vec<String>,
    pub seq: u64,
    pub function: String,
    #[serde(default)]
    pub deadline: Option<DateTime<Utc>>,
}

/// The durable descriptor of a detached agent — persisted as the registry
/// entry (and `agent.json` in the run dir) on every lifecycle transition, so
/// a fresh process can rediscover and resume the whole fleet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDescriptor {
    pub name: String,
    pub run_id: String,
    /// The source module path as given at spawn (possibly relative — replay
    /// keys stay stable across hosts; resolution happens wherever the agent
    /// is woken).
    pub source: String,
    pub input: Value,
    pub restart: String,
    pub max_restarts: u32,
    pub backoff_ms: u64,
    pub owner_run_id: String,
    /// The default model for the agent's prompts, captured at spawn time (the
    /// spawner's resolved model, or an explicit `model` spawn option) so a
    /// wake in a differently-configured process doesn't silently switch
    /// models. `None` on descriptors persisted before this field existed —
    /// those keep resolving from the waking process's environment.
    #[serde(default)]
    pub model: Option<String>,
    /// `running` | `hibernating` | `paused` | `completed` | `failed` | `stopped`
    pub status: String,
    #[serde(default)]
    pub listen: Option<ListenState>,
    #[serde(default)]
    pub restarts: u32,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AgentDescriptor {
    pub(crate) fn status_json(&self) -> Value {
        json!({
            "name": self.name,
            "runId": self.run_id,
            "status": self.status,
            "restarts": self.restarts,
            "output": self.output,
            "error": self.error,
            "waitingFor": self.listen.as_ref().map(|l| l.names.clone()),
            "deadline": self.listen.as_ref().and_then(|l| l.deadline),
        })
    }
}

pub(crate) struct AgentEntry {
    pub(crate) name: String,
    /// The engine parts this agent runs with, captured when the entry was
    /// created — so agents spawned under different run bases (e.g. in tests)
    /// stay bound to their own registry and journals.
    pub(crate) parts: AgentRuntimeParts,
    pub(crate) state: Mutex<AgentState>,
    /// Woken on lifecycle transitions (join waiters) and to interrupt
    /// restart backoff on stop.
    pub(crate) signal: Condvar,
    #[cfg(test)]
    pub(crate) live_send_notify_count: std::sync::atomic::AtomicUsize,
}

impl AgentEntry {
    pub(crate) fn factory(&self) -> RunStoreFactory {
        RunStoreFactory::shared(&self.parts.run_base)
    }
}

pub(crate) fn ensure_run_store_initialized(run_base: &Path, operation: &str) -> Result<(), String> {
    RunStoreFactory::shared(run_base)
        .ensure_initialized()
        .map_err(|err| format!("{operation}: {err:#}"))
}

/// Load a detached agent's durable signal inbox without treating damage as an
/// empty mailbox. A missing blob is the only empty-inbox case.
pub(crate) fn load_detached_signal_inbox(
    store: &dyn crate::runtime::store::RunStore,
    run_id: &str,
) -> Result<Vec<QueuedSignal>, String> {
    let bytes = store.get_blob(SIGNAL_INBOX_FILE).map_err(|err| {
        format!("reading {SIGNAL_INBOX_FILE} for detached-agent run {run_id}: {err:#}")
    })?;
    match bytes {
        None => Ok(Vec::new()),
        Some(bytes) => serde_json::from_slice(&bytes).map_err(|err| {
            format!("parsing {SIGNAL_INBOX_FILE} for detached-agent run {run_id}: {err}")
        }),
    }
}

/// Durably acknowledge one consumed inbox entry: remove it from the signal
/// inbox file in a store-level critical section, matching the in-context
/// consume path's `remove_persisted_signal` semantics. A missing entry is
/// already acknowledged; a malformed inbox fails closed.
pub(crate) fn ack_detached_signal_delivery(
    store: &dyn crate::runtime::store::RunStore,
    queued: &QueuedSignal,
) -> Result<(), String> {
    let mut remove = |current: Option<Vec<u8>>| -> anyhow::Result<Vec<u8>> {
        let mut inbox: Vec<QueuedSignal> = current
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(|err| anyhow::anyhow!("parsing signal inbox for acknowledgement: {err}"))?
            .unwrap_or_default();
        inbox.retain(|entry| entry.delivery_seq != queued.delivery_seq);
        Ok(serde_json::to_vec_pretty(&inbox)?)
    };
    store
        .update_blob(SIGNAL_INBOX_FILE, &mut remove)
        .map(|_| ())
        .map_err(|err| format!("updating signal inbox for acknowledgement: {err}"))
}

pub(crate) fn persist_stop_request(
    store: &dyn crate::runtime::store::RunStore,
    run_id: &str,
) -> Result<(), String> {
    let request = serde_json::to_vec(&json!({
        "run_id": run_id,
        "requested_at": Utc::now(),
    }))
    .map_err(|err| format!("serializing detached-agent stop request: {err}"))?;
    store
        .put_blob(AGENT_STOP_REQUEST_FILE, &request)
        .map_err(|err| format!("persisting detached-agent stop request: {err:#}"))
}

/// Check the run-scoped durable stop intent before replay or another live
/// host effect. A malformed marker fails closed rather than being ignored.
pub(crate) fn stop_request_matches_run(
    store: &dyn crate::runtime::store::RunStore,
    run_id: &str,
) -> Result<bool, String> {
    let Some(bytes) = store
        .get_blob(AGENT_STOP_REQUEST_FILE)
        .map_err(|err| format!("reading {AGENT_STOP_REQUEST_FILE}: {err:#}"))?
    else {
        return Ok(false);
    };
    let request: Value = serde_json::from_slice(&bytes)
        .map_err(|err| format!("parsing {AGENT_STOP_REQUEST_FILE}: {err}"))?;
    let requested_run = request
        .get("run_id")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{AGENT_STOP_REQUEST_FILE} has no string run_id"))?;
    if requested_run != run_id {
        return Err(format!(
            "{AGENT_STOP_REQUEST_FILE} run_id {requested_run} does not match {run_id}"
        ));
    }
    Ok(true)
}

pub(crate) fn stop_agent_call_args(to: &str, run_id: Option<&str>) -> Value {
    match run_id {
        Some(run_id) => json!({ "to": to, "runId": run_id }),
        None => json!({ "to": to }),
    }
}
