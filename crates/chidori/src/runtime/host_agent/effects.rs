//! The `chidori.agents.*` host-effect dispatch entry points.

use serde_json::{json, Value};

use crate::runtime::host_core;
use crate::runtime::typescript::bindings::HostBindingBackend;

use super::*;

/// `chidori.agents.spawn(source, input, options)` — start a detached agent
/// and return `{ name, runId }`. One durable `spawn_agent` record: a parent
/// replay returns the identity from cache without starting anything (the
/// agent is re-materialized from the registry by the next live call that
/// addresses it).
pub(crate) fn spawn_agent(backend: &HostBindingBackend, a: &Value) -> Result<Value, String> {
    let ctx = runtime_ctx(backend, "agents.spawn")?;
    if ctx.is_branch() {
        return Err(
            "chidori.agents.spawn is not supported inside a chidori.branch sub-run".to_string(),
        );
    }
    let parts = install_parts_from(backend, ctx)?;
    let source = a
        .get("source")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("chidori.agents.spawn requires a source module path")?
        .to_string();
    let input = a
        .get("input")
        .cloned()
        .filter(|v| !v.is_null())
        .unwrap_or_else(|| json!({}));
    let options_value = a
        .get("options")
        .cloned()
        .filter(|v| !v.is_null())
        .unwrap_or_else(|| json!({}));
    let mut options = SpawnOptions::parse(&options_value)?;
    if options.model.is_none() {
        // The spawner's resolved default model travels with the agent, so a
        // wake in a differently-configured process keeps the same model.
        options.model = Some(ctx.config().model);
    }
    resolve_spawn_source(backend, ctx, &parts, &source)?;
    let call_args = json!({
        "source": source,
        "input": input,
        "options": options.to_json(),
    });
    let owner = ctx.run_id();
    host_core::execute_durable_json_call(ctx, "spawn_agent", call_args, || {
        hub()
            .spawn(&parts, &source, input.clone(), &options, owner.clone())
            .map_err(|err| anyhow::anyhow!(err))
    })
    .map_err(|err| err.to_string())
}

/// `chidori.agents.send(to, name, payload)` — durable delivery into a
/// detached agent's mailbox; wakes it when it hibernates on a matching name.
pub(crate) fn send_agent(backend: &HostBindingBackend, a: &Value) -> Result<Value, String> {
    let ctx = runtime_ctx(backend, "agents.send")?;
    let parts = install_parts_from(backend, ctx)?;
    let to = a
        .get("to")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("chidori.agents.send requires a target agent name")?
        .to_string();
    let name = a
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("chidori.agents.send requires a string message name")?
        .to_string();
    let payload = a.get("payload").cloned().unwrap_or(Value::Null);
    let from = json!({ "kind": "agent", "id": ctx.run_id() });
    let call_args = json!({ "to": to, "name": name, "payload": payload, "from": from });
    host_core::execute_durable_json_call(ctx, "send_agent", call_args, || {
        hub()
            .send(&parts, &to, &name, payload.clone(), from.clone())
            .map_err(|err| anyhow::anyhow!(err))
    })
    .map_err(|err| err.to_string())
}

/// `chidori.agents.join(to, opts)` — wait for a detached agent to settle.
pub(crate) fn join_agent(backend: &HostBindingBackend, a: &Value) -> Result<Value, String> {
    let ctx = runtime_ctx(backend, "agents.join")?;
    let parts = install_parts_from(backend, ctx)?;
    let to = a
        .get("to")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("chidori.agents.join requires a target agent name")?
        .to_string();
    let timeout_ms = a
        .get("opts")
        .and_then(|o| o.get("timeoutMs"))
        .and_then(Value::as_u64);
    let call_args = json!({ "to": to, "opts": { "timeoutMs": timeout_ms } });
    host_core::execute_durable_json_call(ctx, "join_agent", call_args, || {
        hub()
            .join(&parts, &to, timeout_ms)
            .map_err(|err| anyhow::anyhow!(err))
    })
    .map_err(|err| err.to_string())
}

/// `chidori.agents.stop(to)` — cooperative stop.
pub(crate) fn stop_agent(backend: &HostBindingBackend, a: &Value) -> Result<Value, String> {
    let ctx = runtime_ctx(backend, "agents.stop")?;
    let parts = install_parts_from(backend, ctx)?;
    let to = a
        .get("to")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("chidori.agents.stop requires a target agent name")?
        .to_string();
    let run_id = a
        .get("opts")
        .and_then(|opts| opts.get("runId"))
        .or_else(|| a.get("runId"))
        .and_then(Value::as_str)
        .filter(|run_id| !run_id.is_empty())
        .map(str::to_string);
    let call_args = stop_agent_call_args(&to, run_id.as_deref());
    host_core::execute_durable_json_call(ctx, "stop_agent", call_args, || {
        let result = match run_id.as_deref() {
            Some(run_id) => hub().stop_incarnation(&parts, &to, run_id),
            None => hub().stop(&parts, &to),
        };
        result.map_err(|err| anyhow::anyhow!(err))
    })
    .map_err(|err| err.to_string())
}

/// `chidori.agents.status(to)`.
pub(crate) fn agent_status(backend: &HostBindingBackend, a: &Value) -> Result<Value, String> {
    let ctx = runtime_ctx(backend, "agents.status")?;
    let parts = install_parts_from(backend, ctx)?;
    let to = a
        .get("to")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("chidori.agents.status requires a target agent name")?
        .to_string();
    let call_args = json!({ "to": to });
    host_core::execute_durable_json_call(ctx, "agent_status", call_args, || {
        hub()
            .status(&parts, &to)
            .map_err(|err| anyhow::anyhow!(err))
    })
    .map_err(|err| err.to_string())
}

/// `chidori.agents.lookup(name)` — a registry lookup: `{name, runId, status}`
/// or null.
pub(crate) fn lookup_agent(backend: &HostBindingBackend, a: &Value) -> Result<Value, String> {
    let ctx = runtime_ctx(backend, "agents.lookup")?;
    let parts = install_parts_from(backend, ctx)?;
    let name = a
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("chidori.agents.lookup requires an agent name")?
        .to_string();
    let call_args = json!({ "name": name });
    host_core::execute_durable_json_call(ctx, "lookup_agent", call_args, || {
        hub()
            .lookup(&parts, &name)
            .map_err(|err| anyhow::anyhow!(err))
    })
    .map_err(|err| err.to_string())
}
