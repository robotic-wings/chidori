//! Agent source resolution (spawn-time, wake-time, template) and the
//! runtime-context / engine-parts assembly each supervision iteration uses.

use std::path::{Path, PathBuf};

use crate::runtime::context::RuntimeContext;
use crate::runtime::store::RunStoreFactory;
use crate::runtime::template::TemplateEngine;
use crate::runtime::typescript::bindings::HostBindingBackend;

use super::*;

pub(crate) fn resolve_source(template_engine: &TemplateEngine, source: &str) -> PathBuf {
    let path = Path::new(source);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        template_engine.base_dir().join(path)
    }
}

/// Resolve the entry module for one wake of a detached agent.
///
/// The descriptor's `source` is the path as given at spawn, so a node that
/// holds the agent's project tree resolves it locally — that stays the fast
/// path, and nothing else is read. A node that does **not** have the tree
/// (the fleet case: any node may wake any agent it can lease) materializes
/// the run's own recorded implementation out of its durable store instead
/// (`source_history::materialize_source`), then runs from there. Failing that,
/// the spawning run's materialized tree is the last resort — a freshly
/// spawned agent has no history of its own yet, so its module can only come
/// from the spawner's (see `resolve_spawn_source`).
pub(crate) fn resolve_wake_source(
    parts: &AgentRuntimeParts,
    store: &dyn crate::runtime::store::RunStore,
    descriptor: &AgentDescriptor,
) -> Result<PathBuf, String> {
    let resolved = resolve_source(&parts.template_engine, &descriptor.source);
    if resolved.is_file() {
        return Ok(resolved);
    }
    let mut attempts = Vec::new();
    match materialize_run_source(parts, store, &descriptor.run_id) {
        Ok(Some(entry)) => return Ok(entry),
        Ok(None) => attempts.push("the run store holds no recorded source".to_string()),
        Err(err) => attempts.push(err),
    }
    if let Ok(root) = crate::runtime::source_history::materialization_root(
        &parts.run_base,
        &descriptor.owner_run_id,
    ) {
        let inherited = root.join(&descriptor.source);
        if inherited.is_file() {
            return Ok(inherited);
        }
    }
    Err(format!(
        "reading {}: no such file on this node, and materializing the agent's source \
         from the durable run store failed ({})",
        resolved.display(),
        attempts.join("; ")
    ))
}

/// Materialize `run_id`'s recorded source tree under this node's
/// materialization root, returning the entry module. `Ok(None)` means the
/// store holds no recoverable source (never executed anywhere, or its
/// history was pruned).
pub(crate) fn materialize_run_source(
    parts: &AgentRuntimeParts,
    store: &dyn crate::runtime::store::RunStore,
    run_id: &str,
) -> Result<Option<PathBuf>, String> {
    let root = crate::runtime::source_history::materialization_root(&parts.run_base, run_id)
        .map_err(|err| format!("{err:#}"))?;
    match crate::runtime::source_history::materialize_source(store, &root) {
        Ok(Some(entry)) => {
            tracing::info!(
                run_id,
                entry = %entry.display(),
                "materialized the run's source from the durable store"
            );
            Ok(Some(entry))
        }
        Ok(None) => Ok(None),
        Err(err) => Err(format!("{err:#}")),
    }
}

/// Check a spawn's source module is reachable before recording the spawn, so
/// a typo fails at the call rather than inside the agent's first iteration.
///
/// The spawning run's recorded tree is consulted on a local miss: a run
/// resumed on a node that does not hold its project directory would otherwise
/// fail here on a module it may not even need to read — a replayed
/// `spawn_agent` returns the agent's identity from cache and starts nothing.
/// A freshly spawned agent has no history of its own yet, so the materialized
/// spawner tree is also where its first iteration finds the module
/// (`resolve_wake_source`).
pub(crate) fn resolve_spawn_source(
    backend: &HostBindingBackend,
    ctx: &RuntimeContext,
    parts: &AgentRuntimeParts,
    source: &str,
) -> Result<(), String> {
    if resolve_source(&backend.template_engine(), source).is_file() {
        return Ok(());
    }
    let run_id = ctx.run_id();
    let store = match ctx.store() {
        Some(store) => store,
        None => RunStoreFactory::shared(&parts.run_base).store_for(&run_id),
    };
    if matches!(
        materialize_run_source(parts, store.as_ref(), &run_id),
        Ok(Some(_))
    ) {
        if let Ok(root) =
            crate::runtime::source_history::materialization_root(&parts.run_base, &run_id)
        {
            if root.join(source).is_file() {
                return Ok(());
            }
        }
    }
    Err(format!(
        "chidori.agents.spawn: source module not found: {source} (absent on this node, and \
         materializing the spawning run's recorded source did not produce it)"
    ))
}

// ---------------------------------------------------------------------------
// Host bindings (`chidori.agents.*`)
// ---------------------------------------------------------------------------

pub(crate) fn runtime_ctx<'a>(
    backend: &'a HostBindingBackend,
    what: &str,
) -> Result<&'a RuntimeContext, String> {
    backend
        .runtime_ctx()
        .ok_or_else(|| format!("chidori.{what} requires the runtime host backend"))
}

/// Install the hub's runtime parts from a spawning backend, deriving
/// `run_base` from the run's persist dir. Errors when persistence is off —
/// a detached agent without a journal cannot exist.
pub(crate) fn install_parts_from(
    backend: &HostBindingBackend,
    ctx: &RuntimeContext,
) -> Result<AgentRuntimeParts, String> {
    let (providers, template_engine, tokio_rt, policy, tools, mcp) = backend
        .runtime_parts()
        .ok_or("chidori.agents requires the runtime host backend")?;
    let run_base = ctx
        .persist_dir()
        .and_then(|dir| dir.parent().map(Path::to_path_buf))
        .ok_or(
            "chidori.agents requires persistence: detached agents are durable runs \
             (run with a project dir so `.chidori/runs/` exists)",
        )?;
    let parts = AgentRuntimeParts {
        providers,
        template_engine,
        tokio_rt,
        policy,
        tools,
        mcp,
        run_base,
    };
    hub().install_parts(parts.clone());
    Ok(parts)
}
