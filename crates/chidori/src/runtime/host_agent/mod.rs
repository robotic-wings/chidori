//! `chidori.agents.spawn` — detached, durable, addressable agent processes.
//!
//! A detached agent is the durable-object-shaped sibling of an in-run actor
//! (`docs/actors.md`). Where an actor is confined to its spawning run — its
//! records fold into the parent journal at a join, its lifetime ends with the
//! run — a detached agent is **its own durable run**: it has its own journal
//! under `.chidori/runs/<run_id>/`, its own registered name that outlives the
//! spawner, a durable mailbox other parties (including HTTP clients, via the
//! server's `/agents` endpoints) can deliver into, and a runtime-owned restart
//! policy. The parent's journal records only the `spawn_agent` / `send_agent`
//! / `join_agent` host calls, so the whole conversation replays from cache
//! without the fold-at-join sequence-range machinery.
//!
//! **Hibernation.** A detached agent waiting at a `chidori.signal(name)` /
//! `chidori.alarm(ms)` listen point holds NO thread and NO VM: the pause
//! unwinds the VM (the standard pause path), the supervisor persists the
//! listen state into the agent's registry descriptor, and the thread exits. A
//! matching delivery — or the alarm deadline — re-enters the module under
//! resume-by-replay: recorded effects return from cache, execution goes live
//! at the listen frontier. Because journal + mailbox + listen state are all
//! durable (and mirrored when a durable run store is configured), a fresh
//! process re-arms every hibernating agent from the registry at boot
//! (`DetachedAgentHub::rearm_from_registry`), which is also how a server
//! restart or machine replacement resumes the fleet.
//!
//! **Layout.** Split by concern; this file is the facade: [`entry`] holds the
//! shared types and per-agent durable-file helpers, [`agent_hub`] the hub
//! registry, spawn-name locking and `SpawnOptions`, [`supervisor`] the
//! per-agent supervision loop (run → settle → hibernate), [`persistence`]
//! descriptor/lease persistence, [`sources`] source resolution and
//! runtime-context assembly, and [`effects`] the `chidori.agents.*` dispatch
//! entry points.

mod agent_hub;
mod effects;
mod entry;
mod persistence;
mod sources;
mod supervisor;

pub use agent_hub::*;
pub(crate) use effects::*;
pub use entry::*;
pub(crate) use persistence::*;
pub(crate) use sources::*;
pub(crate) use supervisor::*;

#[cfg(test)]
mod tests;

/// Reserved listen name backing `chidori.alarm(ms)`: an alarm is a durable
/// signal wait on this name with a timeout, so the whole wake machinery
/// (hibernate, deadline re-arm after restart, timeout sentinel) is shared
/// with ordinary signals.
pub const ALARM_SIGNAL_NAME: &str = "__chidori.alarm__";
