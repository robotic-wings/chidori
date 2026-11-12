//! Regression tests for tool-loop replay-resume context integrity.
//!
//! PR #161 flagged a "known follow-up": the JS tool loop's per-turn context
//! (assistant tool_call + tool_result) "is not journaled individually, so a
//! resume rebuilds the prompt without those turns". Investigation with these
//! tests shows that claim is wrong: each `respond()` in the loop is its own
//! journaled `prompt` record whose cached result carries the FULL
//! `LlmResponse` (blocks + toolCalls), the JS loop re-appends those blocks and
//! the rebuilt toolResult segments on replay, and the post-resume live request
//! contains every earlier turn. These tests pin that invariant so a future
//! change cannot silently regress it.
//!
//! The provider here emits `ToolUse` content blocks the way the real OpenAI /
//! Anthropic adapters do (blocks carry the tool_use; `tool_calls` is the
//! normalized twin). A fake provider that omits the blocks would make the
//! assistant turn degenerate to an empty text block — that is a provider
//! artifact, not runtime behavior.

#![cfg(test)]

use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};

use crate::mcp::McpManager;
use crate::policy::{PolicyCache, PolicyConfig};
use crate::providers::{
    ContentBlock, LlmProvider, LlmRequest, LlmResponse, ProviderRegistry, ToolCall,
};
use crate::runtime::characterization_tests::scratch;
use crate::runtime::context::{InputMode, RuntimeContext};
use crate::runtime::engine::Engine;
use crate::runtime::rust_engine::run_agent_file;
use crate::runtime::snapshot::QueuedSignal;
use crate::runtime::snapshot::RuntimePolicy;
use crate::runtime::template::TemplateEngine;
use crate::runtime::typescript::bindings::HostBindingBackend;
use crate::tools::ToolRegistry;

/// Scripted provider whose blocks mirror the real adapters: a turn that
/// requests tools carries `ToolUse` blocks (plus the normalized `tool_calls`
/// twin); the final turn carries a text block.
struct ToolUseProvider {
    /// One entry per scripted turn: `Some((name, args_json))` = a tool call,
    /// `None` = plain text from `final_text`.
    turns: Vec<Option<(String, String)>>,
    final_text: String,
    calls: std::sync::atomic::AtomicUsize,
    requests: Arc<StdMutex<Vec<LlmRequest>>>,
}

#[async_trait::async_trait]
impl LlmProvider for ToolUseProvider {
    fn supports_model(&self, _model: &str) -> bool {
        true
    }

    async fn send(&self, request: &LlmRequest) -> anyhow::Result<LlmResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let i = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match self.turns.get(i).cloned() {
            Some(Some((name, args))) => {
                let input: Value = serde_json::from_str(&args).unwrap_or(Value::Null);
                let id = format!("tc_{i}");
                Ok(LlmResponse {
                    content: String::new(),
                    blocks: vec![ContentBlock::ToolUse {
                        id: id.clone(),
                        name: name.clone(),
                        input: input.clone(),
                    }],
                    tool_calls: vec![ToolCall { id, name, input }],
                    stop_reason: "tool_use".to_string(),
                    input_tokens: 10,
                    output_tokens: 5,
                    ..LlmResponse::default()
                })
            }
            _ => Ok(LlmResponse {
                content: self.final_text.clone(),
                blocks: vec![ContentBlock::Text {
                    text: self.final_text.clone(),
                }],
                tool_calls: vec![],
                stop_reason: "end_turn".to_string(),
                input_tokens: 10,
                output_tokens: 5,
                ..LlmResponse::default()
            }),
        }
    }
}

fn tool_use_provider(
    turns: Vec<Option<(&str, &str)>>,
    final_text: &str,
) -> (ProviderRegistry, Arc<StdMutex<Vec<LlmRequest>>>) {
    let requests = Arc::new(StdMutex::new(Vec::new()));
    let mut providers = ProviderRegistry::new();
    providers.register(Box::new(ToolUseProvider {
        turns: turns
            .into_iter()
            .map(|t| t.map(|(n, a)| (n.to_string(), a.to_string())))
            .collect(),
        final_text: final_text.to_string(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        requests: Arc::clone(&requests),
    }));
    (providers, requests)
}

fn backend_for(ctx: RuntimeContext, providers: ProviderRegistry) -> HostBindingBackend {
    HostBindingBackend::for_runtime(
        ctx,
        Arc::new(providers),
        Arc::new(TemplateEngine::new(".")),
        Arc::new(tokio::runtime::Runtime::new().unwrap()),
        PolicyConfig::from_env(),
        Arc::new(StdMutex::new(PolicyCache::default())),
        RuntimePolicy::durable_default("tool-loop-resume"),
        Arc::new(ToolRegistry::new()),
        Arc::new(McpManager::new()),
    )
}

fn delivered(name: &str, payload: Value) -> Vec<QueuedSignal> {
    vec![QueuedSignal {
        name: name.to_string(),
        payload,
        from: json!({"kind": "test"}),
        delivery_seq: 1,
        enqueued_at: chrono::Utc::now(),
    }]
}

/// The agent under test: one defineTool handle whose body pauses on a signal
/// when `pauseOnRound === round`, else returns a plain string. Parameterized
/// by the round number that pauses so both files stay byte-identical across
/// the two runs of each scenario.
fn pause_agent_source(pause_on_round: i32) -> String {
    format!(
        r#"
        import {{ chidori, run, defineTool }} from "chidori:agent";
        let round = 0;
        const ask = defineTool({{
            name: "ask",
            description: "Ask the human",
            parameters: {{ type: "object", properties: {{}} }},
            run: async (_args, ch) => {{
                round += 1;
                if (round !== {pause_on_round}) return "round-" + round + "-ok";
                const sig = await ch.signal("human.answer");
                return "answer:" + JSON.stringify(sig.payload);
            }},
        }});
        run(async () => {{
            const r = await chidori.prompt("ask twice", {{ tools: [ask], maxTurns: 4 }});
            return {{ r }};
        }});
    "#
    )
}

fn write_agent(dir: &Path, pause_on_round: i32) -> std::path::PathBuf {
    let path = dir.join("agent.ts");
    std::fs::write(&path, pause_agent_source(pause_on_round)).unwrap();
    path
}

fn pause_run(
    _dir: &Path,
    path: &Path,
    pause_provider: ProviderRegistry,
) -> Vec<crate::runtime::call_log::CallRecord> {
    let ctx = RuntimeContext::new();
    ctx.set_input_mode(InputMode::Pause);
    let backend = backend_for(ctx.clone(), pause_provider);
    let result = run_agent_file(path, &json!({}), &backend);
    assert!(
        result.is_err(),
        "run must pause at the signal listen, got: {:?}",
        result.map(|v| format!("{v:?}"))
    );
    let err = result.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        err.contains("PAUSED") && err.contains("human.answer"),
        "expected a human.answer pause, got: {err}"
    );
    ctx.call_log().into_records()
}

fn resume_run(
    dir: &Path,
    path: &Path,
    records: Vec<crate::runtime::call_log::CallRecord>,
    resume_provider: ProviderRegistry,
    inbox: Vec<QueuedSignal>,
) {
    let engine = Engine::new(
        Arc::new(resume_provider),
        Arc::new(TemplateEngine::new(dir)),
        Arc::new(tokio::runtime::Runtime::new().unwrap()),
    )
    .with_persist_base(dir.to_path_buf());
    let result = engine.run_replay_pausable_with_host_promises_vfs_and_signals(
        path,
        &json!({}),
        records,
        vec![],
        crate::runtime::vfs::Vfs::new(),
        inbox,
    );
    assert!(
        result.is_ok(),
        "resume must complete the run: {:#}",
        result.err().unwrap()
    );
    let output = result.unwrap().output;
    assert_eq!(
        output,
        json!({ "r": "final-after-pause" }),
        "the final reply must come from the live continuation turn"
    );
}

/// Pause inside the FIRST tool invocation: one journaled respond, then the
/// pause. The resume's single live request must carry the full exchange —
/// user turn, the assistant's tool_use block (not an empty text block), and
/// the tool_result with the delivered payload.
#[test]
fn tool_loop_resume_preserves_first_round_context() {
    let dir = scratch("tlr_round1");
    let path = write_agent(&dir, 1);

    let (pause_p, _) = tool_use_provider(vec![Some(("ask", "{}"))], "never-reached");
    let records = pause_run(&dir, &path, pause_p);

    let (resume_p, reqs) = tool_use_provider(vec![], "final-after-pause");
    resume_run(
        &dir,
        &path,
        records,
        resume_p,
        delivered("human.answer", json!({"v": 42})),
    );

    let requests = reqs.lock().unwrap();
    assert_eq!(requests.len(), 1, "exactly one live turn after the resume");
    assert_eq!(
        serde_json::to_value(&requests[0].messages).unwrap(),
        json!([
            { "role": "user", "content": [ { "type": "text", "text": "ask twice" } ] },
            { "role": "assistant", "content": [ { "type": "tool_use", "id": "tc_0", "name": "ask", "input": {} } ] },
            { "role": "user", "cache_control": "5m", "content": [ { "type": "tool_result", "tool_use_id": "tc_0", "content": "answer:{\"v\":42}", "is_error": false } ] },
        ]),
        "the live request must contain the assistant tool_use turn and the tool_result"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Pause inside the SECOND tool invocation: one complete tool round-trip is
/// journaled before the pause, so replay must return the SECOND respond's
/// cached tool calls and the live request must carry both rounds verbatim.
#[test]
fn tool_loop_resume_preserves_second_round_context() {
    let dir = scratch("tlr_round2");
    let path = write_agent(&dir, 2);

    let (pause_p, _) = tool_use_provider(vec![Some(("ask", "{}")), Some(("ask", "{}"))], "never");
    let records = pause_run(&dir, &path, pause_p);
    assert!(
        records.iter().filter(|r| r.function == "prompt").count() == 2,
        "two respond turns journaled before the pause"
    );

    let (resume_p, reqs) = tool_use_provider(vec![], "final-after-pause");
    resume_run(
        &dir,
        &path,
        records,
        resume_p,
        delivered("human.answer", json!({"v": 99})),
    );

    let requests = reqs.lock().unwrap();
    assert_eq!(requests.len(), 1, "exactly one live turn after the resume");
    assert_eq!(
        serde_json::to_value(&requests[0].messages).unwrap(),
        json!([
            { "role": "user", "content": [ { "type": "text", "text": "ask twice" } ] },
            { "role": "assistant", "content": [ { "type": "tool_use", "id": "tc_0", "name": "ask", "input": {} } ] },
            { "role": "user", "content": [ { "type": "tool_result", "tool_use_id": "tc_0", "content": "round-1-ok", "is_error": false } ] },
            { "role": "assistant", "content": [ { "type": "tool_use", "id": "tc_1", "name": "ask", "input": {} } ] },
            { "role": "user", "cache_control": "5m", "content": [ { "type": "tool_result", "tool_use_id": "tc_1", "content": "answer:{\"v\":99}", "is_error": false } ] },
        ]),
        "the live request must contain BOTH tool rounds in full"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
