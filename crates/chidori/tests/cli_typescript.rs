use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn chidori_bin() -> &'static str {
    env!("CARGO_BIN_EXE_chidori")
}

fn temp_project(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "chidori-cli-ts-{name}-{}-{}",
        std::process::id(),
        unique_suffix()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn unique_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
}

fn run_chidori(args: &[&str], cwd: &Path) -> Output {
    Command::new(chidori_bin())
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap()
}

fn run_chidori_with_env(args: &[&str], cwd: &Path, envs: &[(&str, &Path)]) -> Output {
    let mut command = Command::new(chidori_bin());
    command.args(args).current_dir(cwd);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().unwrap()
}

fn run_chidori_with_str_env(args: &[&str], cwd: &Path, envs: &[(&str, &str)]) -> Output {
    let mut command = Command::new(chidori_bin());
    command.args(args).current_dir(cwd);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().unwrap()
}

fn run_chidori_with_run_store_mode(args: &[&str], cwd: &Path, mode: Option<&str>) -> Output {
    let mut command = Command::new(chidori_bin());
    command
        .args(args)
        .current_dir(cwd)
        .env_remove("CHIDORI_RUN_STORE")
        .env_remove("CHIDORI_RUN_DB")
        .env_remove("CHIDORI_DURABILITY");
    if let Some(mode) = mode {
        command.env("CHIDORI_RUN_STORE", mode);
    }
    command.output().unwrap()
}

struct LocalHttpServer {
    address: String,
    requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    stopping: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl LocalHttpServer {
    fn start() -> Self {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::thread;
        use std::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let requests = std::sync::Arc::new(AtomicUsize::new(0));
        let stopping = std::sync::Arc::new(AtomicBool::new(false));
        let server_requests = std::sync::Arc::clone(&requests);
        let server_stopping = std::sync::Arc::clone(&stopping);
        let thread = thread::spawn(move || {
            while !server_stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut request = [0u8; 4096];
                        let _ = stream.read(&mut request);
                        server_requests.fetch_add(1, Ordering::SeqCst);
                        let _ = stream.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("local HTTP server failed: {error}"),
                }
            }
        });
        Self {
            address,
            requests,
            stopping,
            thread: Some(thread),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for LocalHttpServer {
    fn drop(&mut self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

struct HttpReplayFixture {
    dir: PathBuf,
    agent: PathBuf,
    run_id: String,
    run_dir: PathBuf,
    records_path: PathBuf,
    http_record: serde_json::Value,
    mirror_db: Option<PathBuf>,
}

impl Drop for HttpReplayFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn create_http_replay_fixture(server: &LocalHttpServer) -> HttpReplayFixture {
    create_http_replay_fixture_with_mirror(server, false)
}

fn create_http_replay_fixture_with_mirror(
    server: &LocalHttpServer,
    use_sqlite_mirror: bool,
) -> HttpReplayFixture {
    let dir = temp_project("http-promise-contract");
    let mirror_db = use_sqlite_mirror.then(|| dir.join("runs.sqlite3"));
    let mirror_db_text = mirror_db
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
                export async function agent() {{
                    await chidori.log("prefix", {{}});
                    const response = await fetch("http://{}/effect");
                    return await response.json();
                }}
            "#,
            server.address
        ),
    )
    .unwrap();
    let mut envs = vec![
        ("CHIDORI_DURABILITY", "effect"),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    if let Some(db_path) = &mirror_db_text {
        envs.push(("CHIDORI_RUN_STORE", "sqlite"));
        envs.push(("CHIDORI_RUN_DB", db_path));
    }
    let output =
        run_chidori_with_str_env(&["run", agent.to_str().unwrap(), "--trusted"], &dir, &envs);
    assert_success(&output);
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let records: Vec<serde_json::Value> = fs::read_to_string(&records_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        records.len(),
        2,
        "fixture should contain a prefix and one HTTP call"
    );
    assert_eq!(records[0]["seq"], 1);
    assert_eq!(records[1]["seq"], 2);
    assert_eq!(records[1]["function"], "http");
    HttpReplayFixture {
        dir,
        agent,
        run_id,
        run_dir,
        records_path,
        http_record: records[1].clone(),
        mirror_db,
    }
}

fn journal_records(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn remove_http_journal_row(fixture: &HttpReplayFixture) {
    let mut records = journal_records(&fixture.records_path);
    records.retain(|record| record["seq"] != 2);
    let records: Vec<chidori::runtime::call_log::CallRecord> = records
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<_, _>>()
        .unwrap();
    fixture_run_store(fixture).write_call_log(&records).unwrap();
}

fn persist_resolved_http_operation(fixture: &HttpReplayFixture) -> serde_json::Value {
    use chidori::runtime::context::RuntimeContext;
    use chidori::runtime::snapshot::PendingHostOperationKind;
    let ctx = RuntimeContext::new();
    ctx.enable_persistence_with_store(fixture.run_dir.clone(), fixture_run_store(fixture));
    let prefix = ctx.begin_host_operation_with_function(
        1,
        PendingHostOperationKind::Log,
        Some("log".to_string()),
        serde_json::json!({"message":"prefix","data":{}}),
    );
    ctx.resolve_host_operation(prefix, serde_json::json!(null))
        .unwrap();
    let http = ctx.begin_host_operation_with_function(
        2,
        PendingHostOperationKind::Http,
        Some("http".to_string()),
        fixture.http_record["args"].clone(),
    );
    assert_eq!(
        http.0, 2,
        "prefix reservation should make HTTP operation id 2"
    );
    ctx.resolve_host_operation(http, fixture.http_record["result"].clone())
        .unwrap();

    let path = fixture.run_dir.join("host_promises").join("2.json");
    let bytes = fs::read(path).expect("runtime API must persist the resolved per-op event");
    serde_json::from_slice(&bytes).unwrap()
}

fn persist_pending_http_operation(fixture: &HttpReplayFixture) -> serde_json::Value {
    use chidori::runtime::context::RuntimeContext;
    use chidori::runtime::snapshot::PendingHostOperationKind;
    let ctx = RuntimeContext::new();
    ctx.enable_persistence_with_store(fixture.run_dir.clone(), fixture_run_store(fixture));
    let prefix = ctx.begin_host_operation_with_function(
        1,
        PendingHostOperationKind::Log,
        Some("log".to_string()),
        serde_json::json!({"message":"prefix","data":{}}),
    );
    ctx.resolve_host_operation(prefix, serde_json::json!(null))
        .unwrap();
    let http = ctx.begin_host_operation_with_function(
        2,
        PendingHostOperationKind::Http,
        Some("http".to_string()),
        fixture.http_record["args"].clone(),
    );
    assert_eq!(http.0, 2);

    let path = fixture.run_dir.join("host_promises").join("2.json");
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn fixture_run_store(
    fixture: &HttpReplayFixture,
) -> std::sync::Arc<dyn chidori::runtime::store::RunStore> {
    use chidori::runtime::store::{FsRunStore, SqliteRunStore, SqliteRunStoreShared, TeeRunStore};
    use std::sync::Arc;

    let primary = FsRunStore::new(&fixture.run_dir);
    match &fixture.mirror_db {
        Some(db_path) => {
            let shared = SqliteRunStoreShared::open(db_path).unwrap();
            Arc::new(TeeRunStore::new(
                primary,
                Arc::new(SqliteRunStore::new(shared, fixture.run_id.clone())),
            ))
        }
        None => Arc::new(primary),
    }
}

fn store_host_promise_blob(fixture: &HttpReplayFixture, id: u64, blob: &serde_json::Value) {
    let dir = fixture.run_dir.join("host_promises");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join(format!("{id}.json")),
        serde_json::to_vec_pretty(blob).unwrap(),
    )
    .unwrap();
}

fn resume_http_fixture(fixture: &HttpReplayFixture, extra_env: &[(&str, &str)]) -> Output {
    let mut envs = vec![
        ("CHIDORI_DURABILITY", "effect"),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    envs.extend_from_slice(extra_env);
    run_chidori_with_str_env(
        &[
            "resume",
            fixture.agent.to_str().unwrap(),
            &fixture.run_id,
            "--trusted",
        ],
        &fixture.dir,
        &envs,
    )
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn output_diagnostic(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_diagnostic_contains(output: &Output, expected: &str) {
    let diagnostic = output_diagnostic(output);
    assert!(
        diagnostic.contains(expected),
        "diagnostic must contain {expected:?}; output:\n{diagnostic}"
    );
}

fn assert_journal_location(output: &Output, path: &Path, line: usize) {
    let diagnostic = output_diagnostic(output);
    assert!(
        diagnostic.contains(&path.display().to_string()),
        "journal diagnostic must identify {}; output:\n{diagnostic}",
        path.display()
    );
    assert!(
        diagnostic.contains(&format!("line {line}")),
        "journal diagnostic must identify 1-based line {line}; output:\n{diagnostic}"
    );
}

#[test]
fn cli_check_accepts_typescript_agent() {
    let dir = temp_project("check");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("checking", { ok: true });
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(&["check", agent.to_str().unwrap()], &dir);
    assert_success(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains("OK:"));

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_check_rejects_legacy_starlark_agent() {
    let dir = temp_project("reject-star");
    let agent = dir.join("agent.star");
    fs::write(
        &agent,
        r#"
            def agent():
                return {"ok": True}
        "#,
    )
    .unwrap();

    let output = run_chidori(&["check", agent.to_str().unwrap()], &dir);
    assert_failure(&output);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("TypeScript `.ts` agents"));

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_run_typescript_agent_outputs_json_and_persists_snapshot_manifest() {
    let dir = temp_project("run");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                const name = input.name ?? "world";
                await chidori.log("hello", { name });
                return { greeting: "Hello, " + name + "!" };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(
        &["run", agent.to_str().unwrap(), "--input", "name=CLI"],
        &dir,
    );
    assert_success(&output);
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["greeting"], "Hello, CLI!");

    let runs_dir = dir.join(".chidori").join("runs");
    let mut run_dirs = fs::read_dir(&runs_dir).unwrap();
    let run_dir = run_dirs.next().unwrap().unwrap().path();
    // Single-copy journal layout: records.jsonl is the journal; no
    // checkpoint.json twin is written.
    assert!(run_dir.join("records.jsonl").exists());
    assert!(!run_dir.join("checkpoint.json").exists());
    assert!(run_dir.join("runtime.snapshot.json").exists());
    let manifest: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("runtime.snapshot.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["abi"]["engine_fork"], "chidori-quickjs");
    assert_eq!(manifest["snapshot_file"], "runtime.snapshot");

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_run_typescript_agent_uses_definetool() {
    let dir = temp_project("run-definetool");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            import { chidori, run, defineTool } from "chidori:agent";
            const webSearch = defineTool({
                name: "web_search",
                description: "Search the web for a short query.",
                parameters: {
                    type: "object",
                    properties: { query: { type: "string", description: "Search query" } },
                    required: ["query"],
                },
                run: async (args) => {
                    await chidori.log("web_search", { query: args.query });
                    return {
                        query: args.query,
                        results: [
                            { title: "Chidori tools", url: "https://example.test/chidori-tools" },
                        ],
                    };
                },
            });
            run(async (input) => {
                const searchResults = await webSearch.run(
                    { query: "latest developments " + input.topic },
                    chidori,
                );
                return { searchResults };
            });
        "#,
    )
    .unwrap();

    let output = run_chidori(
        &[
            "run",
            agent.to_str().unwrap(),
            "--trusted",
            "--input",
            r#"{"topic":"chidori"}"#,
        ],
        &dir,
    );
    assert_success(&output);
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        stdout["searchResults"]["query"],
        "latest developments chidori"
    );
    assert_eq!(
        stdout["searchResults"]["results"][0]["title"],
        "Chidori tools"
    );

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_stream_typescript_agent_emits_call_and_done_events() {
    let dir = temp_project("stream");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("streaming", { value: input.value });
                return { value: input.value };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(
        &[
            "run",
            agent.to_str().unwrap(),
            "--stream",
            "--input",
            r#"{"value": 42}"#,
        ],
        &dir,
    );
    assert_success(&output);
    let events: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events
        .iter()
        .any(|event| event["type"] == "call" && event["record"]["function"] == "log"));
    assert!(events
        .iter()
        .any(|event| event["type"] == "done" && event["output"]["value"] == 42));

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_stream_workspace_binding_writes_real_disk_manifest() {
    let dir = temp_project("stream-workspace");
    let workspace = dir.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.workspace.write("agent.ts", "export default {};", {
                    language: "typescript",
                });
                const files = await chidori.workspace.list({ completeOnly: true });
                return {
                    content: await chidori.workspace.read("agent.ts"),
                    files,
                    manifest: await chidori.workspace.manifest(),
                };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_with_env(
        &[
            "run",
            agent.to_str().unwrap(),
            "--trusted",
            "--stream",
            "--input",
            r#"{}"#,
        ],
        &dir,
        &[("CHIDORI_WORKSPACE_ROOT", &workspace)],
    );
    assert_success(&output);
    assert_eq!(
        fs::read_to_string(workspace.join("agent.ts")).unwrap(),
        "export default {};"
    );
    let manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(workspace.join(".generation/manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["files"]["agent.ts"]["status"], "complete");

    let events: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events
        .iter()
        .any(|event| event["type"] == "call" && event["record"]["function"] == "workspace"));
    assert!(events.iter().any(|event| {
        event["type"] == "done"
            && event["output"]["content"] == "export default {};"
            && event["output"]["files"][0]["path"] == "agent.ts"
    }));

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_workspace_write_denied_by_policy_is_blocked() {
    let dir = temp_project("workspace-policy-deny");
    let workspace = dir.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.workspace.write("secret.ts", "export default {};");
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    // A restrictive profile that denies workspace writes. Before workspace
    // effects were routed through the policy gate, this rule had no effect and
    // the write went through unconditionally.
    let policy = r#"{
        "default": "always_allow",
        "rules": [
            { "target": "workspace:write", "decision": "never_allow", "reason": "read-only profile" }
        ]
    }"#;

    let output = run_chidori_with_str_env(
        &["run", agent.to_str().unwrap(), "--input", r#"{}"#],
        &dir,
        &[
            ("CHIDORI_WORKSPACE_ROOT", workspace.to_str().unwrap()),
            ("CHIDORI_POLICY", policy),
        ],
    );

    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("workspace:write") && stderr.contains("denied"),
        "expected a workspace:write policy denial, got stderr:\n{stderr}"
    );
    // The deny must happen before the effect runs: no file on disk.
    assert!(
        !workspace.join("secret.ts").exists(),
        "denied workspace.write must not have touched the disk"
    );

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_workspace_read_allowed_while_write_denied() {
    let dir = temp_project("workspace-policy-read-ok");
    let workspace = dir.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    // Seed a file the agent is allowed to read.
    fs::write(workspace.join("seed.txt"), "hello").unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                return { content: await chidori.workspace.read("seed.txt") };
            }
        "#,
    )
    .unwrap();

    // Deny only writes; reads fall through to the AlwaysAllow default.
    let policy = r#"{
        "default": "always_allow",
        "rules": [
            { "target": "workspace:write", "decision": "never_allow" }
        ]
    }"#;

    let output = run_chidori_with_str_env(
        &["run", agent.to_str().unwrap(), "--input", r#"{}"#],
        &dir,
        &[
            ("CHIDORI_WORKSPACE_ROOT", workspace.to_str().unwrap()),
            ("CHIDORI_POLICY", policy),
        ],
    );

    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("hello"),
        "expected the allowed workspace.read to return its content, got:\n{stdout}"
    );

    fs::remove_dir_all(dir).ok();
}

/// `chidori.workspace` must resolve out of the box (like the `docs` init
/// template), scoping to the agent's project directory even when
/// `CHIDORI_WORKSPACE_ROOT` is unset. Regression for the scaffolded `docs`
/// template failing with "requires CHIDORI_WORKSPACE_ROOT or a runtime
/// workspace root" on `chidori run`/`chat`.
#[test]
fn cli_workspace_defaults_to_project_dir_without_env() {
    let dir = temp_project("workspace-default-root");
    fs::create_dir_all(dir.join("docs")).unwrap();
    fs::write(dir.join("docs").join("readme.md"), "workspace-ok").unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                const entries = await chidori.workspace.list();
                const md = entries.map((e) => e.path).find((p) => p.endsWith("readme.md"));
                return { content: await chidori.workspace.read(md) };
            }
        "#,
    )
    .unwrap();

    // Note: no CHIDORI_WORKSPACE_ROOT — the CLI must fall back to the agent dir.
    let output = run_chidori(&["run", agent.to_str().unwrap(), "--input", r#"{}"#], &dir);
    assert_success(&output);
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["content"], "workspace-ok");

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_untrusted_profile_denies_workspace_write() {
    let dir = temp_project("untrusted-workspace-write");
    let workspace = dir.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.workspace.write("agent.ts", "export default {};", {
                    language: "typescript",
                });
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_with_str_env(
        &["run", agent.to_str().unwrap(), "--input", r#"{}"#],
        &dir,
        &[
            ("CHIDORI_POLICY_PROFILE", "untrusted"),
            ("CHIDORI_WORKSPACE_ROOT", workspace.to_str().unwrap()),
        ],
    );
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("workspace:write") && stderr.contains("denied"),
        "expected workspace:write denial, got stderr:\n{stderr}"
    );
    // The denied write must not have touched disk.
    assert!(!workspace.join("agent.ts").exists());

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_untrusted_profile_allows_read_only_workspace() {
    let dir = temp_project("untrusted-workspace-read");
    let workspace = dir.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let agent = dir.join("agent.ts");
    // Read-only introspection is on the untrusted allowlist, so listing must
    // succeed even under the deny-by-default profile.
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                const files = await chidori.workspace.list({ completeOnly: true });
                return { count: files.length };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_with_str_env(
        &["run", agent.to_str().unwrap(), "--input", r#"{}"#],
        &dir,
        &[
            ("CHIDORI_POLICY_PROFILE", "untrusted"),
            ("CHIDORI_WORKSPACE_ROOT", workspace.to_str().unwrap()),
        ],
    );
    assert_success(&output);

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_untrusted_flag_overrides_permissive_policy_env() {
    let dir = temp_project("untrusted-flag-precedence");
    let workspace = dir.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.workspace.write("out.txt", "data");
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    // --untrusted must win over an explicitly permissive CHIDORI_POLICY:
    // the flag is the operator's last word, env vars are ambient config.
    let permissive = r#"{ "default": "always_allow", "rules": [] }"#;
    let output = run_chidori_with_str_env(
        &[
            "run",
            agent.to_str().unwrap(),
            "--untrusted",
            "--input",
            r#"{}"#,
        ],
        &dir,
        &[
            ("CHIDORI_POLICY", permissive),
            ("CHIDORI_WORKSPACE_ROOT", workspace.to_str().unwrap()),
        ],
    );
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("workspace:write") && stderr.contains("denied"),
        "expected --untrusted to deny workspace:write despite permissive env policy, got stderr:\n{stderr}"
    );
    assert!(!workspace.join("out.txt").exists());

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_serve_rejects_conflicting_trust_flags() {
    let dir = temp_project("serve-trust-flag-conflict");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    // --trusted and --untrusted contradict each other; clap must refuse the
    // combination before any server starts.
    let output = run_chidori(
        &["serve", agent.to_str().unwrap(), "--untrusted", "--trusted"],
        &dir,
    );
    assert!(
        !output.status.success(),
        "conflicting trust flags must not start a server"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--untrusted") && stderr.contains("--trusted"),
        "expected a flag-conflict error, got stderr:\n{stderr}"
    );

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_stream_accepts_multiline_typed_agent_signature() {
    let dir = temp_project("stream-multiline-types");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            import type { Chidori } from "chidori:agent";

            export async function agent(
                input: { name?: string },
                chidori: Chidori,
            ) {
                const name = input.name ?? "world";
                await chidori.log("hello", { name });
                return { greeting: "Hello, " + name + "!" };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(
        &[
            "run",
            agent.to_str().unwrap(),
            "--stream",
            "--input",
            r#"{"name":"Ada"}"#,
        ],
        &dir,
    );
    assert_success(&output);
    let events: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events
        .iter()
        .any(|event| event["type"] == "done" && event["output"]["greeting"] == "Hello, Ada!"));

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_stream_failure_done_event_includes_full_error_chain() {
    let dir = temp_project("stream-failure");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                const broken = ;
                return { broken };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(
        &[
            "run",
            agent.to_str().unwrap(),
            "--stream",
            "--input",
            r#"{}"#,
        ],
        &dir,
    );
    assert_failure(&output);

    let events: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let done = events
        .iter()
        .find(|event| event["type"] == "done")
        .expect("missing done event");
    let error = done["error"].as_str().unwrap_or_default();
    // oxc surfaces malformed agent code as a parse error before we ever reach
    // QuickJS; the done event still has to name what failed and where so the
    // CLI user can find the broken file.
    assert!(error.contains("agent.ts"), "{error}");
    assert!(error.contains("TypeScript parse error"), "{error}");

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_run_default_posture_fails_closed_on_gated_effects_without_terminal() {
    // With no policy configured, no --trusted, and no terminal to answer the
    // approval prompt, a gated effect (network fetch) must fail closed with an
    // actionable message instead of running fully trusted.
    let dir = temp_project("default-posture");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                return await fetch("https://example.test/gated");
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(&["run", agent.to_str().unwrap()], &dir);
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("requires approval"),
        "default posture should ask/deny, got:\n{stderr}"
    );
    assert!(
        stderr.contains("--trusted"),
        "denial should name the opt-out, got:\n{stderr}"
    );

    // The read-only workspace surface stays open without prompts.
    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_resume_refuses_changed_agent_source() {
    // `chidori resume` must verify the stored source fingerprints before
    // replaying the journal, exactly like the server resume routes: replay is
    // positional, so pairing cached results with changed code is a silent
    // correctness hazard.
    let dir = temp_project("resume-source-check");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("original", {});
                return { version: 1 };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(&["run", agent.to_str().unwrap()], &dir);
    assert_success(&output);

    let runs_dir = dir.join(".chidori").join("runs");
    let run_id = fs::read_dir(&runs_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();

    // Unchanged source resumes fine.
    let output = run_chidori(&["resume", agent.to_str().unwrap(), &run_id], &dir);
    assert_success(&output);

    // Changed source is refused with a source-mismatch error.
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("edited", {});
                return { version: 2 };
            }
        "#,
    )
    .unwrap();
    let output = run_chidori(&["resume", agent.to_str().unwrap(), &run_id], &dir);
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("source") && stderr.contains("mismatch"),
        "expected a source mismatch refusal, got:\n{stderr}"
    );

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_resume_rejects_missing_first_root_record_before_live_effect() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use std::thread;
    use std::time::Duration;

    let dir = temp_project("resume-missing-root-head");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let stopping = Arc::new(AtomicBool::new(false));
    let server_effects = Arc::clone(&effects);
    let server_stopping = Arc::clone(&stopping);
    let server = thread::spawn(move || {
        while !server_stopping.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut request = [0u8; 4096];
                    let _ = stream.read(&mut request);
                    server_effects.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("local effect server failed: {error}"),
            }
        }
    });

    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
                export async function agent(input, chidori) {{
                    await fetch("http://{address}/effect", {{ method: "POST" }});
                    await chidori.log("after_effect", {{}});
                    return {{ ok: true }};
                }}
            "#
        ),
    )
    .unwrap();

    let http_env = [("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1")];
    let output = run_chidori_with_str_env(
        &["run", agent.to_str().unwrap(), "--trusted"],
        &dir,
        &http_env,
    );
    assert_success(&output);
    assert_eq!(
        effects.load(Ordering::SeqCst),
        1,
        "initial run should perform one live effect"
    );
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let journal = fs::read_to_string(&records_path).unwrap();
    let mut lines = journal.lines();
    let first: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(
        first["seq"], 1,
        "the fresh top-level run must establish the seq=1 anchor from its persisted journal"
    );
    assert_eq!(first["function"], "http");
    let damaged = lines.collect::<Vec<_>>().join("\n") + "\n";
    fs::write(&records_path, damaged).unwrap();

    let output = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--trusted"],
        &dir,
        &http_env,
    );
    stopping.store(true, Ordering::Relaxed);
    server.join().unwrap();

    assert_eq!(
        effects.load(Ordering::SeqCst),
        1,
        "corruption must be rejected before replay can execute the deleted live effect"
    );
    assert_failure(&output);
    assert_journal_location(&output, &records_path, 1);
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.to_lowercase().contains("corrupt"),
        "expected a corruption diagnostic, got:\n{diagnostic}"
    );
    assert!(
        diagnostic.contains(&records_path.display().to_string()),
        "corruption diagnostic must identify the journal path; got:\n{diagnostic}"
    );
    assert!(
        diagnostic.contains("line 1"),
        "corruption diagnostic must identify 1-based line 1; got:\n{diagnostic}"
    );
    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_resume_rejects_out_of_order_non_one_history() {
    let dir = temp_project("resume-legacy-out-of-order");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("first", {});
                await chidori.log("second", {});
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(&["run", agent.to_str().unwrap()], &dir);
    assert_success(&output);
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let rows: Vec<serde_json::Value> = fs::read_to_string(&records_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(rows.len() >= 2, "fixture must contain at least two records");
    let mut invalid = rows.clone();
    // Keep seq 1 as the root anchor so this specifically exercises structural
    // order validation rather than the fresh-root missing-head guard.
    invalid[0]["seq"] = serde_json::json!(1);
    invalid[1]["seq"] = serde_json::json!(0);
    let mut journal = Vec::new();
    for row in &invalid {
        journal.extend(serde_json::to_vec(row).unwrap());
        journal.push(b'\n');
    }
    fs::write(&records_path, &journal).unwrap();
    fs::write(
        run_dir.join("checkpoint.json"),
        serde_json::to_vec_pretty(&invalid).unwrap(),
    )
    .unwrap();

    let output = run_chidori(&["resume", agent.to_str().unwrap(), &run_id], &dir);
    assert_failure(&output);
    let diagnostic = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.to_lowercase().contains("corrupt"),
        "out-of-order records must be rejected as journal corruption; output:\n{diagnostic}"
    );
    assert!(
        diagnostic.contains(&records_path.display().to_string()),
        "out-of-order corruption must identify the journal path; output:\n{diagnostic}"
    );
    assert!(
        diagnostic.contains("line 2"),
        "out-of-order corruption must identify 1-based line 2; output:\n{diagnostic}"
    );
    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_branch_resume_imports_legacy_http_prefix_without_repeating_effect() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use std::thread;
    use std::time::Duration;

    let dir = temp_project("branch-resume-legacy-archive");
    let import_dir = temp_project("branch-resume-legacy-import");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let stopping = Arc::new(AtomicBool::new(false));
    let server_requests = Arc::clone(&requests);
    let server_stopping = Arc::clone(&stopping);
    let server = thread::spawn(move || {
        while !server_stopping.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut request = [0u8; 4096];
                    let _ = stream.read(&mut request);
                    server_requests.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("local HTTP server failed: {error}"),
            }
        }
    });
    struct ServerGuard {
        stopping: Arc<AtomicBool>,
        server: Option<thread::JoinHandle<()>>,
    }
    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.stopping.store(true, Ordering::Relaxed);
            if let Some(server) = self.server.take() {
                server.join().unwrap();
            }
        }
    }
    let server_guard = ServerGuard {
        stopping: Arc::clone(&stopping),
        server: Some(server),
    };

    let branch_sources = dir.join("branches");
    fs::create_dir_all(&branch_sources).unwrap();
    fs::write(
        branch_sources.join("effect_then_input.ts"),
        format!(
            r#"
                export async function agent(input) {{
                    const response = await fetch("http://{address}/effect");
                    const result = await response.json();
                    const answer = await chidori.input("Pick a color", {{ default: "initial" }});
                    return {{ result, answer }};
                }}
            "#
        ),
    )
    .unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
                export async function agent() {{
                    const outcomes = await chidori.branch([
                        {{ label: "effect", source: "{}/branches/effect_then_input.ts", input: {{}} }}
                    ]);
                    return {{ outcomes }};
                }}
            "#,
            dir.display()
        ),
    )
    .unwrap();

    let env = [
        ("CHIDORI_DURABILITY", "effect"),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    let output =
        run_chidori_with_str_env(&["run", agent.to_str().unwrap(), "--trusted"], &dir, &env);
    assert_success(&output);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "initial branch HTTP effect"
    );

    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let run_output: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let branch_id = run_output["outcomes"][0]["branchId"].as_str().unwrap();
    assert_eq!(run_output["outcomes"][0]["status"], "completed");
    let branch_store = fs::read_dir(run_dir.join("branches"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find_map(|op_dir| {
            let candidate = op_dir.join("branch-000");
            candidate.join("branch.json").is_file().then_some(candidate)
        })
        .expect("real CLI run must persist its branch store");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&fs::read(branch_store.join("branch.json")).unwrap()).unwrap();
    assert_eq!(meta["branch_id"], branch_id);
    let range_start = meta["sequence_range"]["start"].as_u64().unwrap();
    let range_end = meta["sequence_range"]["end_exclusive"].as_u64().unwrap();
    let records: Vec<serde_json::Value> =
        serde_json::from_slice(&fs::read(branch_store.join("checkpoint.json")).unwrap()).unwrap();
    assert!(
        records.len() >= 2,
        "branch journal must include HTTP and input"
    );
    let http = &records[0];
    assert_eq!(http["function"], "http");
    assert!(http["seq"].as_u64().unwrap() > 1);
    assert!(records.iter().all(|record| {
        let seq = record["seq"].as_u64().unwrap();
        seq >= range_start && seq < range_end
    }));
    let input = records
        .iter()
        .find(|record| record["function"] == "input")
        .expect("default input should leave a persisted input row");

    // Recreate the prior paused archive format from the real child branch:
    // branch identity/range and pending seq come from branch.json, while the
    // HTTP result remains its sole checkpoint row.
    meta["status"] = serde_json::json!("paused");
    meta["pending_prompt"] = serde_json::json!("Pick a color");
    meta["pending_input"] = serde_json::json!({
        "seq": input["seq"],
        "prompt": "Pick a color",
    });
    meta["output"] = serde_json::Value::Null;
    fs::write(
        branch_store.join("branch.json"),
        serde_json::to_vec_pretty(&meta).unwrap(),
    )
    .unwrap();
    fs::write(
        branch_store.join("checkpoint.json"),
        serde_json::to_vec_pretty(&vec![http.clone()]).unwrap(),
    )
    .unwrap();

    let archive = dir.join("legacy-branch.chidori-run.tar.gz");
    let output = run_chidori(
        &[
            "checkpoint",
            "export",
            &run_id,
            "--output",
            archive.to_str().unwrap(),
        ],
        &dir,
    );
    assert_success(&output);
    let output = run_chidori(
        &[
            "checkpoint",
            "import",
            archive.to_str().unwrap(),
            "--dir",
            import_dir.to_str().unwrap(),
        ],
        &dir,
    );
    assert_success(&output);
    let imported_branch = import_dir
        .join(".chidori")
        .join("runs")
        .join(&run_id)
        .join("branches")
        .join(branch_store.parent().unwrap().file_name().unwrap())
        .join("branch-000");
    let imported_meta: serde_json::Value =
        serde_json::from_slice(&fs::read(imported_branch.join("branch.json")).unwrap()).unwrap();
    assert_eq!(imported_meta["sequence_range"]["start"], range_start);
    assert_eq!(imported_meta["sequence_range"]["end_exclusive"], range_end);
    assert_eq!(imported_meta["status"], "paused");
    assert_eq!(imported_meta["pending_input"]["seq"], input["seq"]);

    let output = run_chidori_with_str_env(
        &[
            "branch-resume",
            &run_id,
            branch_id,
            "--value",
            "blue",
            "--dir",
            import_dir.to_str().unwrap(),
            "--trusted",
        ],
        &import_dir,
        &env,
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "branch resume must replay the imported HTTP row before checking command status"
    );
    assert_success(&output);
    let resumed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(resumed["status"], "completed");
    assert_eq!(resumed["output"]["answer"], "blue");
    let after: Vec<serde_json::Value> =
        serde_json::from_slice(&fs::read(imported_branch.join("checkpoint.json")).unwrap())
            .unwrap();
    assert!(after.iter().all(|record| {
        let seq = record["seq"].as_u64().unwrap();
        seq >= range_start && seq < range_end
    }));
    drop(server_guard);
    fs::remove_dir_all(dir).ok();
    fs::remove_dir_all(import_dir).ok();
}

#[test]
fn cli_resume_rejects_truncated_final_effect_before_live_reexecution() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use std::thread;
    use std::time::Duration;

    let dir = temp_project("resume-truncated-final-effect");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let stopping = Arc::new(AtomicBool::new(false));
    let server_effects = Arc::clone(&effects);
    let server_stopping = Arc::clone(&stopping);
    let server = thread::spawn(move || {
        while !server_stopping.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut request = [0u8; 4096];
                    let _ = stream.read(&mut request);
                    server_effects.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("local effect server failed: {error}"),
            }
        }
    });

    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
                export async function agent() {{
                    await fetch("http://{address}/effect/1", {{ method: "POST" }});
                    await fetch("http://{address}/effect/2", {{ method: "POST" }});
                    await fetch("http://{address}/effect/3", {{ method: "POST" }});
                    return {{ completed: true }};
                }}
            "#
        ),
    )
    .unwrap();

    let http_env = [("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1")];
    let output = run_chidori_with_str_env(
        &["run", agent.to_str().unwrap(), "--trusted"],
        &dir,
        &http_env,
    );
    assert_success(&output);
    assert_eq!(
        effects.load(Ordering::SeqCst),
        3,
        "fresh run should perform exactly three live effects"
    );

    let run_id = first_run_id(&dir);
    let records_path = dir
        .join(".chidori")
        .join("runs")
        .join(&run_id)
        .join("records.jsonl");
    let mut journal = fs::read(&records_path).unwrap();
    let rows: Vec<serde_json::Value> = journal
        .split(|byte| *byte == b'\n')
        .filter(|row| !row.is_empty())
        .map(|row| serde_json::from_slice(row).unwrap())
        .collect();
    assert_eq!(rows.len(), 3, "expected one journal row per HTTP effect");
    assert_eq!(
        rows[2]["function"], "http",
        "the final row must represent the third effect"
    );
    assert!(
        journal.len() > 28,
        "final row must be longer than the truncation length"
    );
    journal.truncate(journal.len() - 28);
    fs::write(&records_path, journal).unwrap();

    let output = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--trusted"],
        &dir,
        &http_env,
    );
    stopping.store(true, Ordering::Relaxed);
    server.join().unwrap();

    assert_eq!(
        effects.load(Ordering::SeqCst),
        3,
        "resume must reject the damaged final row before executing the third live effect again"
    );
    assert_failure(&output);
    assert_journal_location(&output, &records_path, 3);
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.to_lowercase().contains("corrupt"),
        "expected a corruption diagnostic, got:\n{diagnostic}"
    );
    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_resume_uses_resolved_http_promise_when_journal_row_is_missing() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture_with_mirror(&server, true);
    assert_eq!(server.request_count(), 1);
    let promise = persist_resolved_http_operation(&fixture);
    assert_eq!(promise["operation"]["seq"], 2);
    assert_eq!(promise["operation"]["kind"], "http");
    assert_eq!(promise["operation"]["args"], fixture.http_record["args"]);
    assert_eq!(
        promise["state"]["resolved"]["value"],
        fixture.http_record["result"]
    );
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[]);
    assert_eq!(
        server.request_count(),
        1,
        "resume must serve the persisted resolved promise without a second HTTP request"
    );
    let recovered = journal_records(&fixture.records_path);
    let recovered_http = recovered
        .iter()
        .find(|record| record["seq"] == 2 && record["function"] == "http")
        .expect("resume should restore the missing HTTP journal row");
    assert_eq!(
        recovered_http["result"],
        promise["state"]["resolved"]["value"]
    );
    assert_success(&output);
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_hydrates_resolved_http_promise_from_sqlite_mirror() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture_with_mirror(&server, true);
    assert_eq!(server.request_count(), 1);
    let promise = persist_resolved_http_operation(&fixture);
    assert_eq!(promise["operation"]["seq"], 2);
    assert_eq!(promise["operation"]["kind"], "http");
    assert_eq!(promise["operation"]["args"], fixture.http_record["args"]);
    assert_eq!(
        promise["state"]["resolved"]["value"],
        fixture.http_record["result"]
    );

    remove_http_journal_row(&fixture);
    let mirror = fixture_run_store(&fixture);
    let mirrored_journal = mirror.load_call_log().unwrap().unwrap();
    assert_eq!(mirrored_journal.len(), 1);
    assert_eq!(mirrored_journal[0].seq, 1);
    assert!(mirror.get_blob("host_promises/2.json").unwrap().is_some());

    fs::remove_dir_all(&fixture.run_dir).unwrap();
    assert!(
        !fixture.run_dir.exists(),
        "resume must begin without a local run directory"
    );
    let db_path = fixture
        .mirror_db
        .as_ref()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let output = resume_http_fixture(
        &fixture,
        &[
            ("CHIDORI_RUN_STORE", "sqlite"),
            ("CHIDORI_RUN_DB", &db_path),
        ],
    );

    // Count external contact before inspecting recovery output: a resumed
    // request would be the duplicate side effect this mirror case guards.
    assert_eq!(
        server.request_count(),
        1,
        "mirror hydration must serve the resolved operation without a second HTTP request"
    );
    assert_success(&output);
    let recovered = journal_records(&fixture.records_path);
    let recovered_http = recovered
        .iter()
        .find(|record| record["seq"] == 2 && record["function"] == "http")
        .expect("hydrated resume should restore the missing HTTP journal row");
    assert_eq!(
        recovered_http["result"],
        promise["state"]["resolved"]["value"]
    );
}

#[test]
fn cli_resume_refuses_held_sqlite_tenure_before_effect_or_journal_mutation() {
    use chidori::runtime::store::acquire_lease_with_token;

    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture_with_mirror(&server, true);
    assert_eq!(server.request_count(), 1);

    // Leave a real completed run with its HTTP row absent and its resolved
    // promise removed, so resume would perform the HTTP request live unless
    // the external SQLite tenure is honored first. Remove the cached promise
    // from both the filesystem primary and SQLite mirror so the live effect
    // is the only possible continuation if the lease is bypassed.
    let store = fixture_run_store(&fixture);
    let mut records = store.load_call_log().unwrap().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].function, "http");
    records.pop();
    store.write_call_log(&records).unwrap();
    store.delete_blob("host_promises/2.json").unwrap();
    store.delete_blob("host_promises.json").unwrap();
    let journal_before = fs::read(&fixture.records_path).unwrap();

    let held = acquire_lease_with_token(
        store.as_ref(),
        "external-sqlite-holder",
        chrono::Duration::minutes(10),
    )
    .unwrap()
    .expect("the fixture run lease must be free before this test holds it");
    let lease_before = store.get_blob("lease.json").unwrap().unwrap();
    let db_path = fixture
        .mirror_db
        .as_ref()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let output = resume_http_fixture(
        &fixture,
        &[
            ("CHIDORI_RUN_STORE", "sqlite"),
            ("CHIDORI_RUN_DB", &db_path),
        ],
    );
    // Capture every oracle before asserting, so a recovered journal row or a
    // successful status cannot hide another sign of the lease bypass.
    let request_count_after = server.request_count();
    let process_status_after = output.status;
    let stdout_after = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr_after = String::from_utf8_lossy(&output.stderr).into_owned();
    let diagnostic_after = format!("{stdout_after}\n{stderr_after}");
    let journal_after = fs::read(&fixture.records_path).unwrap();
    let lease_after = store.get_blob("lease.json").unwrap().unwrap();
    let contender_result = acquire_lease_with_token(
        store.as_ref(),
        "verification-contender",
        chrono::Duration::minutes(1),
    )
    .unwrap();
    let contender_blocked = match contender_result {
        Err(_) => true,
        Ok(contender_lease) => {
            chidori::runtime::store::release_lease_with_token(
                store.as_ref(),
                contender_lease.lease(),
                contender_lease.token(),
            )
            .unwrap();
            false
        }
    };

    // Release before the combined assertion so even the intended Red leaves
    // no externally held tenure behind during fixture cleanup.
    chidori::runtime::store::release_lease_with_token(store.as_ref(), held.lease(), held.token())
        .unwrap();

    let mut observed = Vec::new();
    if request_count_after != 1 {
        observed.push(format!(
            "HTTP request count was {request_count_after}, expected 1"
        ));
    }
    if process_status_after.success() {
        observed.push(format!(
            "resume continued instead of rejecting the held tenure (exit status: {process_status_after})"
        ));
    }
    if !diagnostic_after.contains("external-sqlite-holder") {
        observed.push(
            "resume output did not identify the external held-lease owner; this may be an unrelated CLI failure"
                .to_string(),
        );
    }
    if journal_after != journal_before {
        observed.push("local journal bytes changed".to_string());
    }
    if lease_after != lease_before {
        observed.push("external lease bytes changed".to_string());
    }
    if !contender_blocked {
        observed.push("a contender acquired while the external tenure was held".to_string());
    }
    assert!(
        observed.is_empty(),
        "held SQLite tenure must reject resume with the holder identified before effects or mutations: {}; status={process_status_after}; stdout:\n{stdout_after}\nstderr:\n{stderr_after}",
        observed.join("; ")
    );
    // Once the external tenure is released, the same command must resume the
    // run successfully. Its own fenced lease must then be available to a new
    // contender, proving the complete CLI leg released the token it acquired.
    let retry = resume_http_fixture(
        &fixture,
        &[
            ("CHIDORI_RUN_STORE", "sqlite"),
            ("CHIDORI_RUN_DB", &db_path),
        ],
    );
    assert_eq!(server.request_count(), 2);
    assert_success(&retry);
    assert!(journal_records(&fixture.records_path)
        .iter()
        .any(|record| record["seq"] == 2 && record["function"] == "http"));
    let post_retry = acquire_lease_with_token(
        store.as_ref(),
        "post-resume-contender",
        chrono::Duration::minutes(1),
    )
    .unwrap()
    .expect("CLI resume must release its own SQLite tenure after the leg");
    chidori::runtime::store::release_lease_with_token(
        store.as_ref(),
        post_retry.lease(),
        post_retry.token(),
    )
    .unwrap();
}

#[test]
fn cli_resume_refuses_competing_filesystem_lease() {
    use chidori::runtime::store::{acquire_lease, release_lease};

    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let store = fixture_run_store(&fixture);
    let mut records = store.load_call_log().unwrap().unwrap();
    assert_eq!(records.len(), 2);
    records.pop();
    store.write_call_log(&records).unwrap();
    store.delete_blob("host_promises/2.json").unwrap();
    store.delete_blob("host_promises.json").unwrap();
    let journal_before = fs::read(&fixture.records_path).unwrap();
    let held = acquire_lease(
        store.as_ref(),
        "external-fs-holder",
        chrono::Duration::minutes(10),
    )
    .unwrap()
    .expect("the filesystem lease must be acquirable");
    let lease_before = store.get_blob("lease.json").unwrap().unwrap();

    let output = Command::new(chidori_bin())
        .args([
            "resume",
            fixture.agent.to_str().unwrap(),
            &fixture.run_id,
            "--trusted",
        ])
        .current_dir(&fixture.dir)
        .env_remove("CHIDORI_RUN_STORE")
        .env_remove("CHIDORI_RUN_DB")
        .env("CHIDORI_DURABILITY", "effect")
        .env("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1")
        .output()
        .unwrap();

    let request_count_after = server.request_count();
    let process_status_after = output.status;
    let diagnostic_after = output_diagnostic(&output);
    let journal_after = fs::read(&fixture.records_path).unwrap();
    let lease_after = store.get_blob("lease.json").unwrap().unwrap();

    // Release before checking the Red/control assertions so a failure cannot
    // leave a known filesystem lease behind in this temporary run.
    release_lease(store.as_ref(), &held.owner).unwrap();

    assert_eq!(
        request_count_after, 1,
        "a sequential contender must not execute the unjournaled HTTP effect; stderr/stdout:\n{diagnostic_after}"
    );
    assert!(
        !process_status_after.success(),
        "resume must refuse the known unexpired filesystem holder; output:\n{diagnostic_after}"
    );
    assert!(
        diagnostic_after.contains("external-fs-holder"),
        "filesystem control should identify the competing holder; output:\n{diagnostic_after}"
    );
    assert_eq!(journal_after, journal_before);
    assert_eq!(lease_after, lease_before);
    assert_eq!(held.owner, "external-fs-holder");
    assert_eq!(
        store.load_call_log().unwrap().unwrap().len(),
        1,
        "refused filesystem resume must not repair the removed HTTP row"
    );

    let retry = resume_http_fixture(&fixture, &[]);
    assert_eq!(server.request_count(), 2);
    assert_success(&retry);
    assert!(journal_records(&fixture.records_path)
        .iter()
        .any(|record| record["seq"] == 2 && record["function"] == "http"));
    let reacquired = acquire_lease(
        store.as_ref(),
        "post-filesystem-resume-contender",
        chrono::Duration::minutes(1),
    )
    .unwrap()
    .expect("the sequential filesystem resume leg must release its lease");
    release_lease(store.as_ref(), &reacquired.owner).unwrap();
}

#[test]
fn cli_resume_prefers_intact_http_journal_over_promise_fallback() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let mut promise = persist_resolved_http_operation(&fixture);
    // Deliberately conflict with the intact journal result. Journal replay is
    // authoritative whenever the ordinary record is present.
    promise["state"]["resolved"]["value"] = serde_json::json!({"ok": false});
    store_host_promise_blob(&fixture, 2, &promise);

    let output = resume_http_fixture(&fixture, &[]);
    assert_eq!(
        server.request_count(),
        1,
        "intact journal replay must not contact HTTP again"
    );
    assert_success(&output);
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result, serde_json::json!({"ok": true}));
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_mismatched_http_promise_args_even_in_replay_lax_mode() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let mut promise = persist_resolved_http_operation(&fixture);
    promise["operation"]["args"]["url"] =
        serde_json::json!("http://127.0.0.1:1/different-operation");
    store_host_promise_blob(&fixture, 2, &promise);
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "argument mismatch must not reissue the HTTP effect, even in lax mode"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, "Replay divergence at seq 2");
    assert_diagnostic_contains(
        &output,
        "does not permit re-execution after a durable completion",
    );
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_mismatched_http_promise_kind_before_live_contact() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let mut promise = persist_resolved_http_operation(&fixture);
    promise["operation"]["kind"] = serde_json::json!("tool");
    store_host_promise_blob(&fixture, 2, &promise);
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "kind mismatch must not reissue the HTTP effect"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, "inconsistent with function `http`");
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_ambiguous_duplicate_http_completions_before_live_contact() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let promise = persist_resolved_http_operation(&fixture);
    let mut duplicate = promise.clone();
    duplicate["operation"]["id"] = serde_json::json!(3);
    duplicate["state"]["resolved"]["value"] = serde_json::json!({"ok": "conflicting"});
    store_host_promise_blob(&fixture, 3, &duplicate);
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "ambiguous completions must not trigger a live HTTP effect"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, "multiple operations at seq 2");
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_dangling_journal_reference_before_live_contact() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let mut promise = persist_resolved_http_operation(&fixture);
    let completed_at = promise["state"]["resolved"]["completed_at"].clone();
    promise["state"] = serde_json::json!({
        "resolved_in_journal": { "completed_at": completed_at }
    });
    let _ = fs::remove_file(fixture.run_dir.join("host_promises").join("2.json"));
    fs::write(
        fixture.run_dir.join("host_promises.json"),
        serde_json::to_vec_pretty(&vec![promise]).unwrap(),
    )
    .unwrap();
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "dangling completed reference must fail before live contact"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, "journal has no clean record at that seq");
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_malformed_http_promise_blob_before_live_contact() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let _ = persist_resolved_http_operation(&fixture);
    fs::write(
        fixture.run_dir.join("host_promises").join("2.json"),
        b"{malformed-json",
    )
    .unwrap();
    fs::write(fixture.run_dir.join("host_promises.json"), b"[]").unwrap();
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "malformed completion must fail before live contact"
    );
    assert_failure(&output);
    let diagnostic = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.contains("parsing durable host-promise record host_promises/2.json"),
        "malformed host-promise diagnostic must identify the parsing context and blob key; output:\n{diagnostic}"
    );
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_resolved_http_promise_missing_value_before_live_contact() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let mut promise = persist_resolved_http_operation(&fixture);
    promise["state"]["resolved"]
        .as_object_mut()
        .unwrap()
        .remove("value");
    store_host_promise_blob(&fixture, 2, &promise);
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "malformed resolved value must fail before another HTTP contact"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, "host_promises/2.json");
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_resolved_http_promise_missing_function_before_live_contact() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let mut promise = persist_resolved_http_operation(&fixture);
    promise["operation"]
        .as_object_mut()
        .unwrap()
        .remove("function");
    store_host_promise_blob(&fixture, 2, &promise);
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "completed promise without function identity must fail before another HTTP contact"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, "no function identity");
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_rejects_resolved_http_promise_wrong_function_before_live_contact() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let mut promise = persist_resolved_http_operation(&fixture);
    promise["operation"]["function"] = serde_json::json!("wrong-http-function");
    store_host_promise_blob(&fixture, 2, &promise);
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[("CHIDORI_REPLAY_LAX", "1")]);
    assert_eq!(
        server.request_count(),
        1,
        "completed promise with the wrong function must fail before another HTTP contact"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, "function `wrong-http-function`");
    fs::remove_dir_all(&fixture.dir).ok();
}

#[test]
fn cli_resume_does_not_treat_pending_http_operation_as_cached_success() {
    let server = LocalHttpServer::start();
    let fixture = create_http_replay_fixture(&server);
    let pending = persist_pending_http_operation(&fixture);
    assert_eq!(pending["state"], serde_json::json!("pending"));
    remove_http_journal_row(&fixture);

    let output = resume_http_fixture(&fixture, &[]);
    assert_eq!(
        server.request_count(),
        2,
        "pending intent must execute the unfinished HTTP operation live"
    );
    let recovered = journal_records(&fixture.records_path);
    assert!(recovered
        .iter()
        .any(|record| record["seq"] == 2 && record["function"] == "http"));
    assert_success(&output);
    fs::remove_dir_all(&fixture.dir).ok();
}

fn run_chidori_with_stdin(
    args: &[&str],
    cwd: &Path,
    envs: &[(&str, &str)],
    stdin_text: &str,
) -> Output {
    use std::io::Write as _;
    use std::process::Stdio;

    let mut command = Command::new(chidori_bin());
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(stdin_text.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

// `--stream` must run under the same posture as the plain `run` path: the
// agent's directory is the implicit workspace root, and the run journals under
// `.chidori/runs/<run_id>` — streaming changes progress reporting, not what
// the runtime can do or what survives.
#[test]
fn cli_stream_matches_plain_run_posture() {
    let dir = temp_project("stream-parity");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.workspace.write("out.txt", "streamed", { language: "text" });
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(
        &[
            "run",
            agent.to_str().unwrap(),
            "--trusted",
            "--stream",
            "--input",
            r#"{}"#,
        ],
        &dir,
    );
    assert_success(&output);
    let events: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let done = events
        .iter()
        .find(|event| event["type"] == "done")
        .expect("stream ended without a done event");
    assert_eq!(done["status"], "completed");
    let run_id = done["run_id"].as_str().expect("done event carries run_id");

    // Workspace root defaulted to the agent's directory, no env var required.
    assert_eq!(fs::read_to_string(dir.join("out.txt")).unwrap(), "streamed");

    // The run journaled like a plain run: trace can read it back.
    assert!(dir.join(".chidori").join("runs").join(run_id).exists());
    assert_success(&run_chidori(&["trace", run_id], &dir));

    fs::remove_dir_all(dir).ok();
}

// Plain `chidori run` prints a one-line sign of life per live prompt call on
// stderr (`seq N: prompt started (<model>)`, then a matching `prompt
// finished` line) — long model calls are otherwise total silence until the
// run ends, with `--stream` as the only progress path. Stdout stays reserved
// for the agent's output, and CHIDORI_QUIET=1 restores the old silent stderr.
#[test]
fn cli_plain_run_reports_prompt_progress_on_stderr() {
    let dir = temp_project("prompt-progress");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                const text = await chidori.prompt("say hi");
                return { text };
            }
        "#,
    )
    .unwrap();

    let envs = [("CHIDORI_TEST_LLM_RESPONSE", "hi back")];
    let output = run_chidori_with_str_env(&["run", agent.to_str().unwrap()], &dir, &envs);
    assert_success(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("prompt started ("),
        "plain run should print a prompt progress line on stderr, got:\n{stderr}"
    );
    // Progress must not leak into stdout: the agent's JSON output is still
    // the only thing there, byte-parseable as before.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("prompt started"),
        "progress lines must stay on stderr, got stdout:\n{stdout}"
    );
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(parsed["text"], "hi back");

    // CHIDORI_QUIET=1 suppresses the progress lines entirely.
    let quiet_envs = [
        ("CHIDORI_TEST_LLM_RESPONSE", "hi back"),
        ("CHIDORI_QUIET", "1"),
    ];
    let output = run_chidori_with_str_env(&["run", agent.to_str().unwrap()], &dir, &quiet_envs);
    assert_success(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("prompt started"),
        "CHIDORI_QUIET=1 should suppress progress lines, got:\n{stderr}"
    );

    fs::remove_dir_all(dir).ok();
}

/// Run chidori with every LLM provider hook removed (and HOME pointed at the
/// empty project dir so saved `model-login` credentials can't register a
/// provider either). Used to force a deterministic provider failure — and to
/// prove `verify` needs no provider.
fn run_chidori_without_providers(args: &[&str], cwd: &Path) -> Output {
    run_chidori_without_providers_with_env(args, cwd, &[])
}

fn run_chidori_without_providers_with_env(
    args: &[&str],
    cwd: &Path,
    envs: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(chidori_bin());
    command.args(args).current_dir(cwd).env("HOME", cwd);
    for key in [
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "CHIDORI_OPENAI_COMPAT_URL",
        "CHIDORI_OPENAI_COMPAT_KEY",
        "LITELLM_API_URL",
        "LITELLM_API_KEY",
        "OPENROUTER_API_KEY",
        "CHIDORI_TEST_LLM_RESPONSE",
    ] {
        command.env_remove(key);
    }
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().unwrap()
}

fn first_run_id(dir: &Path) -> String {
    fs::read_dir(dir.join(".chidori").join("runs"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap()
}

// `chidori resume --retry-failed` — first-class repair for a failed run:
// strip the trailing failed record(s) from the journal, replay the intact
// prefix from cache, re-execute the failure live. The repaired journal must
// then hold up to `chidori verify` — the whole point over hand-computed
// `--until-seq` surgery.
#[test]
fn cli_resume_retry_failed_repairs_failed_run_and_verify_passes() {
    let server = LocalHttpServer::start();
    let dir = temp_project("retry-failed");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
            export async function agent(input, chidori) {{
                await chidori.log("start", {{}});
                const response = await fetch("http://{}/resolved-prefix");
                const effect = await response.json();
                const text = await chidori.prompt("say hi");
                return {{ effect, text }};
            }}
        "#,
            server.address
        ),
    )
    .unwrap();

    // Force the failure: no provider configured, so the prompt call fails
    // and journals as the run's trailing failed record. The HTTP result in
    // the prefix is already resolved and must not be contacted again by retry.
    let run_env = [
        ("CHIDORI_DURABILITY", "effect"),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    let output = run_chidori_without_providers_with_env(
        &["run", agent.to_str().unwrap(), "--trusted"],
        &dir,
        &run_env,
    );
    assert_failure(&output);
    assert_eq!(
        server.request_count(),
        1,
        "initial HTTP effect must run once"
    );
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let promise_path = run_dir.join("host_promises.json");
    let promises: serde_json::Value =
        serde_json::from_slice(&fs::read(&promise_path).unwrap()).unwrap();
    let rejected_prompt = promises
        .as_array()
        .unwrap()
        .iter()
        .find(|promise| promise["operation"]["kind"] == "prompt")
        .expect("failed prompt must have a durable host-promise record");
    assert!(rejected_prompt["state"].get("rejected").is_some());
    let journal_before = journal_records(&run_dir.join("records.jsonl"));
    assert!(journal_before.last().unwrap()["error"].is_string());
    let prefix_before = journal_before[..2].to_vec();
    assert_eq!(prefix_before[1]["function"], "http");
    let resolved_http_before = promises
        .as_array()
        .unwrap()
        .iter()
        .find(|promise| promise["operation"]["kind"] == "http")
        .expect("the completed HTTP prefix must have a durable promise record")
        .clone();
    assert!(resolved_http_before["state"]
        .get("resolved_in_journal")
        .is_some());

    // A plain resume would replay the recorded failure. --retry-failed
    // strips it and re-executes the prompt live against the now-working
    // provider — no --allow-source-change needed for the retried tail.
    let output = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--retry-failed"],
        &dir,
        &[
            ("CHIDORI_TEST_LLM_RESPONSE", "repaired"),
            ("CHIDORI_DURABILITY", "effect"),
            ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
        ],
    );
    // Check the external effect and preserved promise before assert_success:
    // on vulnerable main, replay incorrectly returns the old Rejected prompt.
    let request_count_after_retry = server.request_count();
    let journal_after_retry = journal_records(&run_dir.join("records.jsonl"));
    let promises_after_retry: serde_json::Value =
        serde_json::from_slice(&fs::read(&promise_path).unwrap()).unwrap();
    let resolved_http_after = promises_after_retry
        .as_array()
        .unwrap()
        .iter()
        .find(|promise| promise["operation"]["kind"] == "http")
        .expect("retry must retain the completed HTTP promise");
    assert_eq!(
        request_count_after_retry, 1,
        "retry must not repeat an already resolved prefix HTTP effect"
    );
    assert_eq!(
        journal_after_retry[..2],
        prefix_before,
        "retry must preserve the completed prefix records"
    );
    assert_eq!(
        resolved_http_after, &resolved_http_before,
        "retry must preserve the unrelated resolved HTTP promise"
    );
    assert_success(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("retry-failed: stripped 1 failed record(s)"),
        "expected the stripped-count line on stderr, got:\n{stderr}"
    );
    assert!(
        stderr.contains("replaying 2 records then executing live"),
        "expected the replay/live split in the stripped line, got:\n{stderr}"
    );
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["text"], "repaired");
    assert_eq!(stdout["effect"]["ok"], true);
    let promises: serde_json::Value =
        serde_json::from_slice(&fs::read(&promise_path).unwrap()).unwrap();
    let repaired_prompt = promises
        .as_array()
        .unwrap()
        .iter()
        .find(|promise| promise["operation"]["kind"] == "prompt")
        .expect("retried prompt must retain its durable host-promise record");
    assert!(
        repaired_prompt["state"]
            .get("resolved_in_journal")
            .is_some(),
        "retry must replace the old rejected completion with the repaired durable result"
    );

    // The repaired run is a coherent recorded run: verify replays it to the
    // identical output with NO provider configured.
    let output = run_chidori_without_providers(&["verify", agent.to_str().unwrap(), &run_id], &dir);
    assert_success(&output);
    assert_eq!(
        server.request_count(),
        1,
        "verify must replay the HTTP prefix"
    );

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_resume_retry_failed_held_sqlite_tenure_preserves_journal_and_promise_bytes() {
    use chidori::runtime::store::{
        acquire_lease_with_token, release_lease_with_token, RunStore, SqliteRunStore,
        SqliteRunStoreShared,
    };

    let dir = temp_project("retry-failed-held-sqlite-lease");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("start", {});
                const text = await chidori.prompt("say hi");
                return { text };
            }
        "#,
    )
    .unwrap();
    let db_path = dir.join("runs.sqlite3");
    let db_path_text = db_path.to_string_lossy().to_string();
    let sqlite_env = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_path_text.as_str()),
    ];
    let failed = run_chidori_without_providers_with_env(
        &["run", agent.to_str().unwrap()],
        &dir,
        &sqlite_env,
    );
    assert_failure(&failed);

    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let journal_path = run_dir.join("records.jsonl");
    let table_path = run_dir.join("host_promises.json");
    let table_bytes_before = fs::read(&table_path).unwrap();
    let table: serde_json::Value = serde_json::from_slice(&table_bytes_before).unwrap();
    let rejected = table
        .as_array()
        .unwrap()
        .iter()
        .find(|record| {
            record["operation"]["kind"] == "prompt" && record["state"].get("rejected").is_some()
        })
        .expect("failed prompt must have a rejected durable promise");
    let promise_id = rejected["operation"]["id"]
        .as_u64()
        .expect("host promise id is numeric");
    let event_path = run_dir
        .join("host_promises")
        .join(format!("{promise_id}.json"));
    // The normal settled-run compaction has removed per-op blobs. Recreate
    // the matching rejected event to model the persisted pre-compaction
    // frontier that retry-failed is responsible for retiring.
    let event_bytes_before = serde_json::to_vec_pretty(rejected).unwrap();
    fs::create_dir_all(event_path.parent().unwrap()).unwrap();
    fs::write(&event_path, &event_bytes_before).unwrap();
    let journal_bytes_before = fs::read(&journal_path).unwrap();

    let shared = SqliteRunStoreShared::open(&db_path).unwrap();
    let sqlite = SqliteRunStore::new(shared, run_id.clone());
    let event_key = format!("host_promises/{promise_id}.json");
    sqlite.put_blob(&event_key, &event_bytes_before).unwrap();
    let sqlite_journal_before = serde_json::to_vec(
        &sqlite
            .load_call_log()
            .unwrap()
            .expect("SQLite mirror must contain the failed journal"),
    )
    .unwrap();
    let sqlite_table_before = sqlite
        .get_blob("host_promises.json")
        .unwrap()
        .expect("SQLite mirror must contain the promise table");
    let sqlite_event_before = sqlite
        .get_blob(&event_key)
        .unwrap()
        .expect("SQLite mirror must contain the per-operation promise event");
    let held = acquire_lease_with_token(
        &sqlite,
        "external-retry-holder",
        chrono::Duration::minutes(10),
    )
    .unwrap()
    .expect("the failed run must be free before acquiring the test tenure");

    let refused = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--retry-failed"],
        &dir,
        &[
            ("CHIDORI_RUN_STORE", "sqlite"),
            ("CHIDORI_RUN_DB", db_path_text.as_str()),
            ("CHIDORI_TEST_LLM_RESPONSE", "repaired"),
        ],
    );
    let refused_journal = fs::read(&journal_path).unwrap();
    let refused_table = fs::read(&table_path).unwrap();
    let refused_event = fs::read(&event_path).unwrap();
    let refused_sqlite_journal = serde_json::to_vec(
        &sqlite
            .load_call_log()
            .unwrap()
            .expect("held retry must retain SQLite journal"),
    )
    .unwrap();
    let refused_sqlite_table = sqlite.get_blob("host_promises.json").unwrap().unwrap();
    let refused_sqlite_event = sqlite.get_blob(&event_key).unwrap().unwrap();

    // Release the exact external tenure before assertions or fixture cleanup.
    release_lease_with_token(&sqlite, held.lease(), held.token()).unwrap();

    assert_failure(&refused);
    let diagnostic = String::from_utf8_lossy(&refused.stderr);
    assert!(
        diagnostic.contains("external-retry-holder"),
        "held retry should identify the SQLite lease owner, got:\n{diagnostic}"
    );
    assert_eq!(
        refused_journal, journal_bytes_before,
        "refusal changed local journal bytes"
    );
    assert_eq!(
        refused_table, table_bytes_before,
        "refusal changed local promise-table bytes"
    );
    assert_eq!(
        refused_event, event_bytes_before,
        "refusal changed local promise-event bytes"
    );
    assert_eq!(
        refused_sqlite_journal, sqlite_journal_before,
        "refusal changed SQLite journal records"
    );
    assert_eq!(
        refused_sqlite_table, sqlite_table_before,
        "refusal changed SQLite promise-table bytes"
    );
    assert_eq!(
        refused_sqlite_event, sqlite_event_before,
        "refusal changed SQLite promise-event bytes"
    );

    let retry = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--retry-failed"],
        &dir,
        &[
            ("CHIDORI_RUN_STORE", "sqlite"),
            ("CHIDORI_RUN_DB", db_path_text.as_str()),
            ("CHIDORI_TEST_LLM_RESPONSE", "repaired"),
        ],
    );
    assert_success(&retry);
    let output: serde_json::Value = serde_json::from_slice(&retry.stdout).unwrap();
    assert_eq!(output["text"], "repaired");
    let retried_journal = sqlite.load_call_log().unwrap().unwrap();
    assert_eq!(retried_journal.last().unwrap().function, "prompt");
    assert!(retried_journal.last().unwrap().error.is_none());
    let retried_table: serde_json::Value =
        serde_json::from_slice(&sqlite.get_blob("host_promises.json").unwrap().unwrap()).unwrap();
    let retried_prompt = retried_table
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["operation"]["kind"] == "prompt")
        .expect("retry must retain its prompt completion in SQLite");
    assert!(retried_prompt["state"].get("resolved_in_journal").is_some());

    fs::remove_dir_all(dir).ok();
}

// Refusal control: prefix-only state with an old Rejected completion has no
// durable retry-intent marker, so retry-failed must not infer authorization.
#[test]
fn cli_resume_retry_failed_refuses_unmarked_prefix_with_rejected_promise() {
    let dir = temp_project("retry-failed-cut-before-pending");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("start", {});
                const text = await chidori.prompt("say hi");
                return { text };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_without_providers(&["run", agent.to_str().unwrap()], &dir);
    assert_failure(&output);
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let records = journal_records(&records_path);
    assert_eq!(
        records.len(),
        2,
        "fixture must have one prefix and one failed prompt"
    );
    assert_eq!(records[1]["function"], "prompt");
    assert!(records[1]["error"].is_string());

    // Construct prefix-only state without an authorization marker, while the
    // per-operation blob still says Rejected. This is a refusal control.
    let prefix = [serde_json::to_vec(&records[0]).unwrap(), b"\n".to_vec()].concat();
    fs::write(&records_path, &prefix).unwrap();
    let promise_path = run_dir.join("host_promises.json");
    let promise_bytes_before = fs::read(&promise_path).unwrap();
    let promises: serde_json::Value = serde_json::from_slice(&promise_bytes_before).unwrap();
    assert!(promises[1]["state"].get("rejected").is_some());

    let output = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--retry-failed"],
        &dir,
        &[("CHIDORI_TEST_LLM_RESPONSE", "recovered")],
    );
    assert_failure(&output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("not in a failed state"));
    assert!(
        output.stdout.is_empty(),
        "unmarked retry must not execute the prompt"
    );
    assert_eq!(fs::read(&records_path).unwrap(), prefix);
    assert_eq!(fs::read(&promise_path).unwrap(), promise_bytes_before);

    fs::remove_dir_all(dir).ok();
}

// Refusal control: Pending alone is not an authorization marker either. This
// models the durable promise replacement without a recorded retry intent.
#[test]
fn cli_resume_retry_failed_refuses_unmarked_prefix_with_pending_promise() {
    let dir = temp_project("retry-failed-cut-pending-before-dispatch");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("start", {});
                const text = await chidori.prompt("say hi");
                return { text };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_without_providers(&["run", agent.to_str().unwrap()], &dir);
    assert_failure(&output);
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let records = journal_records(&records_path);
    assert_eq!(records.len(), 2);
    let prefix = [serde_json::to_vec(&records[0]).unwrap(), b"\n".to_vec()].concat();
    fs::write(&records_path, &prefix).unwrap();

    let promise_path = run_dir.join("host_promises.json");
    let mut promises: serde_json::Value =
        serde_json::from_slice(&fs::read(&promise_path).unwrap()).unwrap();
    promises[1]["state"] = serde_json::json!("pending");
    fs::write(&promise_path, serde_json::to_vec_pretty(&promises).unwrap()).unwrap();
    let promise_bytes_before = fs::read(&promise_path).unwrap();

    let output = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--retry-failed"],
        &dir,
        &[("CHIDORI_TEST_LLM_RESPONSE", "must-not-dispatch")],
    );
    assert_failure(&output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("not in a failed state"));
    assert!(
        output.stdout.is_empty(),
        "unmarked retry must not dispatch the prompt"
    );
    assert_eq!(fs::read(&records_path).unwrap(), prefix);
    assert_eq!(fs::read(&promise_path).unwrap(), promise_bytes_before);

    fs::remove_dir_all(dir).ok();
}

// Refusal control: a prefix-only journal with the failed operation's promise
// absent also lacks retry authorization and must not start live work.
#[test]
fn cli_resume_retry_failed_refuses_unmarked_prefix_without_promise() {
    let dir = temp_project("retry-failed-cut-prefix-without-promise");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("start", {});
                const text = await chidori.prompt("say hi");
                return { text };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_without_providers(&["run", agent.to_str().unwrap()], &dir);
    assert_failure(&output);
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let records = journal_records(&records_path);
    assert_eq!(records.len(), 2);
    let prefix = [serde_json::to_vec(&records[0]).unwrap(), b"\n".to_vec()].concat();
    fs::write(&records_path, &prefix).unwrap();

    let promise_path = run_dir.join("host_promises.json");
    let mut promises: serde_json::Value =
        serde_json::from_slice(&fs::read(&promise_path).unwrap()).unwrap();
    promises.as_array_mut().unwrap().pop();
    fs::write(&promise_path, serde_json::to_vec_pretty(&promises).unwrap()).unwrap();
    let promise_bytes_before = fs::read(&promise_path).unwrap();

    let output = run_chidori_with_str_env(
        &["resume", agent.to_str().unwrap(), &run_id, "--retry-failed"],
        &dir,
        &[("CHIDORI_TEST_LLM_RESPONSE", "must-not-dispatch")],
    );
    assert_failure(&output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("not in a failed state"));
    assert!(
        output.stdout.is_empty(),
        "unmarked retry must not dispatch the prompt"
    );
    assert_eq!(fs::read(&records_path).unwrap(), prefix);
    assert_eq!(fs::read(&promise_path).unwrap(), promise_bytes_before);

    fs::remove_dir_all(dir).ok();
}

// Crash cut: the provider result is durably resolved, but its journal append
// has not happened. A plain resume must consume that resolution without
// contacting a provider, then write the missing journal row.
#[test]
fn cli_resume_retry_failed_replays_resolved_result_before_journal_append() {
    let dir = temp_project("retry-failed-cut-resolved-before-journal");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("start", {});
                const text = await chidori.prompt("say hi");
                return { text };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_without_providers(&["run", agent.to_str().unwrap()], &dir);
    assert_failure(&output);
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let records = journal_records(&records_path);
    assert_eq!(records.len(), 2);
    fs::write(
        &records_path,
        [serde_json::to_vec(&records[0]).unwrap().as_slice(), b"\n"].concat(),
    )
    .unwrap();

    let promise_path = run_dir.join("host_promises.json");
    let mut promises: serde_json::Value =
        serde_json::from_slice(&fs::read(&promise_path).unwrap()).unwrap();
    promises[1]["state"] = serde_json::json!({
        "resolved": {
            "value": "resolved-before-journal",
            "completed_at": "2026-10-20T00:00:00Z"
        }
    });
    fs::write(&promise_path, serde_json::to_vec_pretty(&promises).unwrap()).unwrap();

    let output = run_chidori_without_providers(&["resume", agent.to_str().unwrap(), &run_id], &dir);
    assert_success(&output);
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["text"], "resolved-before-journal");
    let repaired = journal_records(&records_path);
    assert_eq!(repaired.len(), 2);
    assert_eq!(repaired[1]["result"], "resolved-before-journal");
    assert!(repaired[1]["error"].is_null());

    fs::remove_dir_all(dir).ok();
}

// --retry-failed on a run with nothing to retry must say what state the run
// is actually in and what to do instead.
#[test]
fn cli_resume_retry_failed_on_completed_run_errors_clearly() {
    let dir = temp_project("retry-failed-completed");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("fine", {});
                return { ok: true };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori(&["run", agent.to_str().unwrap()], &dir);
    assert_success(&output);
    let run_id = first_run_id(&dir);

    let output = run_chidori(
        &["resume", agent.to_str().unwrap(), &run_id, "--retry-failed"],
        &dir,
    );
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("completed") && stderr.contains("nothing to retry"),
        "expected a clear completed-run refusal naming the state, got:\n{stderr}"
    );

    fs::remove_dir_all(dir).ok();
}

// --retry-failed and --until-seq name different frontiers; combining them is
// ambiguous and must be rejected up front.
#[test]
fn cli_resume_retry_failed_rejects_until_seq_combination() {
    let dir = temp_project("retry-failed-conflict");
    let output = run_chidori(
        &[
            "resume",
            "agent.ts",
            "some-run",
            "--retry-failed",
            "--until-seq",
            "3",
        ],
        &dir,
    );
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--retry-failed") && stderr.contains("--until-seq"),
        "expected a flag-conflict error naming both flags, got:\n{stderr}"
    );

    fs::remove_dir_all(dir).ok();
}

// A chat session is a durable run: the session id is announced, every turn
// journals under `.chidori/runs/<session_id>` with `input.json` holding the
// dialogue state, and `--resume` replays the transcript and continues the
// same session in place.
#[test]
fn cli_chat_session_persists_and_resumes() {
    let dir = temp_project("chat-persist");
    let envs = [("CHIDORI_TEST_LLM_RESPONSE", "canned reply")];

    let output = run_chidori_with_stdin(&["chat"], &dir, &envs, "hello there\nexit\n");
    assert_success(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let session_id = stderr
        .lines()
        .find(|line| line.starts_with("session ") && !line.starts_with("session saved"))
        .and_then(|line| line.strip_prefix("session "))
        .map(|rest| rest.split_whitespace().next().unwrap().to_string())
        .expect("chat announces its session id");
    assert!(
        stderr.contains("session saved"),
        "exit should point at --resume/trace, got:\n{stderr}"
    );
    // The reply is marked like the `you> ` prompt, so scrollback shows who
    // said what instead of one undifferentiated text column.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("assistant> "),
        "reply should carry an assistant> marker, got:\n{stdout}"
    );
    // Exit prints a usage/cost summary before the "session saved" line,
    // computed from the journaled records the same way `chidori stats` is.
    let summary = stderr
        .lines()
        .find(|line| line.starts_with("session usage:"))
        .expect("exit should print a session usage summary");
    assert!(
        summary.contains("1 prompt call(s)") && summary.contains("est. cost:"),
        "summary should count prompt calls and estimate cost, got:\n{summary}"
    );
    assert!(
        stderr.find("session usage:").unwrap() < stderr.find("session saved").unwrap(),
        "summary should precede the session-saved line, got:\n{stderr}"
    );

    let run_dir = dir.join(".chidori").join("runs").join(&session_id);
    let input: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("input.json")).unwrap()).unwrap();
    assert_eq!(input["messages"], serde_json::json!(["hello there"]));

    // Resume: the restored transcript prints (prior turn replayed from the
    // journal), and the continued dialogue journals into the same run.
    let output = run_chidori_with_stdin(
        &["chat", "--resume", &session_id],
        &dir,
        &envs,
        "second message\nexit\n",
    );
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("hello there") && stdout.contains("canned reply"),
        "resume should print the restored transcript, got:\n{stdout}"
    );
    let input: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("input.json")).unwrap()).unwrap();
    assert_eq!(
        input["messages"],
        serde_json::json!(["hello there", "second message"])
    );

    // The session is an ordinary run to the rest of the toolchain.
    assert_success(&run_chidori(&["trace", &session_id], &dir));

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_chat_resume_refuses_held_sqlite_tenure_before_turn_mutation() {
    use chidori::runtime::store::{
        acquire_lease_with_token, RunStore, SqliteRunStore, SqliteRunStoreShared,
    };
    use std::io::Write as _;
    use std::process::Stdio;

    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().to_path_buf();
    let db_path = dir.join("chat-lease.sqlite3");
    let db_path_text = db_path.to_string_lossy().to_string();
    let initial_env = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_path_text.as_str()),
        ("CHIDORI_TEST_LLM_RESPONSE", "initial reply"),
    ];
    let initial = run_chidori_with_stdin(&["chat"], &dir, &initial_env, "first message\nexit\n");
    assert_success(&initial);
    let session_id = String::from_utf8_lossy(&initial.stderr)
        .lines()
        .find(|line| line.starts_with("session ") && !line.starts_with("session saved"))
        .and_then(|line| line.strip_prefix("session "))
        .and_then(|rest| rest.split_whitespace().next())
        .expect("initial chat announces the durable session id")
        .to_string();

    let server = GatedChatProvider::start();
    let mut interrupted = Command::new(chidori_bin())
        .args(["chat", "--resume", &session_id])
        .current_dir(&dir)
        .env_remove("CHIDORI_TEST_LLM_RESPONSE")
        .env_remove("CHIDORI_OPENAI_COMPAT_URL")
        .env_remove("CHIDORI_OPENAI_COMPAT_KEY")
        .env_remove("OPENAI_BASE_URL")
        .env_remove("ANTHROPIC_API_KEY")
        .env("OPENAI_API_KEY", "local-test-key")
        .env("OPENAI_BASE_URL", &server.base_url)
        .env("CHIDORI_RUN_STORE", "sqlite")
        .env("CHIDORI_RUN_DB", &db_path)
        .env("CHIDORI_DURABILITY", "strict")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    interrupted
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"second message\n")
        .unwrap();
    let interrupted_request = server
        .request_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("resumed chat starts its provider operation");
    assert!(String::from_utf8_lossy(&interrupted_request).contains("POST /v1/chat/completions"));
    interrupted.kill().unwrap();
    let _interrupted_output = interrupted.wait_with_output().unwrap();
    server.release();
    drop(server);
    let run_dir = dir.join(".chidori").join("runs").join(&session_id);
    let input_path = run_dir.join("input.json");

    let shared = SqliteRunStoreShared::open(&db_path).unwrap();
    let store: std::sync::Arc<dyn RunStore> =
        std::sync::Arc::new(SqliteRunStore::new(shared, session_id.clone()));
    assert!(store.load_call_log().unwrap().is_some());
    assert!(store.get_blob("input.json").unwrap().is_some());
    let pending_promise = store
        .list_blobs()
        .unwrap()
        .into_iter()
        .filter(|key| key.starts_with("host_promises/"))
        .filter_map(|key| store.get_blob(&key).unwrap())
        .any(|bytes| {
            serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .is_some_and(|record| record["state"] == "pending")
        });
    assert!(
        pending_promise,
        "fixture must be a real interrupted SQLite chat run"
    );

    // The killed resumed-chat process correctly leaves its SQLite lease in
    // place. Expire that dead owner's tenure before this fixture acquires the
    // next external lease; a process killed with SIGKILL cannot run its Drop.
    let mut interrupted_lease: serde_json::Value = serde_json::from_slice(
        &store
            .get_blob("lease.json")
            .unwrap()
            .expect("chat lease is durable"),
    )
    .unwrap();
    assert!(interrupted_lease["owner"]
        .as_str()
        .is_some_and(|owner| owner.starts_with("chidori-chat-")));
    interrupted_lease["expires_at"] =
        serde_json::Value::String((chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339());
    store
        .put_blob(
            "lease.json",
            &serde_json::to_vec(&interrupted_lease).unwrap(),
        )
        .unwrap();

    // Removing the local journal/checkpoint forces factory hydration from the
    // SQLite mirror. A failed held-lease resume must refuse before it
    // recreates these files or replaces this intentionally divergent input.
    fs::remove_file(run_dir.join("records.jsonl")).unwrap();
    let checkpoint_path = run_dir.join("checkpoint.json");
    if checkpoint_path.exists() {
        fs::remove_file(&checkpoint_path).unwrap();
    }
    let stale_local_input = br#"{"messages":["stale local transcript"]}"#.to_vec();
    fs::write(&input_path, &stale_local_input).unwrap();
    let local_before = snapshot_files(&run_dir);
    assert!(local_before.iter().all(|(name, _)| name != "records.jsonl"));
    assert_eq!(fs::read(&input_path).unwrap(), stale_local_input);
    let snapshot_blobs = |store: &dyn RunStore| {
        store
            .list_blobs()
            .unwrap()
            .into_iter()
            .filter(|key| key != "lease.json")
            .map(|key| {
                let bytes = store
                    .get_blob(&key)
                    .unwrap()
                    .expect("listed SQLite blob must be readable");
                (key, bytes)
            })
            .collect::<Vec<_>>()
    };
    let journal_before = serde_json::to_value(store.load_call_log().unwrap().unwrap()).unwrap();
    let blobs_before = snapshot_blobs(store.as_ref());
    assert!(
        blobs_before.iter().any(|(key, _)| key == "input.json"),
        "the SQLite snapshot must include the chat input blob"
    );
    let held = acquire_lease_with_token(
        store.as_ref(),
        "external-chat-holder",
        chrono::Duration::minutes(10),
    )
    .unwrap()
    .expect("the chat run lease must be free before this test holds it");
    let lease_before = store.get_blob("lease.json").unwrap().unwrap();

    let mut command = Command::new(chidori_bin());
    command
        .args(["chat", "--resume", &session_id])
        .current_dir(&dir)
        .env_remove("CHIDORI_RUN_STORE")
        .env_remove("CHIDORI_RUN_DB")
        .env("CHIDORI_RUN_STORE", "sqlite")
        .env("CHIDORI_RUN_DB", &db_path)
        .env(
            "CHIDORI_TEST_LLM_RESPONSE",
            "held-resume-provider-must-not-run",
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"second message\nexit\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let journal_after = serde_json::to_value(store.load_call_log().unwrap().unwrap()).unwrap();
    let blobs_after = snapshot_blobs(store.as_ref());
    let lease_after = store.get_blob("lease.json").unwrap().unwrap();
    let local_after = snapshot_files(&run_dir);
    let process_status_after = output.status;
    let stdout_after = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr_after = String::from_utf8_lossy(&output.stderr).into_owned();
    let diagnostic_after = format!("{stdout_after}\n{stderr_after}");

    // Release before assertions so a Red failure cannot leave the external
    // token held during fixture cleanup.
    chidori::runtime::store::release_lease_with_token(store.as_ref(), held.lease(), held.token())
        .unwrap();

    let mut observed = Vec::new();
    if process_status_after.success() {
        if stderr_after.contains("warning: could not take the session lease:") {
            observed.push(
                "chat warned that tokenless acquisition failed, then succeeded anyway".to_string(),
            );
        } else {
            observed
                .push("chat resumed successfully while the external tenure was held".to_string());
        }
    }
    if !diagnostic_after.contains("external-chat-holder") {
        observed.push("chat output did not identify the external held-lease owner".to_string());
    }
    if journal_after != journal_before {
        observed.push("SQLite call journal changed during refused chat resume".to_string());
    }
    if blobs_after != blobs_before {
        observed.push("SQLite session/input blobs changed during refused chat resume".to_string());
    }
    if lease_after != lease_before {
        observed.push("held SQLite lease bytes changed during refused chat resume".to_string());
    }
    if local_after != local_before {
        observed
            .push("local chat artifacts changed or hydration ran before held refusal".to_string());
    }
    if stdout_after.contains("held-resume-provider-must-not-run") {
        observed.push("provider executed while another SQLite tenure was held".to_string());
    }
    let held_refusal_passed = !process_status_after.success() && observed.is_empty();
    let held_refusal_diagnostics = observed.join("; ");

    let retry_env = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_path_text.as_str()),
        ("CHIDORI_TEST_LLM_RESPONSE", "released retry reply"),
    ];
    let retry = run_chidori_with_stdin(
        &["chat", "--resume", &session_id],
        &dir,
        &retry_env,
        "released retry message\nexit\n",
    );
    assert_success(&retry);
    let input: serde_json::Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    let messages = input["messages"].as_array().unwrap();
    assert!(messages.iter().any(|message| message == "first message"));
    assert!(messages
        .iter()
        .any(|message| message == "released retry message"));
    let post_retry = acquire_lease_with_token(
        store.as_ref(),
        "post-chat-contender",
        chrono::Duration::minutes(1),
    )
    .unwrap()
    .expect("chat resume must release its own SQLite tenure after the leg");
    chidori::runtime::store::release_lease_with_token(
        store.as_ref(),
        post_retry.lease(),
        post_retry.token(),
    )
    .unwrap();
    assert!(
        held_refusal_passed,
        "chat resume must fail with the held owner identified before hydration/turn execution: {held_refusal_diagnostics}; status={process_status_after}; stdout:\n{stdout_after}\nstderr:\n{stderr_after}"
    );
}

#[test]
fn cli_chat_resume_renews_sqlite_lease_during_idle_and_provider_waits() {
    use chidori::runtime::store::{
        acquire_lease_with_token, release_lease_with_token, RunStore, SqliteRunStore,
        SqliteRunStoreShared,
    };
    use std::io::{Read, Write};
    use std::process::Stdio;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    const TTL_MS: i64 = 600;
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().to_path_buf();
    let db_path = dir.join("chat-renewal.sqlite3");
    let db_text = db_path.to_string_lossy().to_string();
    let initial = run_chidori_with_stdin(
        &["chat"],
        &dir,
        &[
            ("CHIDORI_RUN_STORE", "sqlite"),
            ("CHIDORI_RUN_DB", db_text.as_str()),
            ("CHIDORI_TEST_LLM_RESPONSE", "initial reply"),
        ],
        "first message\nexit\n",
    );
    assert_success(&initial);
    let session_id = String::from_utf8_lossy(&initial.stderr)
        .lines()
        .find(|line| line.starts_with("session ") && !line.starts_with("session saved"))
        .and_then(|line| line.strip_prefix("session "))
        .and_then(|rest| rest.split_whitespace().next())
        .expect("initial chat announces its durable session id")
        .to_string();
    let shared = SqliteRunStoreShared::open(&db_path).unwrap();
    let store: std::sync::Arc<dyn RunStore> =
        std::sync::Arc::new(SqliteRunStore::new(shared, session_id.clone()));
    let server = GatedChatProvider::start();
    let mut child = KillChildOnDrop(Some(
        Command::new(chidori_bin())
            .args(["chat", "--resume", &session_id])
            .current_dir(&dir)
            .env_remove("CHIDORI_TEST_LLM_RESPONSE")
            .env_remove("CHIDORI_OPENAI_COMPAT_URL")
            .env_remove("CHIDORI_OPENAI_COMPAT_KEY")
            .env_remove("OPENAI_BASE_URL")
            .env_remove("ANTHROPIC_API_KEY")
            .env("OPENAI_API_KEY", "local-test-key")
            .env("OPENAI_BASE_URL", &server.base_url)
            .env("CHIDORI_RUN_STORE", "sqlite")
            .env("CHIDORI_RUN_DB", &db_path)
            .env("CHIDORI_DURABILITY", "strict")
            .env("CHIDORI_TEST_CHAT_LEASE_TTL_MS", TTL_MS.to_string())
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));

    let stdout = child.stdout.take().unwrap();
    let (prompt_tx, prompt_rx) = mpsc::channel();
    let stdout_thread = std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut collected = Vec::new();
        let mut byte = [0u8; 1];
        let mut notified = false;
        loop {
            match reader.read(&mut byte) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    collected.push(byte[0]);
                    if !notified && collected.windows(5).any(|window| window == b"you> ") {
                        let _ = prompt_tx.send(());
                        notified = true;
                    }
                }
            }
        }
        collected
    });
    let stderr = child.stderr.take().unwrap();
    let stderr_thread = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = std::io::BufReader::new(stderr).read_to_end(&mut bytes);
        bytes
    });
    prompt_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("resumed chat reaches its idle input prompt");

    let owner = format!("chidori-chat-{}", child.id());
    let lease_bytes = store
        .get_blob("lease.json")
        .unwrap()
        .expect("resumed chat acquired a SQLite lease");
    let lease_before: serde_json::Value = serde_json::from_slice(&lease_bytes).unwrap();
    assert_eq!(lease_before["owner"], owner);
    let initial_expiry =
        chrono::DateTime::parse_from_rfc3339(lease_before["expires_at"].as_str().unwrap()).unwrap();
    let idle_started = Instant::now();
    let ttl = Duration::from_millis(TTL_MS as u64);
    while idle_started.elapsed() <= ttl + Duration::from_millis(100) {
        assert!(
            child.try_wait().unwrap().is_none(),
            "chat exited while idle"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let idle_lease: serde_json::Value =
        serde_json::from_slice(&store.get_blob("lease.json").unwrap().unwrap()).unwrap();
    assert_eq!(idle_lease["owner"], owner);
    assert_eq!(idle_lease["generation"], lease_before["generation"]);
    let idle_expiry =
        chrono::DateTime::parse_from_rfc3339(idle_lease["expires_at"].as_str().unwrap()).unwrap();
    assert!(
        idle_expiry > initial_expiry,
        "the chat heartbeat must extend expiry while stdin is blocked"
    );
    assert!(idle_expiry.with_timezone(&chrono::Utc) > chrono::Utc::now());
    let idle_contender = acquire_lease_with_token(
        store.as_ref(),
        "idle-chat-contender",
        chrono::Duration::milliseconds(TTL_MS),
    )
    .unwrap();
    assert!(matches!(idle_contender, Err(holder) if holder.owner == owner));

    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"provider wait\n")
        .unwrap();
    let request = server
        .request_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("resumed chat dispatches its provider request");
    assert!(String::from_utf8_lossy(&request).contains("POST /v1/chat/completions"));
    let provider_block_lease: serde_json::Value =
        serde_json::from_slice(&store.get_blob("lease.json").unwrap().unwrap()).unwrap();
    let provider_block_expiry =
        chrono::DateTime::parse_from_rfc3339(provider_block_lease["expires_at"].as_str().unwrap())
            .unwrap();
    let renewal_deadline = Instant::now() + Duration::from_secs(4);
    let mut renewed_during_provider_wait = false;
    while Instant::now() < renewal_deadline {
        let current: serde_json::Value =
            serde_json::from_slice(&store.get_blob("lease.json").unwrap().unwrap()).unwrap();
        let current_expiry =
            chrono::DateTime::parse_from_rfc3339(current["expires_at"].as_str().unwrap()).unwrap();
        if current_expiry > provider_block_expiry {
            renewed_during_provider_wait = true;
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "chat exited during provider wait"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        renewed_during_provider_wait,
        "chat lease expiry must advance while the provider response is blocked"
    );
    let provider_contender = acquire_lease_with_token(
        store.as_ref(),
        "provider-chat-contender",
        chrono::Duration::milliseconds(TTL_MS),
    )
    .unwrap();
    assert!(matches!(provider_contender, Err(holder) if holder.owner == owner));

    server.release();
    child.stdin.as_mut().unwrap().write_all(b"exit\n").unwrap();
    drop(child.stdin.take());
    let status = child.wait().unwrap();
    child.0.take();
    let stdout = stdout_thread.join().unwrap();
    let stderr = stderr_thread.join().unwrap();
    assert!(
        status.success(),
        "resumed chat should finish after the provider returns; stdout={} stderr={}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    assert!(String::from_utf8_lossy(&stdout).contains("gated reply"));
    let released = acquire_lease_with_token(
        store.as_ref(),
        "post-chat-renewal-contender",
        chrono::Duration::minutes(1),
    )
    .unwrap()
    .expect("chat must stop heartbeat before releasing its exact tenure");
    release_lease_with_token(store.as_ref(), released.lease(), released.token()).unwrap();
}

fn snapshot_files(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn visit(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, out);
            } else {
                out.push((
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .to_string(),
                    fs::read(path).unwrap(),
                ));
            }
        }
    }

    if !root.exists() {
        return Vec::new();
    }
    let mut files = Vec::new();
    visit(root, root, &mut files);
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

/// An OpenAI-compatible endpoint whose response is held until the test moves
/// the SQLite lease to a newer generation. The request arrival is the
/// persistence safepoint: the prompt's pending host promise is already
/// durable, while its resolved result and journal row do not exist yet.
struct GatedChatProvider {
    base_url: String,
    request_rx: std::sync::mpsc::Receiver<Vec<u8>>,
    release_tx: std::sync::mpsc::Sender<()>,
    address: std::net::SocketAddr,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct KillChildOnDrop(Option<std::process::Child>);

impl std::ops::Deref for KillChildOnDrop {
    type Target = std::process::Child;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("child is still present")
    }
}

impl std::ops::DerefMut for KillChildOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("child is still present")
    }
}

impl Drop for KillChildOnDrop {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl GatedChatProvider {
    fn start() -> Self {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::mpsc;
        use std::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("provider accepts one request");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).expect("read provider headers");
                request.push(byte[0]);
            }
            request_tx.send(request).unwrap();
            release_rx
                .recv_timeout(Duration::from_secs(20))
                .expect("test releases the held provider response");

            let body = concat!(
                "data: {\"id\":\"chatcmpl-red\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"gated reply\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chatcmpl-red\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\n",
                "data: [DONE]\n\n"
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
        });
        Self {
            base_url: format!("http://{address}/v1"),
            request_rx,
            release_tx,
            address,
            thread: Some(thread),
        }
    }

    fn release(&self) {
        let _ = self.release_tx.send(());
    }
}

impl Drop for GatedChatProvider {
    fn drop(&mut self) {
        self.release();
        // Unblock accept if an earlier assertion failed before chat connected.
        let _ = std::net::TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn cli_chat_resume_rejects_stale_sqlite_engine_write_after_tenure_transfer() {
    use chidori::runtime::store::{
        acquire_lease_with_token, RunStore, SqliteRunStore, SqliteRunStoreShared,
    };
    use std::io::{Read, Write};
    use std::process::Stdio;
    use std::sync::mpsc;
    use std::time::Duration;

    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().to_path_buf();
    let db_path = dir.join("chat-stale-write.sqlite3");
    let db_text = db_path.to_string_lossy().to_string();
    let initial_env = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_text.as_str()),
        ("CHIDORI_TEST_LLM_RESPONSE", "initial reply"),
    ];
    let initial = run_chidori_with_stdin(&["chat"], &dir, &initial_env, "first message\nexit\n");
    assert_success(&initial);
    let session_id = String::from_utf8_lossy(&initial.stderr)
        .lines()
        .find(|line| line.starts_with("session ") && !line.starts_with("session saved"))
        .and_then(|line| line.strip_prefix("session "))
        .and_then(|rest| rest.split_whitespace().next())
        .expect("initial chat announces session id")
        .to_string();
    let run_dir = dir.join(".chidori").join("runs").join(&session_id);
    let shared = SqliteRunStoreShared::open(&db_path).unwrap();
    let store: std::sync::Arc<dyn RunStore> =
        std::sync::Arc::new(SqliteRunStore::new(shared, session_id.clone()));
    assert!(store.load_call_log().unwrap().is_some());

    let server = GatedChatProvider::start();
    let mut child = KillChildOnDrop(Some(
        Command::new(chidori_bin())
            .args(["chat", "--resume", &session_id])
            .current_dir(&dir)
            .env_remove("CHIDORI_TEST_LLM_RESPONSE")
            .env_remove("CHIDORI_OPENAI_COMPAT_URL")
            .env_remove("CHIDORI_OPENAI_COMPAT_KEY")
            .env_remove("OPENAI_BASE_URL")
            .env_remove("ANTHROPIC_API_KEY")
            .env("OPENAI_API_KEY", "local-test-key")
            .env("OPENAI_BASE_URL", &server.base_url)
            .env("CHIDORI_RUN_STORE", "sqlite")
            .env("CHIDORI_RUN_DB", &db_path)
            .env("CHIDORI_DURABILITY", "strict")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));

    // The first `you>` prompt proves replay and the CLI lease attempt have
    // completed. Keep stdin open until the provider call is blocked.
    let stdout = child.stdout.take().unwrap();
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let stdout_thread = std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut collected = Vec::new();
        let mut byte = [0u8; 1];
        let mut notified = false;
        loop {
            match reader.read(&mut byte) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    collected.push(byte[0]);
                    if !notified && collected.windows(5).any(|window| window == b"you> ") {
                        let _ = stdout_tx.send(collected.clone());
                        notified = true;
                    }
                }
            }
        }
        collected
    });
    let stderr = child.stderr.take().unwrap();
    let (stderr_tx, stderr_rx) = mpsc::channel();
    let stderr_thread = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = std::io::BufReader::new(stderr).read_to_end(&mut bytes);
        let _ = stderr_tx.send(bytes);
    });
    let prompt_prefix = stdout_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("chat reaches its input prompt");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"second message\n")
        .unwrap();
    let request = match server.request_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(request) => request,
        Err(error) => {
            server.release();
            let _ = child.kill();
            let status = child.wait().unwrap();
            child.0.take();
            let stderr_text = String::from_utf8_lossy(
                &stderr_rx
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap_or_default(),
            )
            .into_owned();
            stderr_thread.join().unwrap();
            panic!(
                "chat did not reach provider request ({error}); status={status}; stdout prompt={}; stderr={stderr_text}",
                String::from_utf8_lossy(&prompt_prefix)
            );
        }
    };
    let request_is_expected =
        String::from_utf8_lossy(&request).contains("POST /v1/chat/completions");

    // The provider is blocked after the Engine has persisted Pending state.
    // Observe cmd_chat's persisted lease before any test-side lease change.
    let owner_a = format!("chidori-chat-{}", child.id());
    let lease_at_safepoint = store.get_blob("lease.json").unwrap();
    let lease_bytes = lease_at_safepoint
        .as_deref()
        .expect("cmd_chat must have persisted its lease before the provider request");
    let observed_lease: serde_json::Value = serde_json::from_slice(lease_bytes).unwrap();
    let chat_owned_a = observed_lease["owner"] == owner_a;
    assert_eq!(
        observed_lease["owner"], owner_a,
        "observe cmd_chat's lease owner before any test-side acquisition"
    );
    let generation_a = observed_lease["generation"]
        .as_u64()
        .expect("cmd_chat lease must include its generation");
    let mut expired_lease = observed_lease;
    expired_lease["expires_at"] =
        serde_json::Value::String((chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339());
    store
        .put_blob("lease.json", &serde_json::to_vec(&expired_lease).unwrap())
        .unwrap();
    let tenure_b = acquire_lease_with_token(
        store.as_ref(),
        "external-chat-takeover",
        chrono::Duration::minutes(10),
    )
    .unwrap()
    .expect("B takes ownership after A is expired at the safepoint");
    let lease_b = store.get_blob("lease.json").unwrap().unwrap();
    let generation_b = serde_json::from_slice::<serde_json::Value>(&lease_b).unwrap()["generation"]
        .as_u64()
        .unwrap();
    let b_has_newer_generation = generation_b > generation_a;
    let b_is_current_owner = serde_json::from_slice::<serde_json::Value>(&lease_b).unwrap()
        ["owner"]
        == "external-chat-takeover";
    let journal_before_release =
        serde_json::to_vec(&store.load_call_log().unwrap().unwrap()).unwrap();
    let blobs_before_release = store
        .list_blobs()
        .unwrap()
        .into_iter()
        .filter(|key| key != "lease.json")
        .map(|key| {
            let bytes = store.get_blob(&key).unwrap().unwrap();
            (key, bytes)
        })
        .collect::<Vec<_>>();
    let local_before_release = snapshot_files(&run_dir);
    let has_durable_promise = blobs_before_release
        .iter()
        .any(|(key, _)| key.starts_with("host_promises/"));
    let has_pending_promise = blobs_before_release.iter().any(|(key, bytes)| {
        key.starts_with("host_promises/")
            && serde_json::from_slice::<serde_json::Value>(bytes)
                .ok()
                .is_some_and(|record| record["state"] == "pending")
    });
    let pending_promises_before_release = blobs_before_release
        .iter()
        .filter(|(key, bytes)| {
            key.starts_with("host_promises/")
                && serde_json::from_slice::<serde_json::Value>(bytes)
                    .ok()
                    .is_some_and(|record| record["state"] == "pending")
        })
        .cloned()
        .collect::<Vec<_>>();
    let pending_blob_before_release = blobs_before_release
        .iter()
        .find(|(key, _)| key == "pending.json")
        .cloned();

    // Queue another turn behind the already-dispatched request. A stale chat
    // must fail its tenure check after the first response and exit before it
    // prints another prompt or starts another turn.
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"follow-up must not run\n")
        .unwrap();
    server.release();
    drop(child.stdin.take());
    let status = child.wait().unwrap();
    child.0.take();
    let stdout_after = stdout_thread.join().unwrap();
    let prompt_count = stdout_after
        .windows(5)
        .filter(|window| *window == b"you> ")
        .count();
    let stderr_text = String::from_utf8_lossy(
        &stderr_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("capture chat stderr after it exits"),
    )
    .into_owned();
    stderr_thread.join().unwrap();
    let journal_after = serde_json::to_vec(&store.load_call_log().unwrap().unwrap()).unwrap();
    let blobs_after = store
        .list_blobs()
        .unwrap()
        .into_iter()
        .filter(|key| key != "lease.json")
        .map(|key| (key.clone(), store.get_blob(&key).unwrap().unwrap()))
        .collect::<Vec<_>>();
    let lease_after = store.get_blob("lease.json").unwrap().unwrap();
    let local_after = snapshot_files(&run_dir);
    chidori::runtime::store::release_lease_with_token(
        store.as_ref(),
        tenure_b.lease(),
        tenure_b.token(),
    )
    .unwrap();
    let journal_unchanged = journal_after == journal_before_release;
    let mirror_unchanged = blobs_after == blobs_before_release;
    let lease_unchanged = lease_after == lease_b;
    let local_unchanged = local_after == local_before_release;
    let pending_promises_after = blobs_after
        .iter()
        .filter(|(key, bytes)| {
            key.starts_with("host_promises/")
                && serde_json::from_slice::<serde_json::Value>(bytes)
                    .ok()
                    .is_some_and(|record| record["state"] == "pending")
        })
        .cloned()
        .collect::<Vec<_>>();
    let pending_promise_unchanged = !pending_promises_before_release.is_empty()
        && pending_promises_after == pending_promises_before_release;
    let pending_blob_after = blobs_after
        .iter()
        .find(|(key, _)| key == "pending.json")
        .cloned();
    let pending_blob_unchanged = pending_blob_after == pending_blob_before_release;
    let stale_write_rejected = stderr_text.contains("SQLite lease ownership lost");
    assert!(
        request_is_expected
            && b_has_newer_generation
            && b_is_current_owner
            && has_durable_promise
            && has_pending_promise
            && chat_owned_a
            && stale_write_rejected
            && !status.success()
            // One `you>` is the restored transcript; one is the active input
            // prompt. A third would show the chat loop accepted another turn.
            && prompt_count == 2
            && journal_unchanged
            && mirror_unchanged
            && pending_promise_unchanged
            && pending_blob_unchanged
            && lease_unchanged
            && local_unchanged,
        "cmd_chat must retain A at the provider safepoint, reject A's stale Engine write, preserve B's SQLite/FS state, and stop before a queued follow-up turn: status={status}; stdout-before-prompt={}; prompt_count={prompt_count}; stderr={stderr_text}; chat_owned_a={chat_owned_a}; observed_generation={generation_a}; stale_write_rejected={stale_write_rejected}; journal_unchanged={journal_unchanged}; mirror_unchanged={mirror_unchanged}; pending_promise_unchanged={pending_promise_unchanged}; pending_blob_unchanged={pending_blob_unchanged}; lease_unchanged={lease_unchanged}; local_unchanged={local_unchanged}",
        String::from_utf8_lossy(&prompt_prefix)
    );
}

/// Total size in bytes of every regular file under `path`, recursively.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

// `chidori export <run_id> --fixture <dest>` — a run directory is tens of MB
// (snapshot blob, promise table, duplicated journal), too heavy to commit as
// a CI test. Export carves out just what `chidori verify` reads
// (records.jsonl, runtime.snapshot.json, output.json, input.json), and
// `verify --runs-dir <dest>` must pass against the fixture alone.
#[test]
fn cli_export_fixture_is_minimal_and_verify_consumes_it() {
    let dir = temp_project("export-fixture");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("start", { who: input.who });
                const text = await chidori.prompt("say hi to " + input.who);
                return { text };
            }
        "#,
    )
    .unwrap();

    let output = run_chidori_with_str_env(
        &[
            "run",
            agent.to_str().unwrap(),
            "--input",
            r#"{"who":"fixture"}"#,
        ],
        &dir,
        &[("CHIDORI_TEST_LLM_RESPONSE", "hi fixture")],
    );
    assert_success(&output);
    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    // Precondition for the size claim: the full run dir carries the heavy
    // resume-only artifacts export must leave behind.
    assert!(run_dir.join("runtime.snapshot").exists());
    assert!(run_dir.join("host_promises.json").exists());
    // A mock run's snapshot blob is tiny; real runs carry multi-MB blobs —
    // that's the whole reason export exists. Pad the blob to a representative
    // size: export must not copy it, so the fixture stays small regardless.
    fs::write(run_dir.join("runtime.snapshot"), vec![0u8; 1 << 20]).unwrap();

    let fixture = dir.join("fixtures");
    let output = run_chidori(
        &["export", &run_id, "--fixture", fixture.to_str().unwrap()],
        &dir,
    );
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("exported fixture:") && stdout.contains("run dir was"),
        "export should summarize fixture vs run-dir size, got:\n{stdout}"
    );
    assert!(
        stdout.contains("--runs-dir"),
        "export should print the verify hint, got:\n{stdout}"
    );

    // The fixture holds exactly the verify set — no snapshot blob, no legacy
    // checkpoint.json, no host-promise/pending/lease state.
    let fixture_run = fixture.join(&run_id);
    assert!(fixture_run.join("records.jsonl").exists());
    assert!(fixture_run.join("runtime.snapshot.json").exists());
    assert!(fixture_run.join("output.json").exists());
    assert!(fixture_run.join("input.json").exists());
    assert!(!fixture_run.join("runtime.snapshot").exists());
    assert!(!fixture_run.join("checkpoint.json").exists());
    assert!(!fixture_run.join("host_promises.json").exists());
    assert!(!fixture_run.join("pending.json").exists());
    assert!(!fixture_run.join("lease.json").exists());

    // Dramatically smaller: the snapshot blob and duplicated journal dominate
    // the run dir, so the fixture must come in at a small fraction of it.
    let run_size = dir_size(&run_dir);
    let fixture_size = dir_size(&fixture_run);
    assert!(
        fixture_size * 10 < run_size,
        "fixture ({fixture_size} B) should be far smaller than the run dir ({run_size} B)"
    );

    // The fixture alone must satisfy `chidori verify` — no provider, and the
    // original run directory is not consulted.
    fs::remove_dir_all(&run_dir).unwrap();
    let output = run_chidori_without_providers(
        &[
            "verify",
            agent.to_str().unwrap(),
            &run_id,
            "--runs-dir",
            fixture.to_str().unwrap(),
        ],
        &dir,
    );
    assert_success(&output);
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("verified:"),
        "verify against the fixture should report success"
    );

    fs::remove_dir_all(dir).ok();
}

// Exporting a run that does not exist must error cleanly, naming the path it
// looked at.
#[test]
fn cli_export_nonexistent_run_errors_cleanly() {
    let dir = temp_project("export-missing");
    let fixture = dir.join("fixtures");
    let output = run_chidori(
        &[
            "export",
            "no-such-run",
            "--fixture",
            fixture.to_str().unwrap(),
        ],
        &dir,
    );
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No persisted run") && stderr.contains("no-such-run"),
        "expected a clear missing-run error, got:\n{stderr}"
    );
    assert!(
        !fixture.exists(),
        "a failed export must not leave a fixture directory behind"
    );

    fs::remove_dir_all(dir).ok();
}

// A run that is still owned by a live process (unexpired lease) has a
// mid-flight journal; export must refuse it rather than freeze an incomplete
// record as a "passing" fixture.
#[test]
fn cli_export_refuses_leased_run() {
    let dir = temp_project("export-leased");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent(input, chidori) {
                await chidori.log("ok", {});
                return { ok: true };
            }
        "#,
    )
    .unwrap();
    let output = run_chidori(&["run", agent.to_str().unwrap()], &dir);
    assert_success(&output);
    let run_id = first_run_id(&dir);

    // Simulate a live owner: an unexpired lease on the run directory.
    let lease = serde_json::json!({
        "owner": "some-other-process",
        "expires_at": chrono::Utc::now() + chrono::Duration::hours(1),
    });
    fs::write(
        dir.join(".chidori")
            .join("runs")
            .join(&run_id)
            .join("lease.json"),
        serde_json::to_vec(&lease).unwrap(),
    )
    .unwrap();

    let fixture = dir.join("fixtures");
    let output = run_chidori(
        &["export", &run_id, "--fixture", fixture.to_str().unwrap()],
        &dir,
    );
    assert_failure(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("leased") && stderr.contains("some-other-process"),
        "expected a live-lease refusal naming the owner, got:\n{stderr}"
    );

    fs::remove_dir_all(dir).ok();
}

fn assert_explicit_sqlite_init_failure(durability: &str, fixture_name: &str) {
    let dir = temp_project(fixture_name);
    let server = LocalHttpServer::start();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
                export async function agent() {{
                    const response = await fetch("http://{}/effect");
                    return await response.json();
                }}
            "#,
            server.address
        ),
    )
    .unwrap();

    // A directory at the configured DB filename is an existing, deterministic
    // SQLite open failure on every supported test platform.
    let db_path = dir.join("sqlite-db-is-a-directory");
    fs::create_dir_all(&db_path).unwrap();
    let db_path_string = db_path.to_string_lossy().to_string();
    let envs = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_path_string.as_str()),
        ("CHIDORI_DURABILITY", durability),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    let output =
        run_chidori_with_str_env(&["run", agent.to_str().unwrap(), "--trusted"], &dir, &envs);

    // Capture all observations and clean the fixture before assertions, so an
    // intended Red failure never leaves a temp project or server behind.
    let request_count = server.request_count();
    let run_base = dir.join(".chidori").join("runs");
    let created_run_directory = fs::read_dir(&run_base)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .any(|entry| entry.path().is_dir());
    let diagnostic = output_diagnostic(&output);
    drop(server);
    fs::remove_dir_all(&dir).ok();

    assert_eq!(
        request_count, 0,
        "SQLite initialization failure must be detected before the live HTTP effect"
    );
    assert_failure(&output);
    assert!(
        !created_run_directory,
        "SQLite initialization failure must not create a per-run directory"
    );
    assert!(
        diagnostic.contains(&db_path_string),
        "diagnostic must identify configured CHIDORI_RUN_DB path `{db_path_string}`; output:\n{diagnostic}"
    );
    assert!(
        diagnostic
            .to_lowercase()
            .contains("opening run store sqlite"),
        "diagnostic must identify the SQLite open operation; output:\n{diagnostic}"
    );
    assert!(
        diagnostic
            .to_lowercase()
            .contains("unable to open database file"),
        "diagnostic must preserve the SQLite open cause; output:\n{diagnostic}"
    );
}

#[test]
fn cli_resume_reports_sqlite_init_error_before_local_lease_or_journal_preflight() {
    let dir = temp_project("resume-sqlite-init-preflight");
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        r#"
            export async function agent() {
                await chidori.log("completed", {});
                return { ok: true };
            }
        "#,
    )
    .unwrap();
    let output = run_chidori(&["run", agent.to_str().unwrap()], &dir);
    assert_success(&output);

    let run_id = first_run_id(&dir);
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let records_path = run_dir.join("records.jsonl");
    let records_before = fs::read(&records_path).unwrap();
    let lease_path = run_dir.join("lease.json");
    let lease = serde_json::json!({
        "owner": "existing-holder",
        "expires_at": chrono::Utc::now() + chrono::Duration::hours(1),
    });
    let lease_before = serde_json::to_vec(&lease).unwrap();
    fs::write(&lease_path, &lease_before).unwrap();

    // A directory at the configured DB filename deterministically fails the
    // SQLite open. The config error must take precedence over local journal
    // reads and a live-lease refusal, without changing either local artifact.
    let db_path = dir.join("sqlite-db-is-a-directory");
    fs::create_dir_all(&db_path).unwrap();
    let db_path_string = db_path.to_string_lossy().to_string();
    let envs = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_path_string.as_str()),
        ("CHIDORI_DURABILITY", "besteffort"),
    ];
    let output =
        run_chidori_with_str_env(&["resume", agent.to_str().unwrap(), &run_id], &dir, &envs);
    let diagnostic = output_diagnostic(&output);

    assert_failure(&output);
    assert!(
        diagnostic.contains(&db_path_string)
            && diagnostic.to_lowercase().contains("opening run store sqlite")
            && diagnostic
                .to_lowercase()
                .contains("unable to open database file"),
        "resume should surface the explicit SQLite initialization error before local preflight; output:\n{diagnostic}"
    );
    assert_eq!(
        fs::read(&lease_path).unwrap(),
        lease_before,
        "resume must not acquire, overwrite, or release a local lease before SQLite initialization is checked"
    );
    assert_eq!(
        fs::read(&records_path).unwrap(),
        records_before,
        "resume must not mutate the local journal before SQLite initialization is checked"
    );

    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_run_rejects_explicit_sqlite_open_failure_before_run_or_effect() {
    assert_explicit_sqlite_init_failure("strict", "sqlite-open-failure-strict");
}

#[test]
fn cli_run_rejects_explicit_sqlite_open_failure_in_besteffort_mode() {
    assert_explicit_sqlite_init_failure("besteffort", "sqlite-open-failure-besteffort");
}

#[test]
fn cli_run_mirrors_records_to_valid_sqlite_store() {
    let dir = temp_project("sqlite-valid-mirror");
    let server = LocalHttpServer::start();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
                export async function agent() {{
                    const response = await fetch("http://{}/effect");
                    return await response.json();
                }}
            "#,
            server.address
        ),
    )
    .unwrap();
    let db_path = dir.join("runs.sqlite3");
    let db_path_string = db_path.to_string_lossy().to_string();
    let envs = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_path_string.as_str()),
        ("CHIDORI_DURABILITY", "strict"),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    let output =
        run_chidori_with_str_env(&["run", agent.to_str().unwrap(), "--trusted"], &dir, &envs);
    assert_success(&output);
    assert_eq!(
        server.request_count(),
        1,
        "valid SQLite configuration should allow the live HTTP effect"
    );

    let run_id = first_run_id(&dir);
    let connection = rusqlite::Connection::open(&db_path).unwrap();
    let mut statement = connection
        .prepare("SELECT data FROM run_records WHERE run_id = ?1 ORDER BY pos, seq")
        .unwrap();
    let rows = statement
        .query_map([run_id.as_str()], |row| row.get::<_, String>(0))
        .unwrap();
    let records: Vec<serde_json::Value> = rows
        .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
        .collect();
    assert!(
        !records.is_empty(),
        "successful run must persist call records to the SQLite mirror"
    );
    assert!(
        records.iter().any(|record| record["function"] == "http"),
        "SQLite mirror must include the HTTP call record: {records:?}"
    );

    drop(statement);
    drop(connection);
    drop(server);
    fs::remove_dir_all(dir).ok();
}

#[test]
fn cli_run_supports_unset_and_filesystem_run_store() {
    for mode in [None, Some("fs")] {
        let dir = temp_project("filesystem-store-control");
        let agent = dir.join("agent.ts");
        fs::write(
            &agent,
            "export async function agent() { await chidori.log('ok', {}); return {}; }",
        )
        .unwrap();
        let output = run_chidori_with_run_store_mode(&["run", agent.to_str().unwrap()], &dir, mode);
        assert_success(&output);
        let run_id = first_run_id(&dir);
        assert!(dir
            .join(".chidori")
            .join("runs")
            .join(run_id)
            .join("records.jsonl")
            .exists());
        fs::remove_dir_all(dir).ok();
    }
}

#[derive(Debug)]
struct ServeHttpResponse {
    status: u16,
    body: serde_json::Value,
}

fn serve_http_request(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> ServeHttpResponse {
    use std::time::Duration;

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let request = client
        .request(
            method.parse().expect("valid HTTP method"),
            format!("http://127.0.0.1:{port}{path}"),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    let response = request
        .body(body.unwrap_or("").to_string())
        .send()
        .unwrap_or_else(|error| panic!("{method} {path} on serve port {port}: {error}"));
    let status = response.status().as_u16();
    let body = response
        .json()
        .unwrap_or_else(|error| panic!("invalid JSON from serve HTTP status {status}: {error}"));
    ServeHttpResponse { status, body }
}

struct DetachedStopGate {
    address: String,
    hold_started: std::sync::mpsc::Receiver<()>,
    sentinel_seen: std::sync::mpsc::Receiver<String>,
    release: std::sync::mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
    sentinel_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    stopping: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl DetachedStopGate {
    fn start() -> Self {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{mpsc, Arc};
        use std::thread;
        use std::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (hold_tx, hold_started) = mpsc::channel();
        let (sentinel_tx, sentinel_seen) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let sentinel_count = Arc::new(AtomicUsize::new(0));
        let server_sentinel_count = Arc::clone(&sentinel_count);
        let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_stopping = Arc::clone(&stopping);
        let thread = thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("set detached stop gate listener nonblocking");
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            let mut hold_count = 0usize;
            let mut sentinel_seen_once = false;
            while std::time::Instant::now() < deadline
                && !sentinel_seen_once
                && !server_stopping.load(Ordering::SeqCst)
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("make accepted detached stop gate stream blocking");
                        stream
                            .set_read_timeout(Some(Duration::from_secs(10)))
                            .expect("set detached stop gate read timeout");
                        let mut request = Vec::new();
                        let mut chunk = [0u8; 2048];
                        let mut header_end = None;
                        let mut content_length = 0usize;
                        loop {
                            let count = stream.read(&mut chunk).expect("read gate request");
                            if count == 0 {
                                break;
                            }
                            request.extend_from_slice(&chunk[..count]);
                            if header_end.is_none() {
                                if let Some(index) =
                                    request.windows(4).position(|window| window == b"\r\n\r\n")
                                {
                                    header_end = Some(index + 4);
                                    let headers = String::from_utf8_lossy(&request[..index]);
                                    content_length = headers
                                        .lines()
                                        .find_map(|line| {
                                            let (name, value) = line.split_once(':')?;
                                            name.eq_ignore_ascii_case("content-length")
                                                .then(|| value.trim().parse().ok())
                                                .flatten()
                                        })
                                        .unwrap_or(0);
                                }
                            }
                            if let Some(header_end) = header_end {
                                if request.len() >= header_end + content_length {
                                    break;
                                }
                            }
                        }
                        let request = String::from_utf8_lossy(&request);
                        let first_line = request.lines().next().unwrap_or_default();
                        let response = if first_line.starts_with("POST /hold ") {
                            hold_count += 1;
                            hold_tx.send(()).expect("report held host call");
                            release_rx
                                .recv_timeout(Duration::from_secs(30))
                                .expect("test releases held host call");
                            r#"{"ok":true}"#
                        } else if first_line.starts_with("POST /sentinel ") {
                            let body = request
                                .split_once("\r\n\r\n")
                                .map(|(_, body)| body)
                                .unwrap_or_default();
                            let value: serde_json::Value =
                                serde_json::from_str(body).expect("parse sentinel payload");
                            let id = value["id"].as_str().unwrap_or_default().to_string();
                            server_sentinel_count.fetch_add(1, Ordering::SeqCst);
                            sentinel_tx
                                .send(id)
                                .expect("report post-stop sentinel effect");
                            sentinel_seen_once = true;
                            r#"{"ok":true}"#
                        } else {
                            panic!("unexpected detached stop gate request: {first_line}");
                        };
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            response.len(),
                            response
                        )
                        .expect("write detached stop gate response");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::yield_now();
                    }
                    Err(error) => panic!("detached stop gate accept failed: {error}"),
                }
            }
            assert!(
                hold_count > 0,
                "detached worker never reached the held host call"
            );
        });
        Self {
            address,
            hold_started,
            sentinel_seen,
            release,
            thread: Some(thread),
            sentinel_count,
            stopping,
        }
    }

    fn wait_until_held(&self) {
        self.hold_started
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("replacement reaches the deterministic host-call barrier");
    }

    fn release(&self) {
        let _ = self.release.send(());
    }
}

impl Drop for DetachedStopGate {
    fn drop(&mut self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() && !std::thread::panicking() {
                panic!("detached stop gate thread panicked");
            }
        }
    }
}

struct DetachedServeProcess {
    child: std::process::Child,
    log: PathBuf,
}

impl DetachedServeProcess {
    fn start(project: &Path, port: u16, backend: &str, db: &Path) -> Self {
        Self::start_with_verbosity(project, port, backend, db, false)
    }

    fn start_with_verbosity(
        project: &Path,
        port: u16,
        backend: &str,
        db: &Path,
        verbose: bool,
    ) -> Self {
        let mut command = Command::new(chidori_bin());
        let log = project.join(format!("serve-{port}.log"));
        let stderr = fs::File::create(&log).expect("create serve fixture log");
        command
            .args([
                "serve",
                "--port",
                &port.to_string(),
                "--no-isolate",
                "--trusted",
            ])
            .current_dir(project)
            .env("CHIDORI_ISOLATE", "off")
            .env("CHIDORI_POLICY_PROFILE", "trusted")
            .env("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1")
            .env("CHIDORI_RUN_STORE", backend)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::from(stderr));
        if verbose {
            command.arg("--verbose");
        }
        if backend == "sqlite" {
            command.env("CHIDORI_RUN_DB", db);
        } else {
            command.env_remove("CHIDORI_RUN_DB");
        }
        Self {
            child: command.spawn().expect("start detached-agent serve process"),
            log,
        }
    }

    fn port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    fn wait_ready(&mut self, port: u16) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "detached serve process exited before readiness: {status}; log={}",
                    fs::read_to_string(&self.log).unwrap_or_default()
                );
            }
            let client = reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(1))
                .build()
                .unwrap();
            if client
                .get(format!("http://127.0.0.1:{port}/health"))
                .send()
                .is_ok_and(|response| response.status().as_u16() == 200)
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "detached serve process did not become ready on {port}"
            );
            std::thread::yield_now();
        }
    }
}

impl Drop for DetachedServeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn detached_registry_entry(project: &Path, name: &str) -> serde_json::Value {
    let path = project
        .join(".chidori/runs/agents")
        .join(format!("{name}.json"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let last = match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(entry) => return entry,
                Err(error) => format!("parse error {error}; bytes={bytes:?}"),
            },
            Err(error) => format!("read error {error}"),
        };
        assert!(
            std::time::Instant::now() < deadline,
            "registry entry {} did not become readable: {last}",
            path.display()
        );
        std::thread::yield_now();
    }
}

struct DetachedStopProject(PathBuf);

impl Drop for DetachedStopProject {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn detached_registry_run_id(entry: &serde_json::Value) -> &str {
    entry["run_id"]
        .as_str()
        .or_else(|| entry["runId"].as_str())
        .or_else(|| entry["descriptor"]["run_id"].as_str())
        .or_else(|| entry["descriptor"]["runId"].as_str())
        .expect("registry entry has a run ID")
}

fn detached_registry_status(entry: &serde_json::Value) -> &str {
    entry["descriptor"]["status"]
        .as_str()
        .or_else(|| entry["status"].as_str())
        .expect("registry entry has a status")
}

fn detached_run_records(project: &Path, run_id: &str) -> Vec<serde_json::Value> {
    let path = project
        .join(".chidori/runs")
        .join(run_id)
        .join("records.jsonl");
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn prepare_rearm_diagnostic_project(project: &Path) -> String {
    let worker = project.join("prefix-worker.ts");
    let starter = project.join("spawn-prefix.ts");
    fs::write(
        &worker,
        r#"import { chidori, run } from "chidori:agent";
               run(async () => {
                 const message = await chidori.signal("wake");
                 await chidori.log("rearm-sentinel", { id: message.payload.id });
                return { id: message.payload.id };
               });"#,
    )
    .unwrap();
    fs::write(
        &starter,
        r#"import { chidori, run } from "chidori:agent";
           run(async () => {
             const handle = await chidori.agents.spawn("prefix-worker.ts", {}, {name:"aa-valid-prefix", restart:"resume"});
             return {runId: handle.runId};
           });"#,
    )
    .unwrap();

    let db = project.join("runs.sqlite3");
    let port = DetachedServeProcess::port();
    let mut serve = DetachedServeProcess::start(project, port, "fs", &db);
    serve.wait_ready(port);
    let response = serve_http_request(
        port,
        "POST",
        "/sessions",
        Some(r#"{"agent":"spawn-prefix.ts","input":{}}"#),
    );
    assert!(
        [200, 201].contains(&response.status),
        "spawn-prefix session failed: {:?}",
        response.body
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut entry = detached_registry_entry(project, "aa-valid-prefix");
    while detached_registry_status(&entry) != "hibernating" && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
        entry = detached_registry_entry(project, "aa-valid-prefix");
    }
    assert_eq!(detached_registry_status(&entry), "hibernating");
    let run_id = detached_registry_run_id(&entry).to_string();
    let lease_path = project
        .join(".chidori/runs")
        .join(&run_id)
        .join("lease.json");
    let lease_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let lease = fs::read(&lease_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
        let released = lease
            .as_ref()
            .and_then(|lease| lease["expires_at"].as_str())
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .is_none_or(|expires_at| expires_at <= chrono::Utc::now());
        if released {
            break;
        }
        assert!(
            std::time::Instant::now() < lease_deadline,
            "hibernating fixture still has an unexpired supervisor lease: {}",
            fs::read_to_string(&lease_path).unwrap_or_default()
        );
        std::thread::yield_now();
    }
    serve.child.kill().unwrap();
    let _ = serve.child.wait();

    let registry_path = project.join(".chidori/runs/agents/aa-valid-prefix.json");
    let run_dir = project.join(".chidori/runs").join(&run_id);
    let descriptor_path = run_dir.join("agent.json");
    let mut registry: serde_json::Value =
        serde_json::from_slice(&fs::read(&registry_path).unwrap()).unwrap();
    registry["descriptor"]["status"] = serde_json::json!("running");
    fs::write(
        &registry_path,
        serde_json::to_vec_pretty(&registry).unwrap(),
    )
    .unwrap();
    let mut descriptor: serde_json::Value =
        serde_json::from_slice(&fs::read(&descriptor_path).unwrap()).unwrap();
    descriptor["status"] = serde_json::json!("running");
    fs::write(
        &descriptor_path,
        serde_json::to_vec_pretty(&descriptor).unwrap(),
    )
    .unwrap();
    let inbox_path = run_dir.join("signals/inbox.json");
    fs::create_dir_all(inbox_path.parent().unwrap()).unwrap();
    fs::write(
        &inbox_path,
        serde_json::to_vec(&[serde_json::json!({
            "name": "wake",
            "payload": {"id": "queued-prefix"},
            "from": {"kind": "test", "id": "rearm-diagnostic"},
            "delivery_seq": 1,
            "enqueued_at": "2026-10-22T00:00:00Z"
        })])
        .unwrap(),
    )
    .unwrap();
    run_id
}

fn run_rearm_diagnostic_case(case: &str, project: &Path) {
    let corrupt = matches!(case, "default-corrupt" | "verbose-corrupt");
    let run_id = if case == "absent-registry" {
        None
    } else {
        Some(prepare_rearm_diagnostic_project(project))
    };
    let corrupt_path = if corrupt {
        let path = project.join(".chidori/runs/agents/zz-corrupt-later.json");
        fs::write(&path, b"{ malformed later registry row").unwrap();
        Some(path)
    } else {
        None
    };
    let before_registry = run_id
        .as_ref()
        .map(|_| fs::read(project.join(".chidori/runs/agents/aa-valid-prefix.json")).unwrap());
    let run_dir = run_id
        .as_ref()
        .map(|id| project.join(".chidori/runs").join(id));
    let before_agent = run_dir
        .as_ref()
        .map(|dir| fs::read(dir.join("agent.json")).unwrap());
    let before_journal = run_dir
        .as_ref()
        .map(|dir| fs::read(dir.join("records.jsonl")).unwrap_or_default());
    let before_inbox = run_dir
        .as_ref()
        .map(|dir| fs::read(dir.join("signals/inbox.json")).unwrap());

    let port = DetachedServeProcess::port();
    let db = project.join("runs.sqlite3");
    let verbose = case == "verbose-corrupt";
    let mut serve = DetachedServeProcess::start_with_verbosity(project, port, "fs", &db, verbose);
    serve.wait_ready(port);
    let health = serve_http_request(port, "GET", "/health", None);
    assert_eq!(
        health.status, 200,
        "serve must remain available after rearm error"
    );
    let stderr = fs::read_to_string(&serve.log).unwrap_or_default();

    if corrupt {
        assert_eq!(
            fs::read(project.join(".chidori/runs/agents/aa-valid-prefix.json")).unwrap(),
            before_registry.unwrap(),
            "failed fleet rearm must not mutate the valid-prefix registry entry"
        );
        assert_eq!(
            fs::read(run_dir.as_ref().unwrap().join("agent.json")).unwrap(),
            before_agent.unwrap(),
            "failed fleet rearm must not mutate the valid-prefix run descriptor"
        );
        assert_eq!(
            fs::read(run_dir.as_ref().unwrap().join("records.jsonl")).unwrap_or_default(),
            before_journal.unwrap(),
            "failed fleet rearm must not run or journal the valid-prefix agent"
        );
        assert_eq!(
            fs::read(run_dir.as_ref().unwrap().join("signals/inbox.json")).unwrap(),
            before_inbox.unwrap(),
            "failed fleet rearm must preserve the matching queued signal"
        );
        assert_eq!(
            detached_registry_status(&detached_registry_entry(project, "aa-valid-prefix")),
            "running",
            "failed fleet rearm must not start the valid-prefix worker"
        );
        let relative_corrupt_path = format!(
            "./.chidori/runs/agents/{}",
            corrupt_path
                .as_ref()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
        );
        assert!(
            stderr.contains("re-arming detached agents")
                && stderr.contains(&relative_corrupt_path),
            "serve stderr must surface rearm failure and corrupt path {relative_corrupt_path} (verbose={verbose}); stderr:\n{stderr}"
        );
    } else if case == "healthy-fleet" {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let journal_path = run_dir.as_ref().unwrap().join("records.jsonl");
        loop {
            let descriptor = detached_registry_entry(project, "aa-valid-prefix");
            let records = journal_records(&journal_path);
            if detached_registry_status(&descriptor) == "completed"
                && records.iter().any(|record| {
                    record["function"] == "log"
                        && record["args"].to_string().contains("rearm-sentinel")
                })
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "healthy fleet did not consume queued signal and reach sentinel; registry={descriptor}; records={records:?}; stderr={}",
                fs::read_to_string(&serve.log).unwrap_or_default()
            );
            std::thread::yield_now();
        }
        let records = journal_records(&journal_path);
        assert_eq!(
            records
                .iter()
                .filter(|record| record["function"] == "log"
                    && record["args"].to_string().contains("rearm-sentinel"))
                .count(),
            1,
            "healthy fleet should run its sentinel exactly once"
        );
        assert_eq!(
            detached_registry_status(&detached_registry_entry(project, "aa-valid-prefix")),
            "completed"
        );
        assert!(
            !stderr.contains("re-arming detached agents"),
            "healthy fleet must not log a rearm warning: {stderr}"
        );
    } else {
        assert_eq!(health.body["status"], "ok");
        assert!(
            !stderr.contains("re-arming detached agents"),
            "empty fleet must not log a rearm warning: {stderr}"
        );
        assert_eq!(
            serve_http_request(port, "GET", "/agents/detached", None).body["agents"],
            serde_json::json!([])
        );
        assert!(
            !project.join(".chidori/runs/agents").exists(),
            "empty fleet should not create a registry directory"
        );
    }
}

#[test]
fn cli_serve_surfaces_detached_rearm_failure_at_default_verbosity_and_keeps_controls() {
    const TEST: &str =
        "cli_serve_surfaces_detached_rearm_failure_at_default_verbosity_and_keeps_controls";
    const CASE: &str = "CHIDORI_TEST_REARM_DIAGNOSTIC_CASE";
    const ROOT: &str = "CHIDORI_TEST_REARM_DIAGNOSTIC_ROOT";

    if let Some(case) = std::env::var_os(CASE) {
        let project = PathBuf::from(std::env::var_os(ROOT).unwrap());
        run_rearm_diagnostic_case(&case.to_string_lossy(), &project);
        return;
    }

    let mut failures = Vec::new();
    let mut outcomes = Vec::new();
    for case in [
        "default-corrupt",
        "verbose-corrupt",
        "healthy-fleet",
        "absent-registry",
    ] {
        let project = temp_project(&format!("rearm-diagnostic-{case}"));
        let _cleanup = DetachedStopProject(project.clone());
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env(CASE, case)
            .env(ROOT, &project)
            .output()
            .unwrap();
        let report = format!(
            "case={case} status={:?}\nstdout:\n{}\nstderr:\n{}",
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
        "serve rearm diagnostic child failures:\n{failures:#?}\nall outcomes:\n{outcomes:#?}"
    );
}

#[test]
fn cli_detached_stop_by_name_targets_current_cross_process_incarnation() {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    const NAME: &str = "stop-current-name";
    let mut failures = Vec::new();
    for backend in ["fs", "sqlite"] {
        let project = temp_project("detached-stop-current-name");
        let _project_cleanup = DetachedStopProject(project.clone());
        let gate = DetachedStopGate::start();
        let db = project.join("shared.sqlite3");
        let worker_x = project.join("old-worker.ts");
        let start_x = project.join("spawn-old.ts");
        let stop_x = project.join("stop-old-name.ts");
        let worker_y = project.join("replacement-worker.ts");
        let idle_worker = project.join("idle-worker.ts");
        let rearm_worker = project.join("hibernating-rearm-worker.ts");
        let name_rearm_worker = project.join("hibernating-name-hold-worker.ts");
        let handle_rearm_worker = project.join("hibernating-handle-worker.ts");
        fs::write(
            &worker_x,
            r#"import { chidori, run } from "chidori:agent";
               run(async () => { await chidori.signal("finish"); return {old: true}; });"#,
        )
        .unwrap();
        fs::write(
            &start_x,
            format!(
                r#"import {{ chidori, run }} from "chidori:agent";
                   run(async () => {{
                     const old = await chidori.agents.spawn("{}", {{}}, {{name:"{}", restart:"never"}});
                     return {{runId: old.runId}};
                   }});"#,
                worker_x.file_name().unwrap().to_string_lossy(),
                NAME
            ),
        )
        .unwrap();
        fs::write(
            &worker_y,
            format!(
                r#"import {{ chidori, run }} from "chidori:agent";
                   run(async () => {{
                     await chidori.signal("start");
                     await fetch("http://{}/hold", {{method:"POST", body:JSON.stringify({{id:"held"}})}});
                     await fetch("http://{}/sentinel", {{method:"POST", body:JSON.stringify({{id:"post-stop"}})}});
                     return {{afterStop:true}};
                   }});"#,
                gate.address, gate.address
            ),
        )
        .unwrap();
        fs::write(
            &idle_worker,
            r#"import { chidori, run } from "chidori:agent";
               run(async () => { await chidori.signal("never"); return {}; });"#,
        )
        .unwrap();
        fs::write(
            &rearm_worker,
            format!(
                r#"import {{ chidori, run }} from "chidori:agent";
                   run(async () => {{
                     await chidori.signal("wake");
                     await fetch("http://{}/sentinel", {{method:"POST", body:JSON.stringify({{id:"hibernating-rearm"}})}});
                     return {{afterStop:true}};
                   }});"#,
                gate.address
            ),
        )
        .unwrap();
        fs::write(
            &name_rearm_worker,
            format!(
                r#"import {{ chidori, run }} from "chidori:agent";
                   run(async () => {{
                     await chidori.signal("wake");
                     await fetch("http://{}/hold", {{method:"POST", body:JSON.stringify({{id:"hibernating-name-hold"}})}});
                     await fetch("http://{}/sentinel", {{method:"POST", body:JSON.stringify({{id:"hibernating-name"}})}});
                     return {{afterStop:true}};
                   }});"#,
                gate.address, gate.address
            ),
        )
        .unwrap();
        fs::write(
            &handle_rearm_worker,
            format!(
                r#"import {{ chidori, run }} from "chidori:agent";
                   run(async () => {{
                     await chidori.signal("wake");
                     await fetch("http://{}/hold", {{method:"POST", body:JSON.stringify({{id:"hibernating-handle-hold"}})}});
                     await fetch("http://{}/sentinel", {{method:"POST", body:JSON.stringify({{id:"hibernating-handle"}})}});
                     return {{afterStop:true}};
                   }});"#,
                gate.address, gate.address
            ),
        )
        .unwrap();
        let port_a = DetachedServeProcess::port();
        let port_b = DetachedServeProcess::port();
        let mut serve_a = DetachedServeProcess::start(&project, port_a, backend, &db);
        serve_a.wait_ready(port_a);

        let old_session = serde_json::json!({"agent":"spawn-old.ts","input":{}}).to_string();
        let old_response = serve_http_request(port_a, "POST", "/sessions", Some(&old_session));
        assert!(
            [200, 201].contains(&old_response.status),
            "old-spawn response: {:?}",
            old_response.body
        );
        let old_status_deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut old_status =
            serve_http_request(port_a, "GET", &format!("/agents/detached/{NAME}"), None);
        while old_status.body["status"] == "running"
            && std::time::Instant::now() < old_status_deadline
        {
            std::thread::yield_now();
            old_status =
                serve_http_request(port_a, "GET", &format!("/agents/detached/{NAME}"), None);
        }
        assert_eq!(old_status.body["status"], "hibernating");
        fs::write(
            &stop_x,
            format!(
                r#"import {{ chidori, run }} from "chidori:agent";
                   run(async () => {{
                     const stopped = await chidori.agents.stop("{}");
                     return {{runId: stopped.runId, status: stopped.status, stopRequested: stopped.stopRequested}};
                   }});"#,
                NAME
            ),
        )
        .unwrap();
        let stop_old_body = serde_json::json!({"agent":"stop-old-name.ts","input":{}}).to_string();
        let stop_old = serve_http_request(port_a, "POST", "/sessions", Some(&stop_old_body));
        assert!(
            [200, 201].contains(&stop_old.status),
            "same-hub predecessor name stop: {:?}",
            stop_old.body
        );
        let predecessor = detached_registry_entry(&project, NAME);
        let predecessor_id = detached_registry_run_id(&predecessor).to_string();
        assert_eq!(detached_registry_status(&predecessor), "hibernating");
        assert_eq!(stop_old.body["output"]["stopRequested"], true);
        assert!(
            stop_old.body.to_string().contains(&predecessor_id),
            "same-hub stop handle should identify predecessor {predecessor_id}: {:?}",
            stop_old.body
        );
        assert_eq!(
            predecessor["descriptor"]["listen"]["names"],
            serde_json::json!(["finish"]),
            "pending same-hub stop must preserve the hibernating listen"
        );
        let predecessor_wake = serde_json::json!({"name":"finish","payload":{}}).to_string();
        let predecessor_wake_response = serve_http_request(
            port_a,
            "POST",
            &format!("/agents/detached/{NAME}/send"),
            Some(&predecessor_wake),
        );
        assert_eq!(predecessor_wake_response.status, 200);
        let predecessor_stop_deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut predecessor_final = detached_registry_entry(&project, NAME);
        while detached_registry_status(&predecessor_final) != "stopped"
            && std::time::Instant::now() < predecessor_stop_deadline
        {
            std::thread::yield_now();
            predecessor_final = detached_registry_entry(&project, NAME);
        }
        assert_eq!(detached_registry_status(&predecessor_final), "stopped");

        // Start a fresh server only after X settled. Its manifest causes the
        // actual public serve boot path to replace the settled name with Y.
        fs::write(
            project.join("chidori.app.yml"),
            format!(
                "name: stop-name-fixture\nagents:\n  - name: {}\n    agent: {}\n    keep_alive: true\n    restart: never\n  - name: current-hub-control\n    agent: {}\n    keep_alive: true\n    restart: never\n  - name: hibernating-rearm-control\n    agent: {}\n    keep_alive: true\n    restart: never\n  - name: hibernating-name-control\n    agent: {}\n    keep_alive: true\n    restart: never\n  - name: hibernating-handle-control\n    agent: {}\n    keep_alive: true\n    restart: never\n",
                NAME,
                worker_y.file_name().unwrap().to_string_lossy(),
                idle_worker.file_name().unwrap().to_string_lossy(),
                rearm_worker.file_name().unwrap().to_string_lossy(),
                name_rearm_worker.file_name().unwrap().to_string_lossy(),
                handle_rearm_worker.file_name().unwrap().to_string_lossy()
            ),
        )
        .unwrap();
        let mut serve_b = DetachedServeProcess::start(&project, port_b, backend, &db);
        serve_b.wait_ready(port_b);
        let current_entry = detached_registry_entry(&project, NAME);
        let current_id = detached_registry_run_id(&current_entry).to_string();
        assert_ne!(
            current_id, predecessor_id,
            "B must publish a replacement run"
        );
        let start_replacement = serde_json::json!({"name":"start","payload":{}}).to_string();
        let start_response = serve_http_request(
            port_b,
            "POST",
            &format!("/agents/detached/{NAME}/send"),
            Some(&start_replacement),
        );
        assert_eq!(
            start_response.status, 200,
            "start replacement: {:?}",
            start_response.body
        );
        gate.wait_until_held();
        let current_entry = detached_registry_entry(&project, NAME);
        assert_eq!(detached_registry_status(&current_entry), "running");

        // This name-addressed request enters A's existing public serve route.
        // The response is the stop boundary; only after receiving it do we
        // release Y's held host call and observe whether its next effect runs.
        let stop_response = serve_http_request(
            port_a,
            "POST",
            &format!("/agents/detached/{NAME}/stop"),
            None,
        );
        gate.release();

        let terminal_deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut final_entry = detached_registry_entry(&project, NAME);
        let mut final_status =
            serve_http_request(port_b, "GET", &format!("/agents/detached/{NAME}"), None);
        while (detached_registry_status(&final_entry) == "running"
            || final_status.body["live"] == true)
            && std::time::Instant::now() < terminal_deadline
        {
            std::thread::yield_now();
            final_entry = detached_registry_entry(&project, NAME);
            final_status =
                serve_http_request(port_b, "GET", &format!("/agents/detached/{NAME}"), None);
        }
        let sentinel = gate.sentinel_seen.try_recv().ok();
        let records = detached_run_records(&project, &current_id);
        let sentinel_rows = records
            .iter()
            .filter(|record| record.to_string().contains("/sentinel"))
            .count();
        let sentinel_count = gate.sentinel_count.load(Ordering::SeqCst);

        if stop_response.status != 200 {
            failures.push(format!(
                "{backend}: stop returned HTTP {}, body={}",
                stop_response.status, stop_response.body
            ));
        }
        if stop_response.body["runId"].as_str() != Some(current_id.as_str()) {
            failures.push(format!(
                "{backend}: name stop response targeted {:?}, expected current replacement {}; predecessor={predecessor_id}; body={}",
                stop_response.body["runId"], current_id, stop_response.body
            ));
        }
        if stop_response.body["stopRequested"] != true {
            failures.push(format!(
                "{backend}: cross-process stop response did not acknowledge a pending request: {}",
                stop_response.body
            ));
        }
        if sentinel_count != 0 || sentinel.is_some() || sentinel_rows != 0 {
            failures.push(format!(
                "{backend}: replacement performed post-stop sentinel: count={sentinel_count}, event={sentinel:?}, journal_rows={sentinel_rows}"
            ));
        }
        if detached_registry_status(&final_entry) != "stopped" {
            failures.push(format!(
                "{backend}: replacement did not settle stopped; current={} final={final_entry}",
                current_id
            ));
        }

        // A's current-name stop must leave a durable intent when the current
        // run is hibernating in B. A later matching wake/rearm must observe the
        // marker before dispatching the worker's next effect.
        let rearm_name = "hibernating-rearm-control";
        let rearm_deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut rearm_entry = detached_registry_entry(&project, rearm_name);
        while detached_registry_status(&rearm_entry) == "running"
            && std::time::Instant::now() < rearm_deadline
        {
            std::thread::yield_now();
            rearm_entry = detached_registry_entry(&project, rearm_name);
        }
        assert_eq!(detached_registry_status(&rearm_entry), "hibernating");
        let rearm_run_id = detached_registry_run_id(&rearm_entry).to_string();
        let remote_hibernating_stop = serve_http_request(
            port_a,
            "POST",
            &format!("/agents/detached/{rearm_name}/stop"),
            None,
        );
        assert_eq!(remote_hibernating_stop.status, 200);
        assert_eq!(
            remote_hibernating_stop.body["runId"].as_str(),
            Some(rearm_run_id.as_str())
        );
        assert_eq!(remote_hibernating_stop.body["stopRequested"], true);
        assert_ne!(remote_hibernating_stop.body["status"], "stopped");

        let matching_wake = serde_json::json!({"name":"wake","payload":{}}).to_string();
        let matching_wake_response = serve_http_request(
            port_b,
            "POST",
            &format!("/agents/detached/{rearm_name}/send"),
            Some(&matching_wake),
        );
        assert_eq!(matching_wake_response.status, 200);
        let rearm_stop_deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut rearm_final = detached_registry_entry(&project, rearm_name);
        while detached_registry_status(&rearm_final) != "stopped"
            && std::time::Instant::now() < rearm_stop_deadline
        {
            std::thread::yield_now();
            rearm_final = detached_registry_entry(&project, rearm_name);
        }
        let rearm_records = detached_run_records(&project, &rearm_run_id);
        let rearm_sentinel_rows = rearm_records
            .iter()
            .filter(|record| record.to_string().contains("/sentinel"))
            .count();
        let rearm_signal_rows = rearm_records
            .iter()
            .filter(|record| record["function"] == "signal")
            .count();
        let rearm_inbox_path = project
            .join(".chidori/runs")
            .join(&rearm_run_id)
            .join("signals/inbox.json");
        let rearm_inbox: Vec<serde_json::Value> =
            serde_json::from_slice(&fs::read(&rearm_inbox_path).unwrap_or_else(|err| {
                panic!(
                    "reading hibernating rearm inbox {}: {err}",
                    rearm_inbox_path.display()
                )
            }))
            .unwrap();
        if detached_registry_status(&rearm_final) != "stopped"
            || rearm_sentinel_rows != 0
            || rearm_signal_rows != 0
            || rearm_inbox.len() != 1
            || rearm_inbox[0]["name"] != "wake"
            || gate.sentinel_count.load(Ordering::SeqCst) != 0
        {
            failures.push(format!(
                "{backend}: hibernating run did not observe durable stop before its matching-wake effect: final={rearm_final}, sentinel_rows={rearm_sentinel_rows}, signal_rows={rearm_signal_rows}, inbox={rearm_inbox:?}, count={}",
                gate.sentinel_count.load(Ordering::SeqCst)
            ));
        }

        // A caches each same-run hibernating incarnation before B wakes it.
        // Once B owns execution and is blocked in /hold, A must persist a
        // run-scoped request through either name routing or its runId handle,
        // without locally settling the stale cached descriptor.
        for (name, stop_via_handle) in [
            ("hibernating-name-control", false),
            ("hibernating-handle-control", true),
        ] {
            let hibernating_deadline = std::time::Instant::now() + Duration::from_secs(20);
            let mut hibernating_entry = detached_registry_entry(&project, name);
            while detached_registry_status(&hibernating_entry) == "running"
                && std::time::Instant::now() < hibernating_deadline
            {
                std::thread::yield_now();
                hibernating_entry = detached_registry_entry(&project, name);
            }
            assert_eq!(detached_registry_status(&hibernating_entry), "hibernating");
            let held_run_id = detached_registry_run_id(&hibernating_entry).to_string();

            let cached =
                serve_http_request(port_a, "GET", &format!("/agents/detached/{name}"), None);
            assert_eq!(cached.status, 200, "A caches {name}: {:?}", cached.body);
            assert_eq!(cached.body["runId"].as_str(), Some(held_run_id.as_str()));

            let wake = serde_json::json!({"name":"wake","payload":{}}).to_string();
            let wake_response = serve_http_request(
                port_b,
                "POST",
                &format!("/agents/detached/{name}/send"),
                Some(&wake),
            );
            assert_eq!(
                wake_response.status, 200,
                "wake {name}: {:?}",
                wake_response.body
            );
            gate.wait_until_held();

            let registry_path = project
                .join(".chidori/runs/agents")
                .join(format!("{name}.json"));
            let agent_path = project
                .join(".chidori/runs")
                .join(&held_run_id)
                .join("agent.json");
            let registry_before_stop = fs::read(&registry_path).unwrap();
            let agent_before_stop = fs::read(&agent_path).unwrap();
            if backend == "sqlite" {
                let connection = rusqlite::Connection::open(&db).expect("open SQLite run store");
                let lease_bytes: Vec<u8> = connection
                    .query_row(
                        "SELECT data FROM run_blobs WHERE run_id = ?1 AND key = 'lease.json'",
                        [&held_run_id],
                        |row| row.get(0),
                    )
                    .expect("active SQLite lease exists while B is held");
                let lease: serde_json::Value =
                    serde_json::from_slice(&lease_bytes).expect("parse active SQLite lease");
                let owner = lease["owner"].as_str().expect("lease owner");
                let expires_at = chrono::DateTime::parse_from_rfc3339(
                    lease["expires_at"].as_str().expect("lease expiry"),
                )
                .expect("parse SQLite lease expiry");
                assert!(!owner.is_empty());
                assert!(expires_at > chrono::Utc::now());
            }

            let stop_response = if stop_via_handle {
                let handle_script = project.join("stop-cached-handle.ts");
                fs::write(
                    &handle_script,
                    format!(
                        r#"import {{ chidori, run }} from "chidori:agent";
                           run(async () => {{
                             const handle = await chidori.agents.lookup("{name}");
                             if (!handle) throw new Error("missing cached handle");
                             const result = await handle.stop();
                             return {{runId: handle.runId, status: result.status, stopRequested: result.stopRequested}};
                           }});"#
                    ),
                )
                .unwrap();
                let request =
                    serde_json::json!({"agent":"stop-cached-handle.ts","input":{}}).to_string();
                serve_http_request(port_a, "POST", "/sessions", Some(&request))
            } else {
                serve_http_request(
                    port_a,
                    "POST",
                    &format!("/agents/detached/{name}/stop"),
                    None,
                )
            };
            assert!(
                [200, 201].contains(&stop_response.status),
                "stop {name}: {:?}",
                stop_response.body
            );
            assert!(
                stop_response.body.to_string().contains(&held_run_id),
                "stop must identify cached current run {held_run_id}: {:?}",
                stop_response.body
            );
            assert!(
                stop_response.body.to_string().contains("stopRequested")
                    && stop_response.body.to_string().contains("true"),
                "remote cached stop must report stopRequested: {:?}",
                stop_response.body
            );
            let registry_after_stop = fs::read(&registry_path).unwrap();
            let agent_after_stop = fs::read(&agent_path).unwrap();
            assert_eq!(
                registry_after_stop, registry_before_stop,
                "stop response must not rewrite registry descriptor for {name}"
            );
            assert_eq!(
                agent_after_stop, agent_before_stop,
                "stop response must not rewrite run descriptor for {name}"
            );
            gate.release();

            let stop_deadline = std::time::Instant::now() + Duration::from_secs(20);
            let mut stopped = detached_registry_entry(&project, name);
            while detached_registry_status(&stopped) != "stopped"
                && std::time::Instant::now() < stop_deadline
            {
                std::thread::yield_now();
                stopped = detached_registry_entry(&project, name);
            }
            let records = detached_run_records(&project, &held_run_id);
            let sentinel_rows = records
                .iter()
                .filter(|record| record.to_string().contains("/sentinel"))
                .count();
            assert_eq!(detached_registry_status(&stopped), "stopped");
            assert_eq!(sentinel_rows, 0, "{name} must not reach sentinel");
            assert_eq!(gate.sentinel_count.load(Ordering::SeqCst), 0);
        }

        // Current-hub route control: B's hub owns this hibernating incarnation;
        // the durable acceptance response still avoids claiming it already settled.
        let idle_name = "current-hub-control";
        let idle_deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut idle_entry = detached_registry_entry(&project, idle_name);
        while detached_registry_status(&idle_entry) == "running"
            && std::time::Instant::now() < idle_deadline
        {
            std::thread::yield_now();
            idle_entry = detached_registry_entry(&project, idle_name);
        }
        assert_eq!(detached_registry_status(&idle_entry), "hibernating");
        let idle_id = detached_registry_run_id(&idle_entry).to_string();
        let current_hub_stop = serve_http_request(
            port_b,
            "POST",
            &format!("/agents/detached/{idle_name}/stop"),
            None,
        );
        assert_eq!(
            current_hub_stop.status, 200,
            "current-hub stop: {:?}",
            current_hub_stop.body
        );
        assert_eq!(
            current_hub_stop.body["runId"].as_str(),
            Some(idle_id.as_str())
        );
        assert_eq!(current_hub_stop.body["stopRequested"], true);

        drop(serve_a);
        drop(serve_b);
        drop(gate);
        fs::remove_dir_all(&project).ok();
    }
    assert!(
        failures.is_empty(),
        "detached name-stop Red failures: {failures:#?}"
    );
}

struct SqliteBranchFixture {
    dir: PathBuf,
    run_id: String,
    branch_id: String,
    branch_dir: PathBuf,
}

impl Drop for SqliteBranchFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn create_sqlite_branch_fixture(
    server: &LocalHttpServer,
    pause_for_resume: bool,
) -> SqliteBranchFixture {
    let dir = temp_project("sqlite-branch-preflight");
    let branch_source = dir.join("branch.ts");
    let branch_body = if pause_for_resume {
        format!(
            r#"
                export async function agent() {{
                    await chidori.input("continue?", {{ default: "yes" }});
                    await fetch("http://{}/effect", {{ method: "POST" }});
                    return {{ complete: true }};
                }}
            "#,
            server.address
        )
    } else {
        format!(
            r#"
                export async function agent() {{
                    await fetch("http://{}/effect", {{ method: "POST" }});
                    return {{ complete: true }};
                }}
            "#,
            server.address
        )
    };
    fs::write(&branch_source, branch_body).unwrap();
    let agent = dir.join("agent.ts");
    fs::write(
        &agent,
        format!(
            r#"
                export async function agent() {{
                    const outcomes = await chidori.branch([
                        {{ label: "work", source: "{}", input: {{}} }}
                    ]);
                    return {{ outcomes }};
                }}
            "#,
            branch_source.display()
        ),
    )
    .unwrap();
    let envs = [
        ("CHIDORI_DURABILITY", "effect"),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    let output =
        run_chidori_with_str_env(&["run", agent.to_str().unwrap(), "--trusted"], &dir, &envs);
    assert_success(&output);
    let run_id = first_run_id(&dir);
    let run_output: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let branch_id = run_output["outcomes"][0]["branchId"]
        .as_str()
        .expect("CLI run must return a branch id")
        .to_string();
    let run_dir = dir.join(".chidori").join("runs").join(&run_id);
    let branch_dir = fs::read_dir(run_dir.join("branches"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find_map(|op_dir| {
            let candidate = op_dir.join("branch-000");
            candidate.join("branch.json").is_file().then_some(candidate)
        })
        .expect("CLI run must persist the branch store");

    if pause_for_resume {
        let meta_path = branch_dir.join("branch.json");
        let mut meta: serde_json::Value =
            serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
        let checkpoint_path = branch_dir.join("checkpoint.json");
        let checkpoint: Vec<serde_json::Value> =
            serde_json::from_slice(&fs::read(&checkpoint_path).unwrap()).unwrap();
        let input = checkpoint
            .iter()
            .find(|record| record["function"] == "input")
            .expect("default input should have a persisted input row");
        meta["status"] = serde_json::json!("paused");
        meta["pending_prompt"] = serde_json::json!("continue?");
        meta["pending_input"] = serde_json::json!({
            "seq": input["seq"],
            "prompt": "continue?",
        });
        meta["output"] = serde_json::Value::Null;
        fs::write(&meta_path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
        // The pending input is the branch's replay boundary. Preserve no rows
        // after it so resume must continue to the HTTP side effect.
        fs::write(&checkpoint_path, b"[]").unwrap();
    }

    SqliteBranchFixture {
        dir,
        run_id,
        branch_id,
        branch_dir,
    }
}

fn assert_invalid_sqlite_branch_command_fails_before_mutation(
    fixture: &SqliteBranchFixture,
    server: &LocalHttpServer,
    command: &[&str],
    expected_requests: usize,
) {
    let db_path = fixture.dir.join("sqlite-db-is-a-directory");
    fs::create_dir_all(&db_path).unwrap();
    let db_path_text = db_path.to_string_lossy().to_string();
    let envs = [
        ("CHIDORI_RUN_STORE", "sqlite"),
        ("CHIDORI_RUN_DB", db_path_text.as_str()),
        ("CHIDORI_DURABILITY", "strict"),
        ("CHIDORI_HTTP_ALLOW_HOSTS", "127.0.0.1"),
    ];
    let tracked = [
        fixture.branch_dir.join("branch.json"),
        fixture.branch_dir.join("checkpoint.json"),
        fixture.branch_dir.join("source.ts"),
        fixture.branch_dir.join("history").join("commits.jsonl"),
    ];
    let before: Vec<_> = tracked
        .into_iter()
        .map(|path| {
            let contents = fs::read(&path).unwrap_or_default();
            (path, contents)
        })
        .collect();
    let output = run_chidori_with_str_env(command, &fixture.dir, &envs);

    // The external request count comes before status and diagnostic checks so
    // vulnerable CLI wiring reports the live branch effect directly.
    assert_eq!(
        server.request_count(),
        expected_requests,
        "branch command performed an HTTP effect before rejecting invalid SQLite"
    );
    assert_failure(&output);
    assert_diagnostic_contains(&output, &db_path_text);
    assert_diagnostic_contains(&output, "unable to open database file");
    for (path, contents) in before {
        assert_eq!(
            fs::read(&path).unwrap_or_default(),
            contents,
            "mutated {}",
            path.display()
        );
    }
}

#[test]
fn cli_branch_resume_rejects_invalid_sqlite_before_branch_effect_or_mutation() {
    let server = LocalHttpServer::start();
    let fixture = create_sqlite_branch_fixture(&server, true);
    assert_eq!(server.request_count(), 1, "fixture's initial HTTP effect");
    let run_id = fixture.run_id.as_str();
    let branch_id = fixture.branch_id.as_str();
    assert_invalid_sqlite_branch_command_fails_before_mutation(
        &fixture,
        &server,
        &[
            "branch-resume",
            run_id,
            branch_id,
            "--value",
            "yes",
            "--dir",
            fixture.dir.to_str().unwrap(),
            "--trusted",
        ],
        1,
    );
}

#[test]
fn cli_branch_rerun_rejects_invalid_sqlite_before_branch_effect_or_mutation() {
    let server = LocalHttpServer::start();
    let fixture = create_sqlite_branch_fixture(&server, false);
    assert_eq!(server.request_count(), 1, "fixture's initial HTTP effect");
    let run_id = fixture.run_id.as_str();
    let branch_id = fixture.branch_id.as_str();
    assert_invalid_sqlite_branch_command_fails_before_mutation(
        &fixture,
        &server,
        &[
            "branch-rerun",
            run_id,
            branch_id,
            "--dir",
            fixture.dir.to_str().unwrap(),
            "--trusted",
        ],
        1,
    );
}
