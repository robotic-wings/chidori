use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

const CHILD_MODE: &str = "CHIDORI_TEST_PUBLIC_SERVE_OUTPUT_CHILD";
const PROJECT_DIR: &str = "CHIDORI_TEST_PUBLIC_SERVE_OUTPUT_PROJECT";
const PORT_ENV: &str = "CHIDORI_TEST_PUBLIC_SERVE_OUTPUT_PORT";
const TRACE_PATH: &str = "CHIDORI_TEST_PUBLIC_SERVE_OUTPUT_TRACE";

fn chidori_bin() -> &'static str {
    env!("CARGO_BIN_EXE_chidori")
}

fn free_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_health(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_error = String::new();
    while Instant::now() < deadline {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                stream
                    .write_all(
                        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).unwrap();
                assert!(
                    response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200"),
                    "health returned an unexpected response: {response}"
                );
                return;
            }
            Err(error) => {
                last_error = error.to_string();
                std::thread::yield_now();
            }
        }
    }
    panic!("server did not become healthy on 127.0.0.1:{port}: {last_error}");
}

fn seed_hibernating_detached_agent(project: &Path) {
    fs::write(
        project.join("worker.ts"),
        r#"import { chidori, run } from "chidori:agent";
           run(async () => { await chidori.signal("never"); return { done: true }; });"#,
    )
    .unwrap();
    let seed = project.join("seed.ts");
    fs::write(
        &seed,
        r#"import { chidori, run } from "chidori:agent";
           run(async () => {
             const agent = await chidori.agents.spawn("worker.ts", {}, {
               name: "logging-probe",
               restart: "resume"
             });
             const deadline = Date.now() + 15000;
             let status = await agent.status();
             while (status.status !== "hibernating" && Date.now() < deadline) {
               status = await agent.status();
             }
             if (status.status !== "hibernating") throw new Error(JSON.stringify(status));
             return { runId: agent.runId, status: status.status };
           });"#,
    )
    .unwrap();
    let output = Command::new(chidori_bin())
        .args(["run", seed.to_str().unwrap(), "--trusted", "--no-isolate"])
        .current_dir(project)
        .env("CHIDORI_RUN_STORE", "fs")
        .env_remove("CHIDORI_RUN_DB")
        .env_remove("CHIDORI_DURABILITY")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "failed to seed the persisted detached agent: status={:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let descriptor = project.join(".chidori/runs/agents/logging-probe.json");
    assert!(descriptor.exists(), "seed did not persist a registry entry");
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&descriptor).unwrap()).unwrap();
    assert_eq!(value["descriptor"]["status"], "hibernating");
}

fn run_public_serve_child() {
    let project = PathBuf::from(std::env::var_os(PROJECT_DIR).unwrap());
    let port = std::env::var(PORT_ENV).unwrap().parse::<u16>().unwrap();
    let trace_path = PathBuf::from(std::env::var_os(TRACE_PATH).unwrap());
    let trace_file = fs::File::create(trace_path).unwrap();
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .with_target(false)
        .with_writer(trace_file)
        .try_init()
        .expect("install child-only JSON tracing subscriber");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    runtime.block_on(async move {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let project_for_server = project.clone();
        let server = tokio::spawn(async move {
            tokio::select! {
                result = chidori::server::serve(
                    Arc::new(chidori::providers::ProviderRegistry::new()),
                    Arc::new(chidori::runtime::template::TemplateEngine::new(project_for_server)),
                    None,
                    "127.0.0.1".to_string(),
                    port,
                    Arc::new(chidori::policy::PolicyConfig::default()),
                    "trusted".to_string(),
                    None,
                ) => result.unwrap(),
                _ = shutdown_rx => {},
            }
        });

        wait_for_health(port);
        tokio::time::timeout(Duration::from_secs(3), async {
            shutdown_tx.send(()).expect("shutdown receiver is active");
            server.await.unwrap();
        })
        .await
        .expect("server child should stop through bounded shutdown control");
    });
}

fn spawn_cli_serve(project: &Path, port: u16) -> std::process::Child {
    Command::new(chidori_bin())
        .args([
            "serve",
            "--port",
            &port.to_string(),
            "--trusted",
            "--no-isolate",
        ])
        .current_dir(project)
        .env("CHIDORI_RUN_STORE", "fs")
        .env_remove("CHIDORI_RUN_DB")
        .env_remove("CHIDORI_DURABILITY")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn cli_serve_until_healthy(project: &Path, port: u16) -> Output {
    let mut guard = ChildGuard(Some(spawn_cli_serve(project, port)));
    wait_for_health(port);
    guard
        .0
        .as_mut()
        .unwrap()
        .kill()
        .expect("stop CLI control after health readiness");
    guard.0.take().unwrap().wait_with_output().unwrap()
}

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn public_serve_routes_successful_startup_status_to_tracing_not_stderr() {
    if std::env::var_os(CHILD_MODE).is_some() {
        run_public_serve_child();
        return;
    }

    let project = TempDir::new().unwrap();
    seed_hibernating_detached_agent(project.path());

    let public_port = free_loopback_port();
    let trace_path = project.path().join("public-serve-trace.jsonl");
    let host_child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "public_serve_routes_successful_startup_status_to_tracing_not_stderr",
            "--nocapture",
        ])
        .current_dir(project.path())
        .env(CHILD_MODE, "1")
        .env(PROJECT_DIR, project.path())
        .env(PORT_ENV, public_port.to_string())
        .env(TRACE_PATH, &trace_path)
        .env("CHIDORI_RUN_STORE", "fs")
        .env_remove("CHIDORI_RUN_DB")
        .env_remove("CHIDORI_DURABILITY")
        .output()
        .unwrap();
    assert!(
        host_child.status.success(),
        "public serve child failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        host_child.status,
        String::from_utf8_lossy(&host_child.stdout),
        String::from_utf8_lossy(&host_child.stderr)
    );
    let host_stderr = String::from_utf8_lossy(&host_child.stderr);
    let leaked_status_lines: Vec<&str> = host_stderr
        .lines()
        .filter(|line| {
            let status = line.trim_start();
            status.starts_with("Re-armed ")
                || status.starts_with("Listening on http://")
                || status.starts_with("Concurrency:")
                || status.starts_with("Auth:")
                || status.starts_with("Policy:")
        })
        .collect();
    assert!(
        leaked_status_lines.is_empty(),
        "public server wrote successful lifecycle status to embedding-host stderr; leaked lines={leaked_status_lines:?}; full stderr:\n{host_stderr}"
    );

    let trace = fs::read_to_string(&trace_path).unwrap_or_default();
    let events: Vec<serde_json::Value> = trace
        .lines()
        .map(|line| serde_json::from_str(line).expect("trace output is JSONL"))
        .collect();
    assert!(
        events.iter().any(|event| event["message"]
            .as_str()
            .is_some_and(|message| message.contains("Listening on http://"))),
        "structured tracing should expose the successful listening lifecycle event: {trace}"
    );
    assert!(
        events
            .iter()
            .any(|event| event["message"].as_str().is_some_and(|message| {
                message.contains("Re-armed 1 detached agent(s) from the registry")
            })),
        "structured tracing should expose the successful detached rearm lifecycle event: {trace}"
    );
}

#[test]
fn cli_serve_retains_user_facing_startup_and_rearm_status() {
    let project = TempDir::new().unwrap();
    seed_hibernating_detached_agent(project.path());
    let cli_port = free_loopback_port();
    let cli = cli_serve_until_healthy(project.path(), cli_port);
    let cli_stderr = String::from_utf8_lossy(&cli.stderr);
    assert!(
        cli_stderr.contains("Listening on http://"),
        "CLI should retain its user-facing listening status:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("Re-armed 1 detached agent(s) from the registry"),
        "CLI should retain its user-facing successful rearm status:\n{cli_stderr}"
    );
}
