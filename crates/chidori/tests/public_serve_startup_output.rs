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
const OPT_IN: &str = "CHIDORI_TEST_PUBLIC_SERVE_OUTPUT_OPT_IN";

fn require_opt_in(test_name: &str) -> bool {
    if std::env::var(OPT_IN).as_deref() == Ok("1") {
        true
    } else {
        eprintln!(
            "skipping {test_name}: set {OPT_IN}=1 to run loopback subprocess integration test"
        );
        false
    }
}

fn chidori_bin() -> &'static str {
    env!("CARGO_BIN_EXE_chidori")
}

fn free_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_health(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_diagnostic = "no request attempted".to_string();
    while Instant::now() < deadline {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut stream) => {
                if let Err(error) = stream.set_read_timeout(Some(Duration::from_millis(500))) {
                    last_diagnostic = format!("setting read timeout failed: {error}");
                } else if let Err(error) = stream.write_all(
                    b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                ) {
                    last_diagnostic = format!("writing health request failed: {error}");
                } else {
                    let mut response = String::new();
                    match stream.read_to_string(&mut response) {
                        Ok(_)
                            if response.starts_with("HTTP/1.1 200")
                                || response.starts_with("HTTP/1.0 200") =>
                        {
                            return;
                        }
                        Ok(_) => {
                            last_diagnostic =
                                format!("health returned non-200 response: {response}");
                        }
                        Err(error) => {
                            last_diagnostic = format!(
                                "reading health response failed: {error}; partial response: {response}"
                            );
                        }
                    }
                }
            }
            Err(error) => last_diagnostic = format!("connecting failed: {error}"),
        }
        std::thread::yield_now();
    }
    panic!("server did not become healthy on 127.0.0.1:{port} before deadline: {last_diagnostic}");
}

fn is_server_startup_status_line(line: &str) -> bool {
    let line = line.trim_start();
    [
        "App fleet:",
        "Re-armed ",
        "Listening on http://",
        "WARNING: non-loopback bind",
        "Concurrency:",
        "Auth:",
        "Policy:",
        "CORS:",
        "Events:",
        "Sessions:",
        "Health:",
        "GET ",
        "POST ",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
}

fn trace_message(event: &serde_json::Value) -> Option<&str> {
    event["fields"]["message"]
        .as_str()
        .or_else(|| event["message"].as_str())
}

fn write_manifest(project: &Path) {
    fs::write(
        project.join("chidori.app.yml"),
        "name: startup-output\nagents:\n  - name: manifest-probe\n    agent: worker.ts\n    keep_alive: true\nroutes:\n  - path: /hooks/example\n    agent: manifest-probe\n    signal: event\n",
    )
    .unwrap();
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
    let manifest =
        chidori::app_manifest::AppManifest::load(&project.join("chidori.app.yml")).unwrap();
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
                    Some(manifest),
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
        .env("CHIDORI_SERVE_ROUTES", "strict")
        .env("CHIDORI_CORS_ORIGINS", "http://allowed.example")
        .env("CHIDORI_MAX_CONCURRENT_SESSIONS", "3")
        .env("CHIDORI_ACQUIRE_TIMEOUT_MS", "2500")
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
#[ignore = "loopback subprocess integration; set CHIDORI_TEST_PUBLIC_SERVE_OUTPUT_OPT_IN=1 and use --ignored"]
fn public_serve_routes_successful_startup_status_to_tracing_not_stderr() {
    if !require_opt_in("public_serve_routes_successful_startup_status_to_tracing_not_stderr") {
        return;
    }
    if std::env::var_os(CHILD_MODE).is_some() {
        run_public_serve_child();
        return;
    }

    let project = TempDir::new().unwrap();
    seed_hibernating_detached_agent(project.path());
    write_manifest(project.path());

    let public_port = free_loopback_port();
    let trace_path = project.path().join("public-serve-trace.jsonl");
    let host_child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "public_serve_routes_successful_startup_status_to_tracing_not_stderr",
            "--ignored",
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
        .filter(|line| is_server_startup_status_line(line))
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
        events.iter().any(|event| {
            trace_message(event).is_some_and(|message| message.contains("Listening on http://"))
        }),
        "structured tracing should expose the successful listening lifecycle event: {trace}"
    );
    assert!(
        events.iter().any(|event| {
            trace_message(event).is_some_and(|message| {
                message.contains("Re-armed 1 detached agent(s) from the registry")
            })
        }),
        "structured tracing should expose the successful detached rearm lifecycle event: {trace}"
    );
    assert!(
        events.iter().any(|event| {
            trace_message(event).is_some_and(|message| message.contains("server routes configured"))
        }),
        "structured tracing should expose startup and route configuration: {trace}"
    );
    let route_event = events.iter().find(|event| {
        trace_message(event).is_some_and(|message| message.contains("server routes configured"))
    });
    assert!(
        route_event.is_some_and(|event| {
            event["fields"]["health_route"] == "GET /health"
                && event["fields"]["routes"]
                    .as_str()
                    .is_some_and(|routes| routes.contains("GET /health"))
        }),
        "structured route event should include health and the route map: {trace}"
    );
    assert!(
        events.iter().any(|event| {
            trace_message(event)
                .is_some_and(|message| message.contains("spawned app manifest agent"))
                && event["fields"]["agent"] == "manifest-probe"
                && event["fields"]["run_id"]
                    .as_str()
                    .is_some_and(|run_id| !run_id.is_empty())
        }),
        "successful manifest spawn tracing should include agent and run id: {trace}"
    );
}

#[test]
#[ignore = "loopback subprocess integration; set CHIDORI_TEST_PUBLIC_SERVE_OUTPUT_OPT_IN=1 and use --ignored"]
fn cli_serve_retains_user_facing_startup_and_rearm_status() {
    if !require_opt_in("cli_serve_retains_user_facing_startup_and_rearm_status") {
        return;
    }
    let project = TempDir::new().unwrap();
    seed_hibernating_detached_agent(project.path());
    write_manifest(project.path());
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
    assert!(
        cli_stderr.contains("App fleet: spawned `manifest-probe` (run "),
        "CLI should retain successful manifest spawn and run id notice:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("Agent: none — fleet-only server"),
        "CLI should retain its agent summary:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains(
            "App manifest: startup-output — 1 agent(s) (1 kept alive, 0 scheduled), 1 route(s)"
        ),
        "CLI should retain its app manifest summary:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("Isolation:"),
        "CLI should retain its isolation summary:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("Concurrency: max 3 sessions, 2500ms acquire timeout"),
        "CLI should format concurrency from the startup snapshot:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("Auth:        disabled (loopback bind; set CHIDORI_API_KEY to enable)"),
        "CLI should format authentication from the startup snapshot:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("Policy:      trusted"),
        "CLI should format policy from the startup snapshot:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("CORS:        allow: http://allowed.example"),
        "CLI should format CORS from the startup snapshot:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("Events:     ANY /events      → agent(event); strict routes"),
        "CLI should format the selected route mode from the startup snapshot:\n{cli_stderr}"
    );
    assert!(
        cli_stderr.contains("POST /sessions   → create & run")
            && cli_stderr.contains("GET  /health"),
        "CLI should retain its documented route and health summaries:\n{cli_stderr}"
    );
}
