use std::fs;
use std::net::TcpListener;
use std::process::Command;

#[test]
#[ignore = "local CLI/socket startup boundary; run with CHIDORI_TEST_MANIFEST_ROUTE_OVERLAP_OPT_IN=1"]
fn manifest_route_conflict_precedes_bind_failure_and_keep_alive_spawn() {
    assert_eq!(
        std::env::var("CHIDORI_TEST_MANIFEST_ROUTE_OVERLAP_OPT_IN").as_deref(),
        Ok("1"),
        "set CHIDORI_TEST_MANIFEST_ROUTE_OVERLAP_OPT_IN=1 to run this local integration case"
    );

    let project = tempfile::tempdir().unwrap();
    fs::write(project.path().join("worker.ts"), "export {};\n").unwrap();
    let manifest = project.path().join("chidori.app.yml");
    fs::write(
        &manifest,
        "agents:\n  - name: keepalive-probe\n    agent: worker.ts\n    keep_alive: true\nroutes:\n  - path: /hooks/{id}\n    agent: keepalive-probe\n    signal: by-id\n  - path: /hooks/{event}\n    agent: keepalive-probe\n    signal: by-event\n",
    )
    .unwrap();

    let held_port = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = held_port.local_addr().unwrap().port();
    let output = Command::new(env!("CARGO_BIN_EXE_chidori"))
        .args([
            "serve",
            "--app",
            manifest.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--trusted",
            "--no-isolate",
        ])
        .current_dir(project.path())
        .env("CHIDORI_RUN_STORE", "fs")
        .env_remove("CHIDORI_RUN_DB")
        .env_remove("CHIDORI_DURABILITY")
        .output()
        .unwrap();
    drop(held_port);

    let stderr = String::from_utf8_lossy(&output.stderr);
    let descriptor = project
        .path()
        .join(".chidori/runs/agents/keepalive-probe.json");
    let mut violations = Vec::new();
    if !stderr.contains("/hooks/{id}") || !stderr.contains("/hooks/{event}") {
        violations.push("stderr lacks the route-conflict diagnostic naming both paths");
    }
    if stderr.contains("Address already in use") {
        violations.push("listener bind failure took precedence over route validation");
    }
    if descriptor.exists() {
        violations.push("keep_alive agent was persisted before invalid manifest rejection");
    }
    assert!(
        !output.status.success() && violations.is_empty(),
        "invalid manifest must fail before bind/fleet startup; status={:?}, violations={violations:?}, stderr:\n{stderr}",
        output.status
    );
}
