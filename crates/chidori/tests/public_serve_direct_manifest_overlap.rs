use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;

use chidori::app_manifest::{AppManifest, ManifestRoute};
use chidori::policy::PolicyConfig;
use chidori::providers::ProviderRegistry;
use chidori::runtime::template::TemplateEngine;
use chidori::server::serve;

const OPT_IN: &str = "CHIDORI_TEST_PUBLIC_SERVE_DIRECT_MANIFEST_OVERLAP_OPT_IN";

fn require_opt_in(test_name: &str) {
    assert_eq!(
        std::env::var(OPT_IN).as_deref(),
        Ok("1"),
        "{test_name} requires a local socket test; set {OPT_IN}=1 and pass --ignored"
    );
}

fn direct_manifest(base_dir: &Path, routes: Vec<ManifestRoute>) -> AppManifest {
    AppManifest {
        name: Some("direct-api-test".to_string()),
        agents: Vec::new(),
        routes,
        base_dir: base_dir.to_path_buf(),
    }
}

fn direct_manifest_route(path: &str) -> ManifestRoute {
    ManifestRoute {
        path: path.to_string(),
        agent: "probe".to_string(),
        signal: "probe".to_string(),
    }
}

async fn public_serve_outcome(project: &Path, manifest: AppManifest, port: u16) -> String {
    let agent_path = project.join("agent.ts");
    std::fs::write(&agent_path, "export {};\n").unwrap();
    let server = tokio::spawn(serve(
        Arc::new(ProviderRegistry::new()),
        Arc::new(TemplateEngine::new(project)),
        Some(agent_path),
        "127.0.0.1".to_string(),
        port,
        Arc::new(PolicyConfig::default()),
        "trusted".to_string(),
        Some(manifest),
    ));

    match server.await {
        Ok(Ok(())) => "returned Ok(())".to_string(),
        Ok(Err(err)) => format!("returned Err: {err:#}"),
        Err(join_error) if join_error.is_panic() => {
            let payload = join_error.into_panic();
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_else(|| "non-string panic payload".to_string());
            format!("caught task panic: {message}")
        }
        Err(join_error) => format!("task failed without panic: {join_error}"),
    }
}

#[tokio::test]
#[ignore = "local public-server socket test; requires explicit opt-in"]
async fn public_serve_rejects_direct_overlapping_manifest_routes_before_bind() {
    require_opt_in("public_serve_rejects_direct_overlapping_manifest_routes_before_bind");
    let project = tempfile::tempdir().unwrap();
    let manifest = direct_manifest(
        project.path(),
        vec![
            direct_manifest_route("/hooks/{id}"),
            direct_manifest_route("/hooks/{event}"),
        ],
    );
    let held_port = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = held_port.local_addr().unwrap().port();

    let outcome = public_serve_outcome(project.path(), manifest, port).await;
    let normalized = outcome.to_ascii_lowercase();
    assert!(
        outcome.starts_with("returned Err:")
            && (normalized.contains("overlap") || normalized.contains("conflict"))
            && outcome.contains("/hooks/{id}")
            && outcome.contains("/hooks/{event}"),
        "public serve should return a contextual route-conflict error before bind; observed {outcome}"
    );
}

#[tokio::test]
#[ignore = "local public-server socket test; requires explicit opt-in"]
async fn public_serve_accepts_direct_valid_manifest_before_occupied_port_error() {
    require_opt_in("public_serve_accepts_direct_valid_manifest_before_occupied_port_error");
    let project = tempfile::tempdir().unwrap();
    let manifest = direct_manifest(project.path(), vec![direct_manifest_route("/hooks/{id}")]);
    let held_port = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = held_port.local_addr().unwrap().port();

    let outcome = public_serve_outcome(project.path(), manifest, port).await;
    assert!(
        outcome.starts_with("returned Err:") && outcome.contains("Address already in use"),
        "valid direct manifest should reach the occupied listener and return its bind error; observed {outcome}"
    );
}
