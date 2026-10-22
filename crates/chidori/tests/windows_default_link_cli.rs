//! Windows default-link smoke coverage for CLI startup.
//!
//! This invokes Cargo's normally linked test binary as a child, rather than
//! calling the CLI parser in-process, so it catches main-thread stack failures.

#[cfg(windows)]
#[test]
fn default_linked_cli_version_starts_and_reports_the_package_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_chidori"))
        .arg("--version")
        .output()
        .expect("run default-linked chidori --version");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success()
            && stdout.starts_with("chidori ")
            && stdout.contains(env!("CARGO_PKG_VERSION")),
        "default-linked chidori --version must start and report its package version; \
         status={:?}; stdout={stdout:?}; stderr={stderr:?}",
        output.status,
    );
}
