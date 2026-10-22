#[cfg(not(windows))]
#[test]
fn effect_direct_sqlite_intent_barrier_on_production_serve() {
    let status = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/effect_sqlite_production_serve.py"
        ))
        .env("CHIDORI_BIN", env!("CARGO_BIN_EXE_chidori"))
        .status()
        .expect("python3 must be available on the Linux CI runner");
    assert!(status.success(), "production serve oracle failed: {status}");
}
