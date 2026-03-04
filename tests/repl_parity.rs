use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Mutex;

static PARITY_LOCK: Mutex<()> = Mutex::new(());

fn lock_parity() -> std::sync::MutexGuard<'static, ()> {
    PARITY_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn run_cli_with_input(args: &[&str], input: &str) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mini-aurora"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn mini-aurora");

    {
        let mut stdin = child.stdin.take().expect("child stdin unavailable");
        stdin
            .write_all(input.as_bytes())
            .expect("failed to write input");
    }

    let output = child
        .wait_with_output()
        .expect("failed to wait for mini-aurora");
    assert!(
        output.status.success(),
        "process exited non-zero: {}",
        output.status
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[test]
fn shared_repl_commands_are_supported_in_viz_repl() {
    let _guard = lock_parity();
    let _ = std::fs::remove_file("/tmp/mini-aurora-repl.wal");
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");

    let shared_sequence = "clear\nput 1 0 hi\nput-random 2\nget 1\nget-raw 1\ndel 1 0 1\nclear-page 1\ncompact\nstate\nquit\n";

    let repl_output = run_cli_with_input(&["repl"], shared_sequence);
    assert!(
        !repl_output.contains("Unknown command"),
        "repl rejected shared command set:\n{repl_output}"
    );

    let viz_output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        shared_sequence,
    );
    assert!(
        !viz_output.contains("Unknown command"),
        "viz-repl rejected shared command set:\n{viz_output}"
    );
}

#[test]
fn viz_repl_clear_resets_data() {
    let _guard = lock_parity();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");

    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "put 1 0 hello\nclear\nget 1\nquit\n",
    );

    assert!(output.contains("OK (database cleared)"));
    assert!(output.contains("Error: page 1 not found"));
}
