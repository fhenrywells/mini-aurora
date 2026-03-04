use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Mutex;

static REPL_LOCK: Mutex<()> = Mutex::new(());

fn lock_repl() -> std::sync::MutexGuard<'static, ()> {
    REPL_LOCK.lock().unwrap_or_else(|e| e.into_inner())
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
fn repl_logical_delete_and_compaction_sequence() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-repl.wal");

    let output = run_cli_with_input(
        &["repl"],
        "clear\nput 1 0 hello\ndel 1 0 5\ncompact\nget 1\nquit\n",
    );

    assert!(output.contains("OK (deleted 5 byte(s), VDL="));
    assert!(output.contains("OK (compacted:"));
    assert!(output.contains("Error: page 1 not found"));
}

#[test]
fn repl_compact_preserves_live_pages() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-repl.wal");

    let output = run_cli_with_input(
        &["repl"],
        "clear\nput 1 0 gone\nput 2 0 keep\ndel 1 0 4\ncompact\nget 1\nget 2\nquit\n",
    );

    assert!(output.contains("OK (compacted:"));
    assert!(output.contains("Error: page 1 not found"));
    assert!(output.contains("\"keep\""));
}

#[test]
fn repl_accepts_semicolon_command_sequences() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-repl.wal");

    let output = run_cli_with_input(&["repl"], "clear; put 1 0 hi; get 1; quit\n");
    assert!(output.contains("OK (database cleared)"));
    assert!(output.contains("OK (VDL="));
    assert!(output.contains("\"hi\""));
}

#[test]
fn viz_repl_put_switch_refresh_get_sequence() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");
    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "put 1 0 hello\nnode B\nget 1\nrefresh\nget 1\nquit\n",
    );

    assert!(output.contains("Switched to Node B"));
    assert!(output.contains("Error: page 1 not found"));
    assert!(output.contains("\"hello\""));
}

#[test]
fn viz_repl_accepts_semicolon_command_sequences() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");

    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "put 1 0 hello; node B; refresh; get 1; quit\n",
    );
    assert!(output.contains("Switched to Node B"));
    assert!(output.contains("\"hello\""));
}

#[test]
fn viz_repl_compact_after_page_clear() {
    let _guard = lock_repl();
    let _ = std::fs::remove_dir_all("/tmp/mini-aurora-viz-tiered");

    let output = run_cli_with_input(
        &["viz-repl", "--preset", "tiered", "--no-color", "--delay", "0"],
        "put 1 0 hello\nclear-page 1\ncompact\nquit\n",
    );

    assert!(output.contains("OK (page 1 cleared, VDL="));
    assert!(output.contains("OK (compacted:"));
}

#[test]
fn viz_repl_compact_rejected_with_running_workers() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");
    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "bg A write 50\ncompact\nbg stop A\nquit\n",
    );

    assert!(output.contains("Started bg write worker on Node A every 50ms"));
    assert!(output.contains("Error compacting storage: stop background workers before compacting"));
}

#[test]
fn viz_repl_single_mode_suggests_del_and_compact() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");

    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "put 1 0 hello\nquit\n",
    );
    assert!(output.contains("del 1 0 1"));
    assert!(output.contains("compact"));
}

#[test]
fn viz_repl_sequence_mode_runs_suggested_flow() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");

    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "suggest sequence\nput 1 0 hello\n1\nquit\n",
    );
    assert!(output.contains("Suggestion mode set to command-sequences."));
    assert!(output.contains(">>> suggested: get 1; del 1 0 1; get-raw 1"));
    assert!(output.contains("OK (deleted 1 byte(s), VDL="));
}

#[test]
fn viz_repl_does_not_suggest_clear_when_empty() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");

    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "state\nquit\n",
    );
    assert!(!output.contains("[1] clear"));
    assert!(!output.contains("[2] clear"));
    assert!(!output.contains("[3] clear"));
}

#[test]
fn viz_repl_does_not_repeat_last_command_in_suggestions() {
    let _guard = lock_repl();
    let _ = std::fs::remove_file("/tmp/mini-aurora-viz-repl.wal");

    let output = run_cli_with_input(
        &["viz-repl", "--preset", "base", "--no-color", "--delay", "0"],
        "put-random 3\nquit\n",
    );
    assert!(output.contains("OK (inserted 3 random string(s)"));
    assert!(!output.contains("[1] put-random 3 "));
    assert!(!output.contains("[2] put-random 3 "));
    assert!(!output.contains("[3] put-random 3 "));
}
