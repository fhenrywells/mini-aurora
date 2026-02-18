/// End-to-end crash recovery test.
///
/// Spawns `mini-aurora crash-writer <wal-path>` as a subprocess, waits until
/// 5 pages have been durably committed (signalled via stdout), then sends SIGTERM.
/// After the process dies, the WAL is reopened — triggering the VCL/VDL scan and
/// truncation logic in `crates/wal/src/recovery.rs` — and the test verifies that
/// all 5 pages are still correctly materializable.
///
/// This demonstrates the core Aurora durability guarantee: a process can be killed
/// at any point and the log-backed store will recover to the last durable state.
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mini_aurora_common::StorageApi;
use mini_aurora_compute::engine::ComputeEngine;
use mini_aurora_storage::engine::StorageEngine;
use tempfile::TempDir;

#[cfg(unix)]
#[tokio::test]
async fn crash_recovery_via_sigterm() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("crash.wal");
    let wal_path_str = wal_path.to_str().unwrap();

    // -----------------------------------------------------------------------
    // Phase 1: Spawn crash-writer and wait until 5 pages are durably written.
    // -----------------------------------------------------------------------
    let mut child = Command::new(env!("CARGO_BIN_EXE_mini-aurora"))
        .args(["crash-writer", wal_path_str])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()) // surface crash-writer's eprintln! in test output
        .spawn()
        .expect("Failed to spawn mini-aurora crash-writer");

    let stdout = child.stdout.take().unwrap();

    let vdl_reached = {
        let mut reader = BufReader::new(stdout);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reached = false;
        let mut line = String::new();

        loop {
            if Instant::now() > deadline {
                panic!("Timed out waiting for crash-writer to reach VDL=5");
            }
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break, // EOF — process exited unexpectedly
                Ok(_) => {
                    if line.trim() == "VDL=5" {
                        reached = true;
                        break;
                    }
                }
                Err(e) => panic!("Error reading crash-writer stdout: {e}"),
            }
        }
        reached
        // reader drops here, closing our end of the stdout pipe
    };

    assert!(vdl_reached, "crash-writer exited before reaching VDL=5");

    // -----------------------------------------------------------------------
    // Phase 2: Send SIGTERM — simulates a node crash.
    // -----------------------------------------------------------------------
    let pid = child.id();
    Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("Failed to send SIGTERM to crash-writer");

    // Wait for the process to actually die (it has no signal handler so it
    // should terminate immediately).
    child.wait().expect("crash-writer did not exit after SIGTERM");

    // -----------------------------------------------------------------------
    // Phase 3: Reopen the WAL — recovery runs automatically in open().
    //
    // Recovery performs:
    //   1. Forward scan to find all valid LSNs and CPLs
    //   2. Compute VCL (highest contiguous LSN)
    //   3. Compute VDL (highest MTR-complete LSN ≤ VCL)
    //   4. Truncate WAL at VDL (discard any incomplete MTR)
    //   5. Rebuild page_index and lsn_offsets
    // -----------------------------------------------------------------------
    let storage = Arc::new(StorageEngine::open(&wal_path).expect("Recovery failed"));
    let state = storage
        .get_durability_state()
        .await
        .expect("get_durability_state failed");

    assert!(
        state.vdl >= 5,
        "Expected VDL >= 5 after recovery, got VDL={}",
        state.vdl
    );

    // -----------------------------------------------------------------------
    // Phase 4: Materialize pages 1–5 and verify their content survived.
    // -----------------------------------------------------------------------
    let compute = ComputeEngine::new(storage, 256);
    compute
        .refresh_read_point()
        .await
        .expect("refresh_read_point failed");

    for page_id in 1u64..=5 {
        let page = compute
            .get(page_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to read page {page_id} after recovery: {e}"));

        // crash-writer writes format!("page-{page_id}") at offset 0
        let expected = format!("page-{page_id}");
        let actual = std::str::from_utf8(&page[..expected.len()])
            .expect("Page content is not valid UTF-8");

        assert_eq!(
            actual, expected,
            "Page {page_id} has wrong content after recovery"
        );
    }
}
