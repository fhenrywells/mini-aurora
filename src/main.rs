use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mini_aurora_common::{PageId, StorageApi, PAGE_SIZE};
use mini_aurora_compute::engine::ComputeEngine;
use mini_aurora_storage::engine::StorageEngine;
use mini_aurora_wal::recovery::recover;
use tokio_util::sync::CancellationToken;

mod utils;
mod viz;

use utils::repl::{
    ensure_range_in_page, parse_len, parse_offset, parse_page_id, print_page_raw, print_page_text,
};
use viz::compute::VizComputeEngine;
use viz::engine::VizStorageEngine;
use viz::events::VizConfig;
use viz::renderer::VizRenderer;
use viz::tracer::JsonTracer;

mod scenario;

// ---------------------------------------------------------------------------
// Viz REPL types
// ---------------------------------------------------------------------------

struct ReplState {
    nodes: HashMap<String, Arc<VizComputeEngine>>,
    current_node: String,
    suggestions: Vec<String>,
    workers: HashMap<String, WorkerHandle>,
    storage: Arc<VizStorageEngine>,
    renderer: Arc<Mutex<VizRenderer>>,
    bg_page_counter: Arc<AtomicU64>,
    bg_output_tx: tokio::sync::mpsc::UnboundedSender<String>,
    storage_mode: VizStorageMode,
    suggestion_mode: SuggestionMode,
}

enum VizStorageMode {
    Base { wal_path: PathBuf },
    Tiered {
        base_dir: PathBuf,
        segment_size: u64,
        cold_latency_ms: u64,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SuggestionMode {
    Single,
    Sequence,
}

/// RAII guard: suppresses viz rendering while held, restores on drop.
/// With viz disabled, VizStorageEngine render calls become no-ops (no
/// thread::sleep under the storage mutex), so operations complete in
/// microseconds instead of seconds.
struct VizGuard {
    renderer: Arc<Mutex<VizRenderer>>,
    was_enabled: bool,
}

impl VizGuard {
    fn suppress(renderer: &Arc<Mutex<VizRenderer>>) -> Self {
        let was_enabled = {
            let mut r = renderer.lock().unwrap();
            let e = r.config_mut().enabled;
            r.config_mut().enabled = false;
            e
        };
        Self {
            renderer: renderer.clone(),
            was_enabled,
        }
    }
}

impl Drop for VizGuard {
    fn drop(&mut self) {
        self.renderer.lock().unwrap().config_mut().enabled = self.was_enabled;
    }
}

struct WorkerHandle {
    cancel: CancellationToken,
    kind: WorkerKind,
    interval_ms: u64,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy)]
enum WorkerKind {
    Write,
    Read,
    Mixed,
}

impl fmt::Display for WorkerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerKind::Write => write!(f, "write"),
            WorkerKind::Read => write!(f, "read"),
            WorkerKind::Mixed => write!(f, "mixed"),
        }
    }
}

#[allow(dead_code)]
enum CommandOutcome {
    Put { page_id: PageId },
    GetSuccess { page_id: PageId },
    GetFailure { page_id: PageId },
    Refresh,
    NodeSwitch,
    BgStarted { node: String },
    BgStopped { node: String },
    None,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("demo");

    // Parse optional flags
    let delay_ms = parse_flag_value(&args, "--delay").unwrap_or(300);
    let no_color = args.iter().any(|a| a == "--no-color");
    let trace_json_path = parse_flag_string(&args, "--trace-json");
    let preset = parse_flag_string(&args, "--preset").unwrap_or_else(|| "base".to_string());
    let segment_size = parse_flag_value(&args, "--segment-size").unwrap_or(4096);
    let cold_latency_ms = parse_flag_value(&args, "--cold-latency-ms").unwrap_or(50);

    match cmd {
        "demo" => run_demo().await?,
        "repl" => run_repl().await?,
        "viz-demo" => run_viz_demo(delay_ms, !no_color).await?,
        "viz-repl" => {
            run_viz_repl(
                delay_ms,
                !no_color,
                trace_json_path,
                &preset,
                segment_size,
                cold_latency_ms,
            )
            .await?
        }
        "scenario" => {
            let scenario_arg = args.get(2).cloned().unwrap_or_else(|| {
                eprintln!("Usage: mini-aurora scenario <file.toml> [--preset base|tiered] [--trace-json path]");
                eprintln!("       mini-aurora scenario --list");
                std::process::exit(1);
            });
            if scenario_arg == "--list" || scenario_arg == "list" {
                print_scenario_catalog();
            } else {
                scenario::run_scenario_cli(
                    &scenario_arg,
                    &preset,
                    trace_json_path.as_deref(),
                    segment_size,
                    cold_latency_ms,
                )
                .await?;
            }
        }
        "compare" => {
            let scenario_path = args.get(2).cloned().unwrap_or_else(|| {
                eprintln!("Usage: mini-aurora compare <file.toml> [--segment-size <bytes>] [--cold-latency-ms <ms>]");
                std::process::exit(1);
            });
            scenario::run_scenario_compare(&scenario_path, segment_size, cold_latency_ms).await?;
        }
        "crash-writer" => {
            let wal_path = args.get(2).cloned().unwrap_or_else(|| {
                eprintln!("Usage: mini-aurora crash-writer <wal-path>");
                std::process::exit(1);
            });
            run_crash_writer(&wal_path).await?;
        }
        _ => {
            eprintln!("Usage: mini-aurora [demo|repl|viz-demo|viz-repl|scenario|compare] [--delay <ms>] [--no-color]");
            eprintln!("       [--preset base|tiered] [--trace-json path]");
            eprintln!("       [--segment-size <bytes>] [--cold-latency-ms <ms>]");
            eprintln!("       mini-aurora scenario <file.toml> [flags...]");
            eprintln!("       mini-aurora scenario --list");
            eprintln!("       mini-aurora compare <file.toml> [flags...]");
            std::process::exit(1);
        }
    }

    Ok(())
}

fn parse_flag_value(args: &[String], flag: &str) -> Option<u64> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
}

fn parse_flag_string(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .map(|v| v.clone())
}

fn next_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

fn random_ascii(seed: &mut u64, len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (*seed % ALPHABET.len() as u64) as usize;
        s.push(ALPHABET[idx] as char);
    }
    s
}

fn print_scenario_catalog() {
    println!("=== Scenarios ===");
    println!("  scenarios/burst.toml               Burst writes and repeated reads");
    println!("  scenarios/cold_reads.toml          Cache miss/hit pattern across pages");
    println!("  scenarios/noisy_neighbor.toml      Stale-reader behavior under write churn");
    println!("  scenarios/tiered_demo.toml         Segment rotation with tiered storage");
}

/// Write pages in a tight loop to a WAL file, printing "VDL={n}" after each durable commit.
///
/// Designed to be spawned by the crash-recovery integration test:
///   1. Test spawns this process and reads its stdout.
///   2. When "VDL=N" is seen, N pages are durably committed.
///   3. Test sends SIGTERM; this process dies immediately (no signal handler).
///   4. Test reopens the WAL and verifies recovery.
///
/// If the WAL already exists (e.g. after a crash), recovery runs automatically on open
/// and writing resumes from the next LSN.
async fn run_crash_writer(wal_path: &str) -> anyhow::Result<()> {
    use std::io::Write as IoWrite;

    let path = PathBuf::from(wal_path);
    let storage = Arc::new(StorageEngine::open(&path)?);
    let compute = ComputeEngine::new(storage.clone(), 256);
    compute.refresh_read_point().await?;

    let state = storage.get_durability_state().await?;
    if state.vdl > 0 {
        eprintln!("[crash-writer] Recovered: VDL={}", state.vdl);
    }

    // Start page IDs after whatever was already durably written.
    let mut page_id: PageId = state.vdl + 1;

    loop {
        let data = format!("page-{page_id}").into_bytes();
        let vdl = compute.put(page_id, 0, data).await?;
        println!("VDL={vdl}");
        io::stdout().flush().ok();
        page_id += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run_demo() -> anyhow::Result<()> {
    println!("=== Mini-Aurora Demo ===\n");

    let wal_path = PathBuf::from("/tmp/mini-aurora-demo.wal");
    // Start fresh for demo
    let _ = std::fs::remove_file(&wal_path);

    let storage = Arc::new(StorageEngine::open(&wal_path)?);
    let compute = ComputeEngine::new(storage.clone(), 256);

    // 1. Write some data
    println!("Writing 'Hello, Aurora!' to page 1 at offset 0...");
    compute.put(1, 0, b"Hello, Aurora!".to_vec()).await?;

    println!("Writing 'Log is the DB' to page 2 at offset 0...");
    compute.put(2, 0, b"Log is the DB".to_vec()).await?;

    // 2. Read it back
    let page1 = compute.get(1).await?;
    let page2 = compute.get(2).await?;
    println!("\nPage 1: {:?}", String::from_utf8_lossy(&page1[..14]));
    println!("Page 2: {:?}", String::from_utf8_lossy(&page2[..13]));

    // 3. Multi-page atomic write
    println!("\nAtomic multi-page write (pages 3, 4, 5)...");
    compute
        .put_multi(vec![
            (3, 0, b"Page Three".to_vec()),
            (4, 0, b"Page Four".to_vec()),
            (5, 0, b"Page Five".to_vec()),
        ])
        .await?;

    for pid in 3..=5 {
        let page = compute.get(pid).await?;
        let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
        println!("Page {pid}: {:?}", String::from_utf8_lossy(&page[..end]));
    }

    // 4. Show durability state
    let state = storage.get_durability_state().await?;
    println!("\n{state}");

    // 5. Overwrite and show versioning
    println!("\nOverwriting page 1 with 'Redo wins!'...");
    compute.put(1, 0, b"Redo wins!".to_vec()).await?;

    let page1_new = compute.get(1).await?;
    println!(
        "Page 1 (latest): {:?}",
        String::from_utf8_lossy(&page1_new[..14])
    );

    // Clean up
    let _ = std::fs::remove_file(&wal_path);
    println!("\nDemo complete.");
    Ok(())
}

async fn run_repl() -> anyhow::Result<()> {
    println!("=== Mini-Aurora REPL ===");
    println!(
        "Commands: put <page> <offset> <text>, put-random <count>, get <page>, get-raw <page>, del <page> <offset> <len>"
    );
    println!("          clear-page <page>, compact, state, clear, quit\n");

    let wal_path = PathBuf::from("/tmp/mini-aurora-repl.wal");
    let mut storage = Arc::new(StorageEngine::open(&wal_path)?);
    let mut compute = ComputeEngine::new(storage.clone(), 256);

    // Refresh read point from any prior session
    compute.refresh_read_point().await?;

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    'repl: loop {
        print!("aurora> ");
        stdout.flush()?;

        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }
        let commands: Vec<String> = line
            .trim()
            .split(';')
            .map(str::trim)
            .filter(|cmd| !cmd.is_empty())
            .map(|cmd| cmd.to_string())
            .collect();
        if commands.is_empty() {
            continue;
        }

        for command in commands {
            let parts: Vec<&str> = command.splitn(4, ' ').collect();
            if parts.is_empty() || parts[0].is_empty() {
                continue;
            }

            match parts[0] {
            "put" => {
                if parts.len() < 4 {
                    println!("Usage: put <page_id> <offset> <text>");
                    continue;
                }
                let page_id = match parse_page_id(parts[1]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                let offset = match parse_offset(parts[2]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                let data = parts[3].as_bytes().to_vec();
                match compute.put(page_id, offset, data).await {
                    Ok(vdl) => println!("OK (VDL={vdl})"),
                    Err(e) => println!("Error: {e}"),
                }
            }
            "put-random" => {
                if parts.len() < 2 {
                    println!("Usage: put-random <count>");
                    continue;
                }
                let count: u64 = match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("Invalid count");
                        continue;
                    }
                };
                if count == 0 {
                    println!("Nothing written (count=0)");
                    continue;
                }
                let recovery = match recover(&wal_path) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("Error reading WAL state: {e}");
                        continue;
                    }
                };
                let start_page = recovery.page_index.keys().copied().max().unwrap_or(0) + 1;
                let mut seed = next_seed();
                let mut last_vdl = 0;
                for i in 0..count {
                    let page_id = start_page + i;
                    let payload = random_ascii(&mut seed, 12).into_bytes();
                    match compute.put(page_id, 0, payload).await {
                        Ok(vdl) => last_vdl = vdl,
                        Err(e) => {
                            println!("Error on page {page_id}: {e}");
                            break;
                        }
                    }
                }
                let end_page = start_page + count - 1;
                println!(
                    "OK (inserted {count} random string(s), pages {start_page}..{end_page}, VDL={last_vdl})"
                );
            }
            "get" => {
                if parts.len() < 2 {
                    println!("Usage: get <page_id>");
                    continue;
                }
                let page_id = match parse_page_id(parts[1]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                match compute.get(page_id).await {
                    Ok(page) => print_page_text(&page),
                    Err(e) => println!("Error: {e}"),
                }
            }
            "get-raw" => {
                if parts.len() < 2 {
                    println!("Usage: get-raw <page_id>");
                    continue;
                }
                let page_id = match parse_page_id(parts[1]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                match compute.get(page_id).await {
                    Ok(page) => print_page_raw(&page),
                    Err(e) => println!("Error: {e}"),
                }
            }
            "del" => {
                if parts.len() < 4 {
                    println!("Usage: del <page_id> <offset> <len>");
                    continue;
                }
                let page_id = match parse_page_id(parts[1]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                let offset = match parse_offset(parts[2]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                let len = match parse_len(parts[3]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                if let Err(msg) = ensure_range_in_page(offset, len) {
                    println!("{msg}");
                    continue;
                }
                let zeros = vec![0u8; len];
                match compute.put(page_id, offset, zeros).await {
                    Ok(vdl) => println!("OK (deleted {len} byte(s), VDL={vdl})"),
                    Err(e) => println!("Error: {e}"),
                }
            }
            "clear-page" => {
                if parts.len() < 2 {
                    println!("Usage: clear-page <page_id>");
                    continue;
                }
                let page_id = match parse_page_id(parts[1]) {
                    Ok(v) => v,
                    Err(msg) => {
                        println!("{msg}");
                        continue;
                    }
                };
                let zeros = vec![0u8; PAGE_SIZE];
                match compute.put(page_id, 0, zeros).await {
                    Ok(vdl) => println!("OK (page {page_id} cleared, VDL={vdl})"),
                    Err(e) => println!("Error: {e}"),
                }
            }
            "state" => match storage.get_durability_state().await {
                Ok(s) => println!("{s}"),
                Err(e) => println!("Error: {e}"),
            },
            "clear" => {
                match std::fs::remove_file(&wal_path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        println!("Error clearing WAL file: {e}");
                        continue;
                    }
                }

                storage = Arc::new(StorageEngine::open(&wal_path)?);
                compute = ComputeEngine::new(storage.clone(), 256);
                compute.refresh_read_point().await?;
                println!("OK (database cleared)");
            }
            "compact" => match compact_repl_wal(&wal_path, &mut storage, &mut compute).await {
                Ok((old_bytes, new_bytes, pages_before, pages_after)) => {
                    println!(
                        "OK (compacted: {old_bytes} -> {new_bytes} bytes, pages: {pages_before} -> {pages_after})"
                    );
                }
                Err(e) => println!("Error compacting WAL: {e}"),
            },
            "quit" | "exit" | "q" => break 'repl,
            other => println!("Unknown command: {other}"),
            }
        }
    }

    println!("Bye!");
    Ok(())
}

async fn compact_repl_wal(
    wal_path: &Path,
    storage: &mut Arc<StorageEngine>,
    compute: &mut ComputeEngine,
) -> anyhow::Result<(u64, u64, usize, usize)> {
    let old_bytes = std::fs::metadata(wal_path).map(|m| m.len()).unwrap_or(0);
    let recovery = recover(wal_path)?;
    let page_ids: Vec<PageId> = recovery.page_index.keys().copied().collect();
    let pages_before = page_ids.len();

    compute.refresh_read_point().await?;

    let mut snapshot = Vec::new();
    for page_id in &page_ids {
        let page = compute.get(*page_id).await?;
        if page.iter().all(|&b| b == 0) {
            continue;
        }
        snapshot.push((*page_id, page.to_vec()));
    }
    let pages_after = snapshot.len();

    let _ = std::fs::remove_file(wal_path);
    *storage = Arc::new(StorageEngine::open(wal_path)?);
    *compute = ComputeEngine::new(storage.clone(), 256);
    compute.refresh_read_point().await?;

    for (page_id, bytes) in snapshot {
        compute.put(page_id, 0, bytes).await?;
    }

    let new_bytes = std::fs::metadata(wal_path).map(|m| m.len()).unwrap_or(0);
    Ok((old_bytes, new_bytes, pages_before, pages_after))
}

fn open_viz_storage(
    mode: &VizStorageMode,
    renderer: Arc<Mutex<VizRenderer>>,
    reset_on_open: bool,
) -> anyhow::Result<Arc<VizStorageEngine>> {
    match mode {
        VizStorageMode::Base { wal_path } => {
            if reset_on_open {
                let _ = std::fs::remove_file(wal_path);
            }
            Ok(Arc::new(VizStorageEngine::open(wal_path, renderer)?))
        }
        VizStorageMode::Tiered {
            base_dir,
            segment_size,
            cold_latency_ms,
        } => {
            if reset_on_open {
                let _ = std::fs::remove_dir_all(base_dir);
            }
            Ok(Arc::new(VizStorageEngine::open_tiered(
                base_dir,
                *segment_size,
                Duration::from_millis(*cold_latency_ms),
                renderer,
            )?))
        }
    }
}

fn storage_bytes(mode: &VizStorageMode) -> u64 {
    match mode {
        VizStorageMode::Base { wal_path } => std::fs::metadata(wal_path).map(|m| m.len()).unwrap_or(0),
        VizStorageMode::Tiered { base_dir, .. } => directory_size_bytes(base_dir),
    }
}

fn directory_size_bytes(path: &Path) -> u64 {
    let mut total = 0u64;
    let entries = match std::fs::read_dir(path) {
        Ok(e) => e,
        Err(_) => return 0,
    };
    for entry in entries.flatten() {
        let entry_path = entry.path();
        match entry.metadata() {
            Ok(meta) if meta.is_file() => total += meta.len(),
            Ok(meta) if meta.is_dir() => total += directory_size_bytes(&entry_path),
            _ => {}
        }
    }
    total
}

async fn compact_viz_repl_storage(
    state: &mut ReplState,
) -> anyhow::Result<(u64, u64, usize, usize)> {
    if !state.workers.is_empty() {
        anyhow::bail!("stop background workers before compacting (use `bg stop A` / `bg stop B`)");
    }

    let _guard = VizGuard::suppress(&state.renderer);
    let old_bytes = storage_bytes(&state.storage_mode);
    let page_ids = state.storage.durable_page_ids();
    let pages_before = page_ids.len();

    let current = state
        .nodes
        .get(&state.current_node)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("current node {} is unavailable", state.current_node))?;
    current.refresh_read_point().await?;

    let mut snapshot = Vec::new();
    for page_id in &page_ids {
        let page = current.get(*page_id).await?;
        if page.iter().all(|&b| b == 0) {
            continue;
        }
        snapshot.push((*page_id, page.to_vec()));
    }
    let pages_after = snapshot.len();

    let storage = open_viz_storage(&state.storage_mode, state.renderer.clone(), true)?;
    let node_a = Arc::new(VizComputeEngine::new(
        storage.clone(),
        256,
        state.renderer.clone(),
        "A".to_string(),
    ));
    let node_b = Arc::new(VizComputeEngine::new(
        storage.clone(),
        256,
        state.renderer.clone(),
        "B".to_string(),
    ));
    node_a.refresh_read_point().await?;
    node_b.refresh_read_point().await?;

    for (page_id, bytes) in snapshot {
        node_a.put(page_id, 0, bytes).await?;
    }

    let mut nodes = HashMap::new();
    nodes.insert("A".to_string(), node_a);
    nodes.insert("B".to_string(), node_b);
    state.storage = storage;
    state.nodes = nodes;
    if !state.nodes.contains_key(&state.current_node) {
        state.current_node = "A".to_string();
    }

    let new_bytes = storage_bytes(&state.storage_mode);
    Ok((old_bytes, new_bytes, pages_before, pages_after))
}

async fn clear_viz_repl_storage(state: &mut ReplState) -> anyhow::Result<()> {
    if !state.workers.is_empty() {
        anyhow::bail!("stop background workers before clearing (use `bg stop A` / `bg stop B`)");
    }

    let _guard = VizGuard::suppress(&state.renderer);
    let storage = open_viz_storage(&state.storage_mode, state.renderer.clone(), true)?;
    let node_a = Arc::new(VizComputeEngine::new(
        storage.clone(),
        256,
        state.renderer.clone(),
        "A".to_string(),
    ));
    let node_b = Arc::new(VizComputeEngine::new(
        storage.clone(),
        256,
        state.renderer.clone(),
        "B".to_string(),
    ));
    node_a.refresh_read_point().await?;
    node_b.refresh_read_point().await?;

    let mut nodes = HashMap::new();
    nodes.insert("A".to_string(), node_a);
    nodes.insert("B".to_string(), node_b);
    state.storage = storage;
    state.nodes = nodes;
    if !state.nodes.contains_key(&state.current_node) {
        state.current_node = "A".to_string();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Viz modes
// ---------------------------------------------------------------------------

async fn run_viz_demo(delay_ms: u64, color: bool) -> anyhow::Result<()> {
    let config = VizConfig {
        step_delay: Duration::from_millis(delay_ms),
        color,
        enabled: true,
    };
    let renderer = Arc::new(Mutex::new(VizRenderer::new(config)));

    let wal_path = PathBuf::from("/tmp/mini-aurora-viz-demo.wal");
    let _ = std::fs::remove_file(&wal_path);

    let storage = Arc::new(VizStorageEngine::open(&wal_path, renderer.clone())?);
    let node_a = VizComputeEngine::new(storage.clone(), 256, renderer.clone(), "A".to_string());
    let node_b = VizComputeEngine::new(storage.clone(), 256, renderer.clone(), "B".to_string());

    // Phase 1: Single Writer — Node A writes pg1, Node B idle
    node_a.put(1, 0, b"Hello".to_vec()).await?;

    // Phase 2: Read Isolation — Node B at rp=0 can't see pg1
    match node_b.get(1).await {
        Err(_) => { /* expected: page not found at rp=0 */ }
        Ok(_) => {}
    }
    node_b.refresh_read_point().await?;
    let page = node_b.get(1).await?;
    println!(
        "  Node B reads pg1: {:?}",
        String::from_utf8_lossy(&page[..5])
    );

    // Phase 3: Stale Reads & Buffer Independence
    node_a.put(1, 0, b"World".to_vec()).await?;
    let page = node_b.get(1).await?; // buffer pool HIT, still "Hello"
    println!(
        "  Node B reads pg1 (stale): {:?}",
        String::from_utf8_lossy(&page[..5])
    );
    node_b.refresh_read_point().await?;
    let page = node_b.get(1).await?; // fresh, sees "World"
    println!(
        "  Node B reads pg1 (fresh): {:?}",
        String::from_utf8_lossy(&page[..5])
    );

    // Phase 4: Atomic Multi-Page
    node_a
        .put_multi(vec![
            (2, 0, b"Page Two".to_vec()),
            (3, 0, b"Page Three".to_vec()),
        ])
        .await?;
    node_b.refresh_read_point().await?;
    for pid in 2..=3 {
        let page = node_b.get(pid).await?;
        let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
        println!(
            "  Node B reads pg{pid}: {:?}",
            String::from_utf8_lossy(&page[..end])
        );
    }

    // Clean up
    let _ = std::fs::remove_file(&wal_path);
    println!("\nViz demo complete.");
    Ok(())
}

async fn run_viz_repl(
    delay_ms: u64,
    color: bool,
    trace_json: Option<String>,
    preset: &str,
    segment_size: u64,
    cold_latency_ms: u64,
) -> anyhow::Result<()> {
    println!("=== Mini-Aurora Viz REPL (preset: {preset}) ===");
    println!("Commands: put <page> <offset> <text>, put-random <count>, get <page>, get-raw <page>");
    println!("          del <page> <offset> <len>, clear-page <page>, compact, clear, refresh");
    println!("          node A|B, state, metrics, bg <node> write|read|mixed <ms>");
    println!("          bg stop <node>, bg list, viz on|off, delay <ms>, suggest single|sequence");
    println!("          1/2/3 (run suggestion), quit\n");
    println!("Tip: type `suggest sequence` to switch to sequence mode.\n");

    let config = VizConfig {
        step_delay: Duration::from_millis(delay_ms),
        color,
        enabled: true,
    };
    let mut renderer_inner = VizRenderer::new(config);
    if let Some(ref path) = trace_json {
        let tracer = JsonTracer::open(std::path::Path::new(path))?;
        renderer_inner.set_tracer(tracer);
        println!("Tracing events to: {path}");
    }
    let renderer = Arc::new(Mutex::new(renderer_inner));

    let storage_mode = match preset {
        "tiered" => {
            println!(
                "Tiered storage: segment_size={segment_size}B, cold_latency={cold_latency_ms}ms"
            );
            VizStorageMode::Tiered {
                base_dir: PathBuf::from("/tmp/mini-aurora-viz-tiered"),
                segment_size,
                cold_latency_ms,
            }
        }
        _ => VizStorageMode::Base {
            wal_path: PathBuf::from("/tmp/mini-aurora-viz-repl.wal"),
        },
    };
    let reset_on_open = matches!(&storage_mode, VizStorageMode::Tiered { .. });
    let storage = open_viz_storage(&storage_mode, renderer.clone(), reset_on_open)?;

    let node_a = Arc::new(VizComputeEngine::new(
        storage.clone(),
        256,
        renderer.clone(),
        "A".to_string(),
    ));
    let node_b = Arc::new(VizComputeEngine::new(
        storage.clone(),
        256,
        renderer.clone(),
        "B".to_string(),
    ));

    node_a.refresh_read_point().await?;
    node_b.refresh_read_point().await?;

    let mut nodes = HashMap::new();
    nodes.insert("A".to_string(), node_a);
    nodes.insert("B".to_string(), node_b);

    let (bg_output_tx, mut bg_output_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let mut state = ReplState {
        nodes,
        current_node: "A".to_string(),
        suggestions: Vec::new(),
        workers: HashMap::new(),
        storage,
        renderer,
        bg_page_counter: Arc::new(AtomicU64::new(100)),
        bg_output_tx,
        storage_mode,
        suggestion_mode: SuggestionMode::Single,
    };

    // --- Async stdin: OS thread + mpsc channel ---
    let (line_tx, mut line_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let prompt_str = Arc::new(std::sync::Mutex::new("A> ".to_string()));
    let prompt_for_thread = prompt_str.clone();

    std::thread::spawn(move || {
        let stdin = io::stdin();
        loop {
            {
                let p = prompt_for_thread.lock().unwrap();
                print!("{}", *p);
                let _ = io::stdout().flush();
            }
            let mut line = String::new();
            match stdin.read_line(&mut line) {
                Ok(0) | Err(_) => break, // EOF or error
                Ok(_) => {
                    if line_tx.send(line).is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Print initial suggestions (single mode by default)
    update_suggestions(&mut state, &CommandOutcome::None, "").await;
    print_suggestions(&state);

    // --- Main loop: select on stdin + bg output ---
    loop {
        // Drain any bg output that queued up (e.g. during a slow viz command)
        while let Ok(msg) = bg_output_rx.try_recv() {
            println!("{msg}");
        }

        tokio::select! {
            biased; // prefer user input over bg output

            line = line_rx.recv() => {
                let line = match line {
                    Some(l) => l,
                    None => break,
                };

                let trimmed = line.trim().to_string();
                if trimmed.is_empty() {
                    continue;
                }

                // Check for suggestion shortcut (1, 2, 3)
                let (cmd, from_suggestion) = if let Ok(n) = trimmed.parse::<usize>() {
                    if n >= 1 && n <= state.suggestions.len() {
                        let resolved = state.suggestions[n - 1].clone();
                        (resolved, true)
                    } else {
                        (trimmed, false)
                    }
                } else {
                    (trimmed, false)
                };
                if from_suggestion {
                    println!(">>> suggested: {cmd}");
                } else {
                    println!(">>> input: {cmd}");
                }

                let commands: Vec<String> = cmd
                    .split(';')
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                    .map(|c| c.to_string())
                    .collect();
                if commands.is_empty() {
                    continue;
                }

                let mut outcome = CommandOutcome::None;
                let mut should_quit = false;
                let mut last_executed = String::new();
                for command in commands {
                    let parts: Vec<&str> = command.splitn(5, ' ').collect();
                    if parts.is_empty() || parts[0].is_empty() {
                        continue;
                    }
                    last_executed = command.clone();

                    outcome = match parts[0] {
                    "put" => {
                        if parts.len() < 4 {
                            println!("Usage: put <page_id> <offset> <text>");
                            CommandOutcome::None
                        } else {
                            let page_id = match parse_page_id(parts[1]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            let offset = match parse_offset(parts[2]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            let data = parts[3].as_bytes().to_vec();
                            if let Some(w) = state.workers.get(&state.current_node) {
                                println!("(warning: node {} has active bg {} worker)", state.current_node, w.kind);
                            }
                            let compute = state.nodes[&state.current_node].clone();
                            match compute.put(page_id, offset, data).await {
                                Ok(vdl) => {
                                    println!("OK (VDL={vdl})");
                                    CommandOutcome::Put { page_id }
                                }
                                Err(e) => {
                                    println!("Error: {e}");
                                    CommandOutcome::None
                                }
                            }
                        }
                    }
                    "put-random" => {
                        if parts.len() < 2 {
                            println!("Usage: put-random <count>");
                            CommandOutcome::None
                        } else {
                            let count: u64 = match parts[1].parse() {
                                Ok(v) => v,
                                Err(_) => {
                                    println!("Invalid count");
                                    continue;
                                }
                            };
                            if count == 0 {
                                println!("Nothing written (count=0)");
                                CommandOutcome::None
                            } else {
                                if let Some(w) = state.workers.get(&state.current_node) {
                                    println!("(warning: node {} has active bg {} worker)", state.current_node, w.kind);
                                }
                                let start_page = state
                                    .storage
                                    .durable_page_ids()
                                    .into_iter()
                                    .max()
                                    .unwrap_or(0)
                                    + 1;
                                let compute = state.nodes[&state.current_node].clone();
                                let mut seed = next_seed();
                                let mut last_vdl = 0;
                                let mut failed = false;
                                for i in 0..count {
                                    let page_id = start_page + i;
                                    let payload = random_ascii(&mut seed, 12).into_bytes();
                                    match compute.put(page_id, 0, payload).await {
                                        Ok(vdl) => last_vdl = vdl,
                                        Err(e) => {
                                            println!("Error on page {page_id}: {e}");
                                            failed = true;
                                            break;
                                        }
                                    }
                                }
                                if failed {
                                    CommandOutcome::None
                                } else {
                                let end_page = start_page + count - 1;
                                println!(
                                    "OK (inserted {count} random string(s), pages {start_page}..{end_page}, VDL={last_vdl})"
                                );
                                CommandOutcome::Put { page_id: end_page }
                                }
                            }
                        }
                    }
                    "get" => {
                        if parts.len() < 2 {
                            println!("Usage: get <page_id>");
                            CommandOutcome::None
                        } else {
                            let page_id = match parse_page_id(parts[1]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            if let Some(w) = state.workers.get(&state.current_node) {
                                println!("(warning: node {} has active bg {} worker)", state.current_node, w.kind);
                            }
                            let compute = state.nodes[&state.current_node].clone();
                            match compute.get(page_id).await {
                                Ok(page) => {
                                    print_page_text(&page);
                                    CommandOutcome::GetSuccess { page_id }
                                }
                                Err(e) => {
                                    println!("Error: {e}");
                                    CommandOutcome::GetFailure { page_id }
                                }
                            }
                        }
                    }
                    "get-raw" => {
                        if parts.len() < 2 {
                            println!("Usage: get-raw <page_id>");
                            CommandOutcome::None
                        } else {
                            let page_id = match parse_page_id(parts[1]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            if let Some(w) = state.workers.get(&state.current_node) {
                                println!("(warning: node {} has active bg {} worker)", state.current_node, w.kind);
                            }
                            let compute = state.nodes[&state.current_node].clone();
                            match compute.get(page_id).await {
                                Ok(page) => print_page_raw(&page),
                                Err(e) => println!("Error: {e}"),
                            }
                            CommandOutcome::None
                        }
                    }
                    "del" => {
                        if parts.len() < 4 {
                            println!("Usage: del <page_id> <offset> <len>");
                            CommandOutcome::None
                        } else {
                            let page_id = match parse_page_id(parts[1]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            let offset = match parse_offset(parts[2]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            let len = match parse_len(parts[3]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            if let Err(msg) = ensure_range_in_page(offset, len) {
                                println!("{msg}");
                                CommandOutcome::None
                            } else {
                                if let Some(w) = state.workers.get(&state.current_node) {
                                    println!("(warning: node {} has active bg {} worker)", state.current_node, w.kind);
                                }
                                let compute = state.nodes[&state.current_node].clone();
                                let zeros = vec![0u8; len];
                                match compute.put(page_id, offset, zeros).await {
                                    Ok(vdl) => {
                                        println!("OK (deleted {len} byte(s), VDL={vdl})");
                                        CommandOutcome::Put { page_id }
                                    }
                                    Err(e) => {
                                        println!("Error: {e}");
                                        CommandOutcome::None
                                    }
                                }
                            }
                        }
                    }
                    "clear-page" => {
                        if parts.len() < 2 {
                            println!("Usage: clear-page <page_id>");
                            CommandOutcome::None
                        } else {
                            let page_id = match parse_page_id(parts[1]) {
                                Ok(v) => v,
                                Err(msg) => { println!("{msg}"); continue; }
                            };
                            if let Some(w) = state.workers.get(&state.current_node) {
                                println!("(warning: node {} has active bg {} worker)", state.current_node, w.kind);
                            }
                            let compute = state.nodes[&state.current_node].clone();
                            let zeros = vec![0u8; PAGE_SIZE];
                            match compute.put(page_id, 0, zeros).await {
                                Ok(vdl) => {
                                    println!("OK (page {page_id} cleared, VDL={vdl})");
                                    CommandOutcome::Put { page_id }
                                }
                                Err(e) => {
                                    println!("Error: {e}");
                                    CommandOutcome::None
                                }
                            }
                        }
                    }
                    "compact" => {
                        match compact_viz_repl_storage(&mut state).await {
                            Ok((old_bytes, new_bytes, pages_before, pages_after)) => {
                                println!(
                                    "OK (compacted: {old_bytes} -> {new_bytes} bytes, pages: {pages_before} -> {pages_after})"
                                );
                            }
                            Err(e) => println!("Error compacting storage: {e}"),
                        }
                        CommandOutcome::None
                    }
                    "clear" => {
                        match clear_viz_repl_storage(&mut state).await {
                            Ok(()) => println!("OK (database cleared)"),
                            Err(e) => println!("Error clearing storage: {e}"),
                        }
                        CommandOutcome::None
                    }
                    "refresh" => {
                        let compute = state.nodes[&state.current_node].clone();
                        match compute.refresh_read_point().await {
                            Ok(rp) => println!("read_point -> {rp}"),
                            Err(e) => println!("Error: {e}"),
                        }
                        CommandOutcome::Refresh
                    }
                    "node" => {
                        if parts.len() < 2 {
                            println!("Usage: node A|B");
                            CommandOutcome::None
                        } else {
                            let target = parts[1].to_uppercase();
                            if state.nodes.contains_key(&target) {
                                state.current_node = target;
                                *prompt_str.lock().unwrap() = format!("{}> ", state.current_node);
                                println!("Switched to Node {}", state.current_node);
                                CommandOutcome::NodeSwitch
                            } else {
                                println!("Unknown node: {}. Available: A, B", parts[1]);
                                CommandOutcome::None
                            }
                        }
                    }
                    "state" => {
                        match state.storage.get_durability_state().await {
                            Ok(s) => {
                                println!("{s}");
                                let compute = state.nodes[&state.current_node].clone();
                                let rp = compute.read_point().await;
                                state.storage.emit_state_snapshot(
                                    state.current_node.clone(), rp, 0, Vec::new(),
                                );
                            }
                            Err(e) => println!("Error: {e}"),
                        }
                        CommandOutcome::None
                    }
                    "metrics" => {
                        let r = state.renderer.lock().unwrap();
                        match r.metrics_summary() {
                            Some(summary) => println!("{summary}"),
                            None => println!("Metrics not available."),
                        }
                        CommandOutcome::None
                    }
                    "bg" => {
                        handle_bg_command(&parts, &mut state).await
                    }
                    "viz" => {
                        if parts.len() < 2 {
                            println!("Usage: viz on|off");
                        } else {
                            match parts[1] {
                                "on" => {
                                    state.renderer.lock().unwrap().config_mut().enabled = true;
                                    println!("Visualization enabled.");
                                }
                                "off" => {
                                    state.renderer.lock().unwrap().config_mut().enabled = false;
                                    println!("Visualization disabled.");
                                }
                                _ => println!("Usage: viz on|off"),
                            }
                        }
                        CommandOutcome::None
                    }
                    "delay" => {
                        if parts.len() < 2 {
                            println!("Usage: delay <ms>");
                        } else {
                            match parts[1].parse::<u64>() {
                                Ok(ms) => {
                                    state.renderer.lock().unwrap().config_mut().step_delay =
                                        Duration::from_millis(ms);
                                    println!("Step delay set to {ms}ms.");
                                }
                                Err(_) => println!("Invalid delay value"),
                            }
                        }
                        CommandOutcome::None
                    }
                    "suggest" => {
                        if parts.len() < 2 {
                            match state.suggestion_mode {
                                SuggestionMode::Single => {
                                    println!("Suggestion mode: single (switch with `suggest sequence`)");
                                }
                                SuggestionMode::Sequence => {
                                    println!("Suggestion mode: sequence (switch with `suggest single`)");
                                }
                            }
                        } else {
                            match parts[1] {
                                "single" => {
                                    state.suggestion_mode = SuggestionMode::Single;
                                    println!("Suggestion mode set to single-command.");
                                }
                                "sequence" => {
                                    state.suggestion_mode = SuggestionMode::Sequence;
                                    println!("Suggestion mode set to command-sequences.");
                                }
                                _ => println!("Usage: suggest single|sequence"),
                            }
                        }
                        CommandOutcome::None
                    }
                    "quit" | "exit" | "q" => {
                        for (label, handle) in state.workers.drain() {
                            handle.cancel.cancel();
                            let _ = handle.task.await;
                            println!("Stopped bg worker on Node {label}");
                        }
                        should_quit = true;
                        CommandOutcome::None
                    }
                    other => {
                        println!("Unknown command: {other}");
                        CommandOutcome::None
                    }
                };

                // Drain bg output that accumulated during the (possibly slow) viz command
                while let Ok(msg) = bg_output_rx.try_recv() {
                    println!("{msg}");
                }
                if should_quit {
                    break;
                }
                }

                if should_quit {
                    break;
                }
                update_suggestions(&mut state, &outcome, &last_executed).await;
                print_suggestions(&state);
            }

            // Stream bg output while idle (user hasn't pressed Enter yet)
            msg = bg_output_rx.recv() => {
                if let Some(msg) = msg {
                    println!("{msg}");
                }
            }
        }
    }

    println!("Bye!");
    Ok(())
}

fn other_node(current: &str) -> &'static str {
    if current == "A" {
        "B"
    } else {
        "A"
    }
}

async fn update_suggestions(state: &mut ReplState, outcome: &CommandOutcome, last_command: &str) {
    let mut page_ids = state.storage.durable_page_ids();
    page_ids.sort_unstable();
    let sample_page = page_ids.first().copied().unwrap_or(1);
    let other = other_node(&state.current_node);
    let workers_running = !state.workers.is_empty();
    let has_data = !page_ids.is_empty();

    let raw = match state.suggestion_mode {
        SuggestionMode::Single => {
            single_command_suggestions(outcome, sample_page, other, workers_running, has_data)
        }
        SuggestionMode::Sequence => {
            sequence_suggestions(outcome, sample_page, other, workers_running, has_data)
        }
    };
    let fallback = if state.suggestion_mode == SuggestionMode::Sequence {
        vec![
            format!("put {sample_page} 0 Hello; get {sample_page}"),
            "state; metrics".to_string(),
            format!("node {other}; refresh; get {sample_page}"),
        ]
    } else {
        vec![
            format!("put {sample_page} 0 Hello"),
            format!("get {sample_page}"),
            "state".to_string(),
        ]
    };
    state.suggestions = normalize_suggestions(raw, fallback, last_command);
}

fn single_command_suggestions(
    outcome: &CommandOutcome,
    sample_page: PageId,
    other: &str,
    workers_running: bool,
    has_data: bool,
) -> Vec<String> {
    let mut suggestions = match outcome {
        CommandOutcome::Put { page_id } => vec![
            format!("get {page_id}"),
            format!("del {page_id} 0 1"),
            "compact".to_string(),
        ],
        CommandOutcome::GetSuccess { page_id } => vec![
            format!("del {page_id} 0 1"),
            format!("put {page_id} 0 updated"),
            "compact".to_string(),
        ],
        CommandOutcome::GetFailure { page_id } => vec![
            "refresh".to_string(),
            format!("put {page_id} 0 Hello"),
            format!("node {other}"),
        ],
        CommandOutcome::Refresh => vec![
            format!("get {sample_page}"),
            format!("del {sample_page} 0 1"),
            "compact".to_string(),
        ],
        CommandOutcome::NodeSwitch => vec![
            "refresh".to_string(),
            format!("get {sample_page}"),
            "state".to_string(),
        ],
        CommandOutcome::BgStarted { node } => vec![
            "bg list".to_string(),
            format!("bg stop {node}"),
            "state".to_string(),
        ],
        CommandOutcome::BgStopped { .. } => vec![
            "bg list".to_string(),
            format!("get {sample_page}"),
            "compact".to_string(),
        ],
        CommandOutcome::None => {
            if workers_running {
                vec![
                    "bg list".to_string(),
                    format!("bg stop {other}"),
                    "state".to_string(),
                ]
            } else {
                vec![
                    format!("put {sample_page} 0 Hello"),
                    "put-random 30".to_string(),
                    "state".to_string(),
                ]
            }
        }
    };
    if has_data {
        suggestions.push("clear".to_string());
    }
    if !workers_running {
        suggestions.push("put-random 30".to_string());
    }
    suggestions
}

fn sequence_suggestions(
    outcome: &CommandOutcome,
    sample_page: PageId,
    other: &str,
    workers_running: bool,
    has_data: bool,
) -> Vec<String> {
    let mut suggestions = if workers_running {
        vec![
            "bg list; state; metrics".to_string(),
            "bg stop A; bg stop B; compact".to_string(),
            format!("node {other}; refresh; get {sample_page}"),
        ]
    } else {
        match outcome {
            CommandOutcome::GetFailure { page_id } => vec![
                format!("refresh; get {page_id}; state"),
                format!("put {page_id} 0 Hello; get {page_id}"),
                format!("node {other}; refresh; get {page_id}"),
            ],
            CommandOutcome::Put { page_id } | CommandOutcome::GetSuccess { page_id } => vec![
                format!("get {page_id}; del {page_id} 0 1; get-raw {page_id}"),
                format!("node {other}; refresh; get {page_id}"),
                "compact; state; metrics".to_string(),
            ],
            _ => vec![
                format!("put {sample_page} 0 Hello; get {sample_page}"),
                format!("node {other}; refresh; get {sample_page}"),
                "compact; state; metrics".to_string(),
            ],
        }
    };
    if has_data {
        suggestions.push("clear; state; metrics".to_string());
    }
    if !workers_running {
        suggestions.push("put-random 30; state; metrics".to_string());
        suggestions.push(format!(
            "put {sample_page} 0 hello; del {sample_page} 0 1; compact"
        ));
    }
    suggestions
}

fn normalize_suggestions(raw: Vec<String>, fallback: Vec<String>, last_command: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut sources = raw;
    sources.extend(fallback);
    for s in sources {
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() || trimmed == last_command {
            continue;
        }
        if seen.insert(trimmed.clone()) {
            out.push(trimmed);
        }
        if out.len() == 3 {
            break;
        }
    }
    while out.len() < 3 {
        out.push("state".to_string());
    }
    out
}

fn print_suggestions(state: &ReplState) {
    if state.suggestions.is_empty() {
        return;
    }
    match state.suggestion_mode {
        SuggestionMode::Single => {
            println!("Suggestions (single mode, switch with `suggest sequence`):");
        }
        SuggestionMode::Sequence => {
            println!("Suggestions (sequence mode, switch with `suggest single`):");
        }
    }
    let descs = |cmd: &str| -> String {
        if cmd.contains(';') {
            let steps: Vec<String> = cmd
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .take(3)
                .map(command_desc)
                .collect();
            return steps.join(" -> ");
        }
        if cmd.starts_with("get ") {
            return "read the page".to_string();
        }
        if cmd.starts_with("get-raw ") {
            return "read full page bytes".to_string();
        }
        if cmd.starts_with("del ") {
            return "zero out a byte range".to_string();
        }
        if cmd.starts_with("put-random ") {
            return "insert many random strings".to_string();
        }
        if cmd == "compact" {
            return "rewrite WAL to live data".to_string();
        }
        if cmd.starts_with("put ") && cmd.contains("updated") {
            return "overwrite with new data".to_string();
        }
        if cmd.starts_with("put ") && cmd.contains("new-data") {
            return "overwrite with new data".to_string();
        }
        if cmd.starts_with("put ") && cmd.contains("Hello") {
            return "write data to the page".to_string();
        }
        if cmd.starts_with("put ") {
            return "write to a page".to_string();
        }
        if cmd == "refresh" {
            return "advance read_point to VDL".to_string();
        }
        if cmd.starts_with("node ") {
            return "switch compute node".to_string();
        }
        if cmd == "state" {
            return "show durability watermarks".to_string();
        }
        if cmd == "metrics" {
            return "show runtime metrics".to_string();
        }
        if cmd.starts_with("suggest ") {
            return "switch suggestion mode".to_string();
        }
        if cmd.starts_with("bg stop") {
            return "stop the background worker".to_string();
        }
        if cmd == "bg list" {
            return "show running workers".to_string();
        }
        if cmd.starts_with("bg ") {
            return "start background worker".to_string();
        }
        "".to_string()
    };
    for (i, cmd) in state.suggestions.iter().enumerate() {
        let desc = descs(cmd);
        let pad = 24usize.saturating_sub(cmd.len());
        if desc.is_empty() {
            println!("  [{}] {}", i + 1, cmd);
        } else {
            println!("  [{}] {}{}\u{2014} {}", i + 1, cmd, " ".repeat(pad), desc);
        }
    }
}

fn command_desc(cmd: &str) -> String {
    if cmd.starts_with("put-random ") {
        return "insert random data".to_string();
    }
    if cmd.starts_with("get-raw ") {
        return "inspect page bytes".to_string();
    }
    if cmd.starts_with("get ") {
        return "read page".to_string();
    }
    if cmd.starts_with("put ") {
        return "write page".to_string();
    }
    if cmd.starts_with("del ") {
        return "delete range".to_string();
    }
    if cmd.starts_with("clear-page ") {
        return "clear full page".to_string();
    }
    if cmd == "compact" {
        return "compact WAL".to_string();
    }
    if cmd == "clear" {
        return "clear database".to_string();
    }
    if cmd == "refresh" {
        return "sync read point".to_string();
    }
    if cmd.starts_with("node ") {
        return "switch node".to_string();
    }
    if cmd == "state" {
        return "show state".to_string();
    }
    if cmd == "metrics" {
        return "show metrics".to_string();
    }
    if cmd.starts_with("bg ") {
        return "manage background worker".to_string();
    }
    cmd.to_string()
}

async fn handle_bg_command(parts: &[&str], state: &mut ReplState) -> CommandOutcome {
    if parts.len() < 2 {
        println!("Usage: bg <node> write|read|mixed <ms>");
        println!("       bg stop <node>");
        println!("       bg list");
        return CommandOutcome::None;
    }

    match parts[1] {
        "list" => {
            if state.workers.is_empty() {
                println!("No background workers running.");
            } else {
                for (label, w) in &state.workers {
                    println!("  Node {label}: {} every {}ms", w.kind, w.interval_ms);
                }
            }
            CommandOutcome::None
        }
        "stop" => {
            if parts.len() < 3 {
                println!("Usage: bg stop <node>");
                return CommandOutcome::None;
            }
            let target = parts[2].to_uppercase();
            if let Some(handle) = state.workers.remove(&target) {
                handle.cancel.cancel();
                let _ = handle.task.await;
                println!("Stopped bg worker on Node {target}");
                CommandOutcome::BgStopped { node: target }
            } else {
                println!("No worker running on Node {target}");
                CommandOutcome::None
            }
        }
        _ => {
            // bg <node> write|read|mixed <ms>
            if parts.len() < 4 {
                println!("Usage: bg <node> write|read|mixed <ms>");
                return CommandOutcome::None;
            }
            let target = parts[1].to_uppercase();
            if !state.nodes.contains_key(&target) {
                println!("Unknown node: {target}. Available: A, B");
                return CommandOutcome::None;
            }
            let kind = match parts[2] {
                "write" => WorkerKind::Write,
                "read" => WorkerKind::Read,
                "mixed" => WorkerKind::Mixed,
                other => {
                    println!("Unknown worker kind: {other}. Use write|read|mixed");
                    return CommandOutcome::None;
                }
            };
            let interval_ms: u64 = match parts[3].parse() {
                Ok(v) if v >= 50 => v,
                Ok(_) => {
                    println!("Minimum interval is 50ms");
                    return CommandOutcome::None;
                }
                Err(_) => {
                    println!("Invalid interval");
                    return CommandOutcome::None;
                }
            };

            // Stop existing worker on this node if any
            if let Some(handle) = state.workers.remove(&target) {
                handle.cancel.cancel();
                let _ = handle.task.await;
                println!("Stopped previous worker on Node {target}");
            }

            let cancel = CancellationToken::new();
            let bg_counter = state.bg_page_counter.clone();
            let cancel_clone = cancel.clone();
            let node_label = target.clone();
            let renderer_for_bg = state.renderer.clone();
            let bg_tx = state.bg_output_tx.clone();

            // Non-viz ComputeEngine: shares storage but never touches the
            // renderer at the compute level (no set_active, no render_op_header,
            // no event emissions). Storage-level renders are suppressed via
            // VizGuard so the storage mutex is held for microseconds, not seconds.
            let storage_for_bg: Arc<dyn StorageApi> = state.storage.clone();
            let bg_compute = ComputeEngine::new(storage_for_bg, 256);
            {
                let _guard = VizGuard::suppress(&state.renderer);
                let _ = bg_compute.refresh_read_point().await;
            }

            let task = tokio::spawn(async move {
                let mut cycle: u64 = 0;
                loop {
                    if cancel_clone.is_cancelled() {
                        break;
                    }

                    // Scope the VizGuard so viz is restored before the sleep
                    {
                        let _guard = VizGuard::suppress(&renderer_for_bg);
                        match kind {
                            WorkerKind::Write => {
                                let pg = bg_counter.fetch_add(1, Ordering::Relaxed);
                                match bg_compute.put(pg, 0, format!("bg-{pg}").into_bytes()).await {
                                    Ok(vdl) => {
                                        let _ = bg_tx.send(format!(
                                            "[bg {node_label}] PUT pg{pg} OK (VDL={vdl})"
                                        ));
                                    }
                                    Err(e) => {
                                        let _ = bg_tx.send(format!(
                                            "[bg {node_label}] PUT pg{pg} Error: {e}"
                                        ));
                                    }
                                }
                            }
                            WorkerKind::Read => {
                                let pg = (cycle % 10) + 1;
                                match bg_compute.get(pg).await {
                                    Ok(page) => {
                                        let end =
                                            page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
                                        let preview = if end == 0 {
                                            "(empty)".to_string()
                                        } else {
                                            let s = String::from_utf8_lossy(&page[..end.min(20)]);
                                            format!("{:?}", s)
                                        };
                                        let _ = bg_tx.send(format!(
                                            "[bg {node_label}] GET pg{pg} -> {preview}"
                                        ));
                                    }
                                    Err(e) => {
                                        let _ = bg_tx.send(format!(
                                            "[bg {node_label}] GET pg{pg} Error: {e}"
                                        ));
                                    }
                                }
                            }
                            WorkerKind::Mixed => {
                                if cycle % 2 == 0 {
                                    match bg_compute.refresh_read_point().await {
                                        Ok(rp) => {
                                            let _ = bg_tx.send(format!(
                                                "[bg {node_label}] REFRESH -> rp={rp}"
                                            ));
                                        }
                                        Err(e) => {
                                            let _ = bg_tx.send(format!(
                                                "[bg {node_label}] REFRESH Error: {e}"
                                            ));
                                        }
                                    }
                                } else {
                                    let pg = ((cycle / 2) % 10) + 1;
                                    match bg_compute.get(pg).await {
                                        Ok(page) => {
                                            let end = page
                                                .iter()
                                                .position(|&b| b == 0)
                                                .unwrap_or(PAGE_SIZE);
                                            let preview = if end == 0 {
                                                "(empty)".to_string()
                                            } else {
                                                let s =
                                                    String::from_utf8_lossy(&page[..end.min(20)]);
                                                format!("{:?}", s)
                                            };
                                            let _ = bg_tx.send(format!(
                                                "[bg {node_label}] GET pg{pg} -> {preview}"
                                            ));
                                        }
                                        Err(e) => {
                                            let _ = bg_tx.send(format!(
                                                "[bg {node_label}] GET pg{pg} Error: {e}"
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    } // _guard dropped: viz restored before sleep

                    cycle += 1;
                    tokio::select! {
                        _ = cancel_clone.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
                    }
                }
            });

            state.workers.insert(
                target.clone(),
                WorkerHandle {
                    cancel,
                    kind,
                    interval_ms,
                    task,
                },
            );
            println!("Started bg {kind} worker on Node {target} every {interval_ms}ms");
            CommandOutcome::BgStarted { node: target }
        }
    }
}
