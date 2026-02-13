use std::collections::HashMap;
use std::fmt;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mini_aurora_common::{PageId, StorageApi, PAGE_SIZE};
use mini_aurora_compute::engine::ComputeEngine;
use mini_aurora_storage::engine::StorageEngine;
use tokio_util::sync::CancellationToken;

mod viz;

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
        Self { renderer: renderer.clone(), was_enabled }
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
        "viz-repl" => run_viz_repl(delay_ms, !no_color, trace_json_path, &preset, segment_size, cold_latency_ms).await?,
        "scenario" => {
            let scenario_path = args.get(2).cloned().unwrap_or_else(|| {
                eprintln!("Usage: mini-aurora scenario <file.toml> [--preset base|tiered] [--trace-json path]");
                std::process::exit(1);
            });
            scenario::run_scenario_cli(&scenario_path, &preset, trace_json_path.as_deref(), segment_size, cold_latency_ms).await?;
        }
        _ => {
            eprintln!("Usage: mini-aurora [demo|repl|viz-demo|viz-repl|scenario] [--delay <ms>] [--no-color]");
            eprintln!("       [--preset base|tiered] [--trace-json path]");
            eprintln!("       [--segment-size <bytes>] [--cold-latency-ms <ms>]");
            eprintln!("       mini-aurora scenario <file.toml> [flags...]");
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

async fn run_demo() -> anyhow::Result<()> {
    println!("=== Mini-Aurora Demo ===\n");

    let wal_path = PathBuf::from("/tmp/mini-aurora-demo.wal");
    // Start fresh for demo
    let _ = std::fs::remove_file(&wal_path);

    let storage = Arc::new(StorageEngine::open(&wal_path)?);
    let compute = ComputeEngine::new(storage.clone(), 256);

    // 1. Write some data
    println!("Writing 'Hello, Aurora!' to page 1 at offset 0...");
    compute
        .put(1, 0, b"Hello, Aurora!".to_vec())
        .await?;

    println!("Writing 'Log is the DB' to page 2 at offset 0...");
    compute
        .put(2, 0, b"Log is the DB".to_vec())
        .await?;

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
        println!(
            "Page {pid}: {:?}",
            String::from_utf8_lossy(&page[..end])
        );
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
    println!("Commands: put <page> <offset> <text>, get <page>, state, quit\n");

    let wal_path = PathBuf::from("/tmp/mini-aurora-repl.wal");
    let storage = Arc::new(StorageEngine::open(&wal_path)?);
    let compute = ComputeEngine::new(storage.clone(), 256);

    // Refresh read point from any prior session
    compute.refresh_read_point().await?;

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("aurora> ");
        stdout.flush()?;

        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }
        let parts: Vec<&str> = line.trim().splitn(4, ' ').collect();
        if parts.is_empty() || parts[0].is_empty() {
            continue;
        }

        match parts[0] {
            "put" => {
                if parts.len() < 4 {
                    println!("Usage: put <page_id> <offset> <text>");
                    continue;
                }
                let page_id: PageId = match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => { println!("Invalid page_id"); continue; }
                };
                let offset: u16 = match parts[2].parse() {
                    Ok(v) => v,
                    Err(_) => { println!("Invalid offset"); continue; }
                };
                let data = parts[3].as_bytes().to_vec();
                match compute.put(page_id, offset, data).await {
                    Ok(vdl) => println!("OK (VDL={vdl})"),
                    Err(e) => println!("Error: {e}"),
                }
            }
            "get" => {
                if parts.len() < 2 {
                    println!("Usage: get <page_id>");
                    continue;
                }
                let page_id: PageId = match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => { println!("Invalid page_id"); continue; }
                };
                match compute.get(page_id).await {
                    Ok(page) => {
                        let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
                        if end == 0 {
                            println!("(empty page)");
                        } else {
                            println!("{:?}", String::from_utf8_lossy(&page[..end]));
                        }
                    }
                    Err(e) => println!("Error: {e}"),
                }
            }
            "state" => {
                match storage.get_durability_state().await {
                    Ok(s) => println!("{s}"),
                    Err(e) => println!("Error: {e}"),
                }
            }
            "quit" | "exit" | "q" => break,
            other => println!("Unknown command: {other}"),
        }
    }

    println!("Bye!");
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

async fn run_viz_repl(delay_ms: u64, color: bool, trace_json: Option<String>, preset: &str, segment_size: u64, cold_latency_ms: u64) -> anyhow::Result<()> {
    println!("=== Mini-Aurora Viz REPL (preset: {preset}) ===");
    println!("Commands: put <page> <offset> <text>, get <page>, refresh");
    println!("          node A|B, state, metrics, bg <node> write|read|mixed <ms>");
    println!("          bg stop <node>, bg list, viz on|off, delay <ms>");
    println!("          1/2/3 (run suggestion), quit\n");

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

    let storage: Arc<VizStorageEngine> = match preset {
        "tiered" => {
            let base_dir = PathBuf::from("/tmp/mini-aurora-viz-tiered");
            let _ = std::fs::remove_dir_all(&base_dir);
            let cold_latency = Duration::from_millis(cold_latency_ms);
            println!("Tiered storage: segment_size={segment_size}B, cold_latency={cold_latency_ms}ms");
            Arc::new(VizStorageEngine::open_tiered(&base_dir, segment_size, cold_latency, renderer.clone())?)
        }
        _ => {
            let wal_path = PathBuf::from("/tmp/mini-aurora-viz-repl.wal");
            Arc::new(VizStorageEngine::open(&wal_path, renderer.clone())?)
        }
    };

    let node_a = Arc::new(VizComputeEngine::new(
        storage.clone(), 256, renderer.clone(), "A".to_string(),
    ));
    let node_b = Arc::new(VizComputeEngine::new(
        storage.clone(), 256, renderer.clone(), "B".to_string(),
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

    // Print initial suggestions
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
                let cmd = if let Ok(n) = trimmed.parse::<usize>() {
                    if n >= 1 && n <= state.suggestions.len() {
                        let resolved = state.suggestions[n - 1].clone();
                        println!(">>> {resolved}");
                        resolved
                    } else {
                        trimmed
                    }
                } else {
                    trimmed
                };

                let parts: Vec<&str> = cmd.splitn(5, ' ').collect();
                if parts.is_empty() || parts[0].is_empty() {
                    continue;
                }

                let outcome = match parts[0] {
                    "put" => {
                        if parts.len() < 4 {
                            println!("Usage: put <page_id> <offset> <text>");
                            CommandOutcome::None
                        } else {
                            let page_id: PageId = match parts[1].parse() {
                                Ok(v) => v,
                                Err(_) => { println!("Invalid page_id"); continue; }
                            };
                            let offset: u16 = match parts[2].parse() {
                                Ok(v) => v,
                                Err(_) => { println!("Invalid offset"); continue; }
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
                    "get" => {
                        if parts.len() < 2 {
                            println!("Usage: get <page_id>");
                            CommandOutcome::None
                        } else {
                            let page_id: PageId = match parts[1].parse() {
                                Ok(v) => v,
                                Err(_) => { println!("Invalid page_id"); continue; }
                            };
                            if let Some(w) = state.workers.get(&state.current_node) {
                                println!("(warning: node {} has active bg {} worker)", state.current_node, w.kind);
                            }
                            let compute = state.nodes[&state.current_node].clone();
                            match compute.get(page_id).await {
                                Ok(page) => {
                                    let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
                                    if end == 0 {
                                        println!("(empty page)");
                                    } else {
                                        println!("{:?}", String::from_utf8_lossy(&page[..end]));
                                    }
                                    CommandOutcome::GetSuccess { page_id }
                                }
                                Err(e) => {
                                    println!("Error: {e}");
                                    CommandOutcome::GetFailure { page_id }
                                }
                            }
                        }
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
                    "quit" | "exit" | "q" => {
                        for (label, handle) in state.workers.drain() {
                            handle.cancel.cancel();
                            let _ = handle.task.await;
                            println!("Stopped bg worker on Node {label}");
                        }
                        break;
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

                update_suggestions(&mut state, &outcome);
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
    if current == "A" { "B" } else { "A" }
}

fn update_suggestions(state: &mut ReplState, outcome: &CommandOutcome) {
    state.suggestions.clear();
    match outcome {
        CommandOutcome::Put { page_id } => {
            state.suggestions.push(format!("get {page_id}"));
            state.suggestions.push(format!("put {page_id} 0 updated"));
            state.suggestions.push(format!("node {}", other_node(&state.current_node)));
        }
        CommandOutcome::GetSuccess { page_id } => {
            state.suggestions.push(format!("put {page_id} 0 new-data"));
            state.suggestions.push(format!("get {}", page_id + 1));
            state.suggestions.push("refresh".to_string());
        }
        CommandOutcome::GetFailure { page_id } => {
            state.suggestions.push("refresh".to_string());
            state.suggestions.push(format!("put {page_id} 0 Hello"));
            state.suggestions.push(format!("node {}", other_node(&state.current_node)));
        }
        CommandOutcome::Refresh => {
            state.suggestions.push("get 1".to_string());
            state.suggestions.push("state".to_string());
            state.suggestions.push(format!("node {}", other_node(&state.current_node)));
        }
        CommandOutcome::NodeSwitch => {
            state.suggestions.push("refresh".to_string());
            state.suggestions.push("get 1".to_string());
            state.suggestions.push("state".to_string());
        }
        CommandOutcome::BgStarted { node } => {
            let other = if node == "A" { "B" } else { "A" };
            state.suggestions.push(format!("bg {other} mixed 500"));
            state.suggestions.push("bg list".to_string());
            state.suggestions.push(format!("bg stop {node}"));
        }
        CommandOutcome::BgStopped { node: _ } => {
            state.suggestions.push("bg list".to_string());
            state.suggestions.push("get 1".to_string());
            state.suggestions.push("state".to_string());
        }
        CommandOutcome::None => {
            // Keep previous suggestions or show defaults
            if state.suggestions.is_empty() {
                state.suggestions.push("put 1 0 Hello".to_string());
            }
        }
    }
}

fn print_suggestions(state: &ReplState) {
    if state.suggestions.is_empty() {
        return;
    }
    let descs = |cmd: &str| -> &str {
        if cmd.starts_with("get ") { return "read the page"; }
        if cmd.starts_with("put ") && cmd.contains("updated") { return "overwrite with new data"; }
        if cmd.starts_with("put ") && cmd.contains("new-data") { return "overwrite with new data"; }
        if cmd.starts_with("put ") && cmd.contains("Hello") { return "write data to the page"; }
        if cmd.starts_with("put ") { return "write to a page"; }
        if cmd == "refresh" { return "advance read_point to VDL"; }
        if cmd.starts_with("node ") { return "switch compute node"; }
        if cmd == "state" { return "show durability watermarks"; }
        if cmd.starts_with("bg stop") { return "stop the background worker"; }
        if cmd == "bg list" { return "show running workers"; }
        if cmd.starts_with("bg ") { return "start background worker"; }
        ""
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
                                    Ok(vdl) => { let _ = bg_tx.send(format!("[bg {node_label}] PUT pg{pg} OK (VDL={vdl})")); }
                                    Err(e) => { let _ = bg_tx.send(format!("[bg {node_label}] PUT pg{pg} Error: {e}")); }
                                }
                            }
                            WorkerKind::Read => {
                                let pg = (cycle % 10) + 1;
                                match bg_compute.get(pg).await {
                                    Ok(page) => {
                                        let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
                                        let preview = if end == 0 {
                                            "(empty)".to_string()
                                        } else {
                                            let s = String::from_utf8_lossy(&page[..end.min(20)]);
                                            format!("{:?}", s)
                                        };
                                        let _ = bg_tx.send(format!("[bg {node_label}] GET pg{pg} -> {preview}"));
                                    }
                                    Err(e) => { let _ = bg_tx.send(format!("[bg {node_label}] GET pg{pg} Error: {e}")); }
                                }
                            }
                            WorkerKind::Mixed => {
                                if cycle % 2 == 0 {
                                    match bg_compute.refresh_read_point().await {
                                        Ok(rp) => { let _ = bg_tx.send(format!("[bg {node_label}] REFRESH -> rp={rp}")); }
                                        Err(e) => { let _ = bg_tx.send(format!("[bg {node_label}] REFRESH Error: {e}")); }
                                    }
                                } else {
                                    let pg = ((cycle / 2) % 10) + 1;
                                    match bg_compute.get(pg).await {
                                        Ok(page) => {
                                            let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
                                            let preview = if end == 0 {
                                                "(empty)".to_string()
                                            } else {
                                                let s = String::from_utf8_lossy(&page[..end.min(20)]);
                                                format!("{:?}", s)
                                            };
                                            let _ = bg_tx.send(format!("[bg {node_label}] GET pg{pg} -> {preview}"));
                                        }
                                        Err(e) => { let _ = bg_tx.send(format!("[bg {node_label}] GET pg{pg} Error: {e}")); }
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

            state.workers.insert(target.clone(), WorkerHandle {
                cancel,
                kind,
                interval_ms,
                task,
            });
            println!("Started bg {kind} worker on Node {target} every {interval_ms}ms");
            CommandOutcome::BgStarted { node: target }
        }
    }
}
