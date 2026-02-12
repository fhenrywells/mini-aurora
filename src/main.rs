use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mini_aurora_common::{PageId, StorageApi, PAGE_SIZE};
use mini_aurora_compute::engine::ComputeEngine;
use mini_aurora_storage::engine::StorageEngine;

mod viz;

use viz::compute::VizComputeEngine;
use viz::engine::VizStorageEngine;
use viz::events::VizConfig;
use viz::renderer::VizRenderer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("demo");

    // Parse optional flags
    let delay_ms = parse_flag_value(&args, "--delay").unwrap_or(300);
    let no_color = args.iter().any(|a| a == "--no-color");

    match cmd {
        "demo" => run_demo().await?,
        "repl" => run_repl().await?,
        "viz-demo" => run_viz_demo(delay_ms, !no_color).await?,
        "viz-repl" => run_viz_repl(delay_ms, !no_color).await?,
        _ => {
            eprintln!("Usage: mini-aurora [demo|repl|viz-demo|viz-repl] [--delay <ms>] [--no-color]");
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
    println!("=== Mini-Aurora Viz Demo ===\n");

    let config = VizConfig {
        step_delay: Duration::from_millis(delay_ms),
        color,
        enabled: true,
    };
    let renderer = Arc::new(Mutex::new(VizRenderer::new(config)));

    let wal_path = PathBuf::from("/tmp/mini-aurora-viz-demo.wal");
    let _ = std::fs::remove_file(&wal_path);

    let storage = Arc::new(VizStorageEngine::open(&wal_path, renderer.clone())?);
    let compute = VizComputeEngine::new(storage.clone(), 256, renderer.clone());

    // Show initial state
    storage.emit_state_snapshot(0, 1, Vec::new());

    // 1. Write some data
    compute.put(1, 0, b"Hello, Aurora!".to_vec()).await?;
    compute.put(2, 0, b"Log is the DB".to_vec()).await?;

    // 2. Read it back
    let page1 = compute.get(1).await?;
    let page2 = compute.get(2).await?;
    println!("  Result — Page 1: {:?}", String::from_utf8_lossy(&page1[..14]));
    println!("  Result — Page 2: {:?}", String::from_utf8_lossy(&page2[..13]));

    // 3. Multi-page atomic write
    compute
        .put_multi(vec![
            (3, 0, b"Page Three".to_vec()),
            (4, 0, b"Page Four".to_vec()),
            (5, 0, b"Page Five".to_vec()),
        ])
        .await?;

    // 4. Read multi-page results
    for pid in 3..=5 {
        let page = compute.get(pid).await?;
        let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
        println!(
            "  Result — Page {pid}: {:?}",
            String::from_utf8_lossy(&page[..end])
        );
    }

    // 5. Overwrite to show versioning
    compute.put(1, 0, b"Redo wins!".to_vec()).await?;

    let page1_new = compute.get(1).await?;
    println!(
        "  Result — Page 1 (latest): {:?}",
        String::from_utf8_lossy(&page1_new[..10])
    );

    // Clean up
    let _ = std::fs::remove_file(&wal_path);
    println!("\nViz demo complete.");
    Ok(())
}

async fn run_viz_repl(delay_ms: u64, color: bool) -> anyhow::Result<()> {
    println!("=== Mini-Aurora Viz REPL ===");
    println!("Commands: put <page> <offset> <text>, get <page>, state");
    println!("          viz on|off, delay <ms>, quit\n");

    let config = VizConfig {
        step_delay: Duration::from_millis(delay_ms),
        color,
        enabled: true,
    };
    let renderer = Arc::new(Mutex::new(VizRenderer::new(config)));

    let wal_path = PathBuf::from("/tmp/mini-aurora-viz-repl.wal");
    let storage = Arc::new(VizStorageEngine::open(&wal_path, renderer.clone())?);
    let compute = VizComputeEngine::new(storage.clone(), 256, renderer.clone());

    compute.refresh_read_point().await?;

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("aurora-viz> ");
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
                    Ok(s) => {
                        println!("{s}");
                        let rp = compute.read_point().await;
                        storage.emit_state_snapshot(rp, 0, Vec::new());
                    }
                    Err(e) => println!("Error: {e}"),
                }
            }
            "viz" => {
                if parts.len() < 2 {
                    println!("Usage: viz on|off");
                    continue;
                }
                match parts[1] {
                    "on" => {
                        renderer.lock().unwrap().config_mut().enabled = true;
                        println!("Visualization enabled.");
                    }
                    "off" => {
                        renderer.lock().unwrap().config_mut().enabled = false;
                        println!("Visualization disabled.");
                    }
                    _ => println!("Usage: viz on|off"),
                }
            }
            "delay" => {
                if parts.len() < 2 {
                    println!("Usage: delay <ms>");
                    continue;
                }
                match parts[1].parse::<u64>() {
                    Ok(ms) => {
                        renderer.lock().unwrap().config_mut().step_delay =
                            Duration::from_millis(ms);
                        println!("Step delay set to {ms}ms.");
                    }
                    Err(_) => println!("Invalid delay value"),
                }
            }
            "quit" | "exit" | "q" => break,
            other => println!("Unknown command: {other}"),
        }
    }

    println!("Bye!");
    Ok(())
}
