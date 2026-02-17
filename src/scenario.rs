use std::collections::HashMap;
use std::io::{self, Write as IoWrite};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;

use crate::viz::compute::VizComputeEngine;
use crate::viz::engine::VizStorageEngine;
use crate::viz::events::VizConfig;
use crate::viz::metrics::MetricsSummary;
use crate::viz::renderer::VizRenderer;
use crate::viz::tracer::JsonTracer;

// ---------------------------------------------------------------------------
// PrintFn: output sink used by execute_step / execute_steps
// ---------------------------------------------------------------------------

/// Arc-wrapped callable for emitting output lines.
/// Using Arc (not &dyn) lets us move it into async blocks and clone cheaply.
type PrintFn = Arc<dyn Fn(String) + Send + Sync>;

fn println_fn() -> PrintFn {
    Arc::new(|s: String| println!("{s}"))
}

fn silent_fn() -> PrintFn {
    Arc::new(|_: String| {})
}

// ---------------------------------------------------------------------------
// Scenario file types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct Scenario {
    pub meta: ScenarioMeta,
    pub steps: Vec<ScenarioStep>,
}

#[derive(Deserialize)]
pub struct ScenarioMeta {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "op")]
pub enum ScenarioStep {
    #[serde(rename = "put")]
    Put {
        page_id: u64,
        offset: u16,
        data: String,
        node: Option<String>,
    },
    #[serde(rename = "get")]
    Get {
        page_id: u64,
        node: Option<String>,
    },
    #[serde(rename = "refresh")]
    Refresh {
        node: Option<String>,
    },
    #[serde(rename = "sleep_ms")]
    SleepMs {
        value: u64,
    },
    #[serde(rename = "repeat")]
    Repeat {
        count: u64,
        steps: Vec<ScenarioStep>,
    },
}

// ---------------------------------------------------------------------------
// Result types for compare mode
// ---------------------------------------------------------------------------

/// Result of executing one top-level scenario step.
pub struct StepResult {
    pub op_label: String,
    pub result: String,
}

/// Collected output of a full scenario run.
pub struct ScenarioRunResult {
    pub step_results: Vec<StepResult>,
    pub metrics: MetricsSummary,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Run a scenario from the CLI (prints as it goes).
pub async fn run_scenario_cli(
    scenario_path: &str,
    preset: &str,
    trace_json: Option<&str>,
    segment_size: u64,
    cold_latency_ms: u64,
) -> anyhow::Result<()> {
    let toml_content = std::fs::read_to_string(scenario_path)?;
    let scenario: Scenario = toml::from_str(&toml_content)?;

    println!("=== Scenario: {} (preset: {preset}) ===", scenario.meta.name);
    if let Some(ref desc) = scenario.meta.description {
        println!("{desc}");
    }

    let config = VizConfig {
        step_delay: Duration::ZERO,
        color: false,
        enabled: false,
    };
    let mut renderer_inner = VizRenderer::new(config);
    if let Some(path) = trace_json {
        let tracer = JsonTracer::open(std::path::Path::new(path))?;
        renderer_inner.set_tracer(tracer);
        println!("Tracing events to: {path}");
    }
    if preset == "tiered" {
        println!(
            "Tiered storage: segment_size={segment_size}B, cold_latency={cold_latency_ms}ms"
        );
    }
    let renderer = Arc::new(Mutex::new(renderer_inner));

    let storage = build_storage(preset, segment_size, cold_latency_ms, renderer.clone(), "scenario")?;
    let (node_a, node_b) = make_nodes(storage.clone(), renderer.clone()).await?;
    let nodes = node_map(node_a, node_b);

    execute_steps(&scenario.steps, &nodes, "A", println_fn()).await?;

    let r = renderer.lock().unwrap();
    if let Some(summary) = r.metrics_summary() {
        println!("\n=== Metrics ===");
        println!("{summary}");
    }
    println!("\nScenario complete.");
    Ok(())
}

/// Run the same scenario against BASE and TIERED backends, print a side-by-side table.
pub async fn run_scenario_compare(
    scenario_path: &str,
    segment_size: u64,
    cold_latency_ms: u64,
) -> anyhow::Result<()> {
    let toml_content = std::fs::read_to_string(scenario_path)?;
    let scenario: Scenario = toml::from_str(&toml_content)?;

    println!("=== Compare: {} ===\n", scenario.meta.name);
    if let Some(ref desc) = scenario.meta.description {
        println!("{desc}\n");
    }

    print!("Running BASE    ... ");
    let _ = io::stdout().flush();
    let base = run_scenario_inner(scenario_path, "base", segment_size, cold_latency_ms).await?;
    println!(
        "done  ({} appends, {:.1}s, all data on fast local storage)",
        base.metrics.write_count, base.metrics.uptime_secs
    );

    print!("Running TIERED  ... ");
    let _ = io::stdout().flush();
    let tiered =
        run_scenario_inner(scenario_path, "tiered", segment_size, cold_latency_ms).await?;
    println!(
        "done  ({} appends, {:.1}s total, {}ms cold overhead, {} segs sealed → cold storage)",
        tiered.metrics.write_count,
        tiered.metrics.uptime_secs,
        tiered.metrics.cold_latency_total_ms,
        tiered.metrics.segments_cooled,
    );

    println!();
    print_comparison_table(&base, &tiered);
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Run a scenario headlessly and capture step results + metrics.
async fn run_scenario_inner(
    scenario_path: &str,
    preset: &str,
    segment_size: u64,
    cold_latency_ms: u64,
) -> anyhow::Result<ScenarioRunResult> {
    let toml_content = std::fs::read_to_string(scenario_path)?;
    let scenario: Scenario = toml::from_str(&toml_content)?;

    let config = VizConfig {
        step_delay: Duration::ZERO,
        color: false,
        enabled: false,
    };
    let renderer = Arc::new(Mutex::new(VizRenderer::new(config)));
    let storage = build_storage(preset, segment_size, cold_latency_ms, renderer.clone(), "compare")?;
    let (node_a, node_b) = make_nodes(storage.clone(), renderer.clone()).await?;
    let nodes = node_map(node_a, node_b);

    let step_results = execute_steps(&scenario.steps, &nodes, "A", silent_fn()).await?;

    let metrics = renderer
        .lock()
        .unwrap()
        .metrics_summary()
        .ok_or_else(|| anyhow::anyhow!("metrics collector not initialised"))?;

    Ok(ScenarioRunResult { step_results, metrics })
}

/// Build a `VizStorageEngine` for the given preset, cleaning up any prior state.
fn build_storage(
    preset: &str,
    segment_size: u64,
    cold_latency_ms: u64,
    renderer: Arc<Mutex<VizRenderer>>,
    tmp_tag: &str,
) -> anyhow::Result<Arc<VizStorageEngine>> {
    Ok(match preset {
        "tiered" => {
            let base_dir = PathBuf::from(format!("/tmp/mini-aurora-{tmp_tag}-tiered"));
            let _ = std::fs::remove_dir_all(&base_dir);
            let cold_latency = Duration::from_millis(cold_latency_ms);
            Arc::new(VizStorageEngine::open_tiered(
                &base_dir,
                segment_size,
                cold_latency,
                renderer,
            )?)
        }
        _ => {
            let wal_path = PathBuf::from(format!("/tmp/mini-aurora-{tmp_tag}.wal"));
            let _ = std::fs::remove_file(&wal_path);
            Arc::new(VizStorageEngine::open(&wal_path, renderer)?)
        }
    })
}

/// Create node A and node B, refresh both read points.
async fn make_nodes(
    storage: Arc<VizStorageEngine>,
    renderer: Arc<Mutex<VizRenderer>>,
) -> anyhow::Result<(Arc<VizComputeEngine>, Arc<VizComputeEngine>)> {
    let node_a = Arc::new(VizComputeEngine::new(
        storage.clone(),
        256,
        renderer.clone(),
        "A".to_string(),
    ));
    let node_b = Arc::new(VizComputeEngine::new(
        storage,
        256,
        renderer,
        "B".to_string(),
    ));
    node_a.refresh_read_point().await?;
    node_b.refresh_read_point().await?;
    Ok((node_a, node_b))
}

fn node_map(
    node_a: Arc<VizComputeEngine>,
    node_b: Arc<VizComputeEngine>,
) -> HashMap<String, Arc<VizComputeEngine>> {
    [("A".to_string(), node_a), ("B".to_string(), node_b)].into()
}

// ---------------------------------------------------------------------------
// Step execution
// ---------------------------------------------------------------------------

fn execute_steps<'a>(
    steps: &'a [ScenarioStep],
    nodes: &'a HashMap<String, Arc<VizComputeEngine>>,
    default_node: &'a str,
    print_fn: PrintFn,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<StepResult>>> + 'a>> {
    Box::pin(async move {
        let mut results = Vec::new();
        for step in steps {
            let r = execute_step(step, nodes, default_node, &print_fn).await?;
            results.push(r);
        }
        Ok(results)
    })
}

async fn execute_step(
    step: &ScenarioStep,
    nodes: &HashMap<String, Arc<VizComputeEngine>>,
    default_node: &str,
    print_fn: &PrintFn,
) -> anyhow::Result<StepResult> {
    match step {
        ScenarioStep::Put { page_id, offset, data, node } => {
            let node_key = node.as_deref().unwrap_or(default_node);
            let compute = nodes
                .get(node_key)
                .ok_or_else(|| anyhow::anyhow!("Unknown node: {node_key}"))?;
            let result = match compute.put(*page_id, *offset, data.as_bytes().to_vec()).await {
                Ok(vdl) => {
                    let r = format!("VDL={vdl}");
                    print_fn(format!(
                        "  [{node_key}] PUT pg{page_id} @{offset} {data:?} -> {r}"
                    ));
                    r
                }
                Err(e) => {
                    let r = format!("Error: {e}");
                    print_fn(format!(
                        "  [{node_key}] PUT pg{page_id} @{offset} {data:?} -> {r}"
                    ));
                    r
                }
            };
            Ok(StepResult {
                op_label: format!("[{node_key}] PUT pg{page_id} @{offset}"),
                result,
            })
        }

        ScenarioStep::Get { page_id, node } => {
            let node_key = node.as_deref().unwrap_or(default_node);
            let compute = nodes
                .get(node_key)
                .ok_or_else(|| anyhow::anyhow!("Unknown node: {node_key}"))?;
            let result = match compute.get(*page_id).await {
                Ok(page) => {
                    let end = page
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(mini_aurora_common::PAGE_SIZE);
                    let preview = if end == 0 {
                        "(empty)".to_string()
                    } else {
                        format!("{:?}", String::from_utf8_lossy(&page[..end.min(40)]))
                    };
                    print_fn(format!("  [{node_key}] GET pg{page_id} -> {preview}"));
                    preview
                }
                Err(e) => {
                    let r = format!("Error: {e}");
                    print_fn(format!("  [{node_key}] GET pg{page_id} -> {r}"));
                    r
                }
            };
            Ok(StepResult {
                op_label: format!("[{node_key}] GET pg{page_id}"),
                result,
            })
        }

        ScenarioStep::Refresh { node } => {
            let node_key = node.as_deref().unwrap_or(default_node);
            let compute = nodes
                .get(node_key)
                .ok_or_else(|| anyhow::anyhow!("Unknown node: {node_key}"))?;
            let result = match compute.refresh_read_point().await {
                Ok(rp) => {
                    let r = format!("rp={rp}");
                    print_fn(format!("  [{node_key}] REFRESH -> {r}"));
                    r
                }
                Err(e) => {
                    let r = format!("Error: {e}");
                    print_fn(format!("  [{node_key}] REFRESH -> {r}"));
                    r
                }
            };
            Ok(StepResult {
                op_label: format!("[{node_key}] REFRESH"),
                result,
            })
        }

        ScenarioStep::SleepMs { value } => {
            print_fn(format!("  sleep {value}ms"));
            tokio::time::sleep(Duration::from_millis(*value)).await;
            Ok(StepResult {
                op_label: format!("sleep {value}ms"),
                result: "(done)".to_string(),
            })
        }

        ScenarioStep::Repeat { count, steps } => {
            let inner_label = inner_steps_label(steps, 3);
            let op_label = format!("repeat {count}\u{d7} ({inner_label})");
            print_fn(format!("  repeat {count}x:"));
            for i in 0..*count {
                if *count <= 10 || i % (*count / 5).max(1) == 0 {
                    print_fn(format!("    iteration {}/{count}", i + 1));
                }
                // Inner results are discarded; we return one StepResult for the whole repeat.
                execute_steps(steps, nodes, default_node, print_fn.clone()).await?;
            }
            Ok(StepResult {
                op_label,
                result: "(done)".to_string(),
            })
        }
    }
}

/// Build a short summary of inner steps for use in a Repeat label.
fn inner_steps_label(steps: &[ScenarioStep], max: usize) -> String {
    let labels: Vec<String> = steps
        .iter()
        .take(max)
        .map(|s| match s {
            ScenarioStep::Put { page_id, .. } => format!("put pg{page_id}"),
            ScenarioStep::Get { page_id, .. } => format!("get pg{page_id}"),
            ScenarioStep::Refresh { .. } => "refresh".to_string(),
            ScenarioStep::SleepMs { value } => format!("sleep {value}ms"),
            ScenarioStep::Repeat { count, .. } => format!("repeat {count}\u{d7}"),
        })
        .collect();
    let suffix = if steps.len() > max { ", \u{2026}" } else { "" };
    format!("{}{suffix}", labels.join(", "))
}

// ---------------------------------------------------------------------------
// Comparison table rendering
// ---------------------------------------------------------------------------

// Row layout (84 chars total):
//   ║ {label:36} │ {base:18} │ {tiered:18} {marker:1} ║
// Border segment widths (═ chars between corners):
//   38 (label col + surrounding spaces)
//   20 (base col + surrounding spaces)
//   22 (tiered col + marker + surrounding spaces)

fn print_comparison_table(base: &ScenarioRunResult, tiered: &ScenarioRunResult) {
    let border = |l: char, m: char, r: char| -> String {
        format!(
            "{l}{}{m}{}{m}{}{r}",
            "═".repeat(38),
            "═".repeat(20),
            "═".repeat(22)
        )
    };
    let top = border('╔', '╤', '╗');
    let sep = border('╠', '╪', '╣');
    let bot = border('╚', '╧', '╝');

    // Data row: left-pads each field; appends ← where values differ.
    let row = |label: &str, base_val: &str, tiered_val: &str| -> String {
        let marker = if base_val != tiered_val { "←" } else { " " };
        format!(
            "║ {:<36} │ {:<18} │ {:<18} {:<1} ║",
            trunc(label, 36),
            trunc(base_val, 18),
            trunc(tiered_val, 18),
            marker
        )
    };

    // Section-header row: no marker column (tiered col gets the spare space).
    let header = |label: &str| -> String {
        format!(
            "║ {:<36} │ {:<18} │ {:<20} ║",
            trunc(label, 36),
            "BASE",
            "TIERED"
        )
    };

    println!("{top}");
    println!("{}", header("Step"));
    println!("{sep}");

    // Step rows (one per top-level ScenarioStep; both runs produce the same count)
    let n = base.step_results.len().max(tiered.step_results.len());
    for i in 0..n {
        let (label, base_val, tiered_val) = match (
            base.step_results.get(i),
            tiered.step_results.get(i),
        ) {
            (Some(b), Some(t)) => (b.op_label.as_str(), b.result.as_str(), t.result.as_str()),
            (Some(b), None) => (b.op_label.as_str(), b.result.as_str(), "—"),
            (None, Some(t)) => (t.op_label.as_str(), "—", t.result.as_str()),
            (None, None) => break,
        };
        println!("{}", row(label, base_val, tiered_val));
    }

    // Metrics section
    println!("{sep}");
    println!("{}", header("Metrics"));
    println!("{sep}");

    let bm = &base.metrics;
    let tm = &tiered.metrics;

    let fmt_bytes = |b: u64| -> String {
        if b < 1024 {
            format!("{b} B")
        } else {
            format!("{:.1} KB", b as f64 / 1024.0)
        }
    };
    let cache_pct = |hits: u64, misses: u64| -> String {
        let total = hits + misses;
        if total == 0 {
            "—".to_string()
        } else {
            format!("{}%", hits * 100 / total)
        }
    };
    // Show "—" when the base value is zero for tiered-specific counters
    let opt_zero = |n: u64| -> String {
        if n == 0 { "—".to_string() } else { n.to_string() }
    };

    // WAL write metrics
    println!("{}", row("WAL appends", &bm.write_count.to_string(), &tm.write_count.to_string()));
    println!(
        "{}",
        row("WAL bytes written", &fmt_bytes(bm.wal_bytes_written), &fmt_bytes(tm.wal_bytes_written))
    );

    // Tiered-specific: segments sealed and moved to cheap storage
    // "—" in BASE makes it immediately obvious these are tiered-only features.
    println!(
        "{}",
        row(
            "Segments sealed (immutable)",
            &opt_zero(bm.segment_rotations),
            &tm.segment_rotations.to_string()
        )
    );
    println!(
        "{}",
        row(
            "Segments on cold storage",
            &opt_zero(bm.segments_cooled),
            &tm.segments_cooled.to_string()
        )
    );

    // Read metrics: split by tier to show hot-path is equally fast
    let fmt_ms = |ms: u64| -> String {
        if ms == 0 { "—".to_string() } else { format!("{ms} ms") }
    };
    println!(
        "{}",
        row(
            "Cold storage reads (WAL recs)",
            &opt_zero(bm.cold_tier_reads),
            &tm.cold_tier_reads.to_string()
        )
    );
    println!(
        "{}",
        row(
            "Cold read overhead",
            &fmt_ms(bm.cold_latency_total_ms),
            &fmt_ms(tm.cold_latency_total_ms)
        )
    );

    // Cache and materialization metrics
    println!(
        "{}",
        row("Page cache hits", &bm.page_cache_hits.to_string(), &tm.page_cache_hits.to_string())
    );
    println!(
        "{}",
        row(
            "Page cache misses",
            &bm.page_cache_misses.to_string(),
            &tm.page_cache_misses.to_string()
        )
    );
    println!(
        "{}",
        row(
            "Cache hit rate",
            &cache_pct(bm.page_cache_hits, bm.page_cache_misses),
            &cache_pct(tm.page_cache_hits, tm.page_cache_misses)
        )
    );
    println!(
        "{}",
        row("Materializations", &bm.materialize_count.to_string(), &tm.materialize_count.to_string())
    );
    println!(
        "{}",
        row("VCL / VDL", &format!("{} / {}", bm.vcl, bm.vdl), &format!("{} / {}", tm.vcl, tm.vdl))
    );

    println!("{bot}");
    println!();
    println!("  Tiered trade-off: sealed segments move to cheap object storage (~10× less per");
    println!("  GB than SSD). Cold read overhead is the simulated latency of fetching WAL");
    println!("  records from that tier — the cost paid for lower storage spend. Each sealed");
    println!("  segment is immutable and independently checksummed for stronger durability.");
    println!("  BASE keeps all data on fast local storage indefinitely.");
}

/// Truncate a string to at most `max` bytes, appending `…` if cut.
fn trunc(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}\u{2026}", &s[..max.saturating_sub(1)])
    }
}
