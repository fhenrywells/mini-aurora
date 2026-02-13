use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;

use crate::viz::compute::VizComputeEngine;
use crate::viz::engine::VizStorageEngine;
use crate::viz::events::VizConfig;
use crate::viz::renderer::VizRenderer;
use crate::viz::tracer::JsonTracer;

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

/// Run a scenario from the CLI.
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

    // Set up viz infrastructure with rendering disabled (headless)
    let config = VizConfig {
        step_delay: Duration::ZERO,
        color: false,
        enabled: false,
    };
    let mut renderer_inner = VizRenderer::new(config);
    if let Some(path) = trace_json {
        let tracer = JsonTracer::open(Path::new(path))?;
        renderer_inner.set_tracer(tracer);
        println!("Tracing events to: {path}");
    }
    let renderer = Arc::new(Mutex::new(renderer_inner));

    let storage: Arc<VizStorageEngine> = match preset {
        "tiered" => {
            let base_dir = PathBuf::from("/tmp/mini-aurora-scenario-tiered");
            let _ = std::fs::remove_dir_all(&base_dir);
            let cold_latency = Duration::from_millis(cold_latency_ms);
            println!("Tiered storage: segment_size={segment_size}B, cold_latency={cold_latency_ms}ms");
            Arc::new(VizStorageEngine::open_tiered(&base_dir, segment_size, cold_latency, renderer.clone())?)
        }
        _ => {
            let wal_path = PathBuf::from("/tmp/mini-aurora-scenario.wal");
            let _ = std::fs::remove_file(&wal_path);
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

    let nodes: std::collections::HashMap<String, Arc<VizComputeEngine>> = [
        ("A".to_string(), node_a),
        ("B".to_string(), node_b),
    ].into();

    let default_node = "A".to_string();

    execute_steps(&scenario.steps, &nodes, &default_node).await?;

    // Print final metrics
    let r = renderer.lock().unwrap();
    if let Some(summary) = r.metrics_summary() {
        println!("\n=== Metrics ===");
        println!("{summary}");
    }

    println!("\nScenario complete.");
    Ok(())
}

fn execute_steps<'a>(
    steps: &'a [ScenarioStep],
    nodes: &'a std::collections::HashMap<String, Arc<VizComputeEngine>>,
    default_node: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
    Box::pin(async move {
        for step in steps {
            execute_step(step, nodes, default_node).await?;
        }
        Ok(())
    })
}

async fn execute_step(
    step: &ScenarioStep,
    nodes: &std::collections::HashMap<String, Arc<VizComputeEngine>>,
    default_node: &str,
) -> anyhow::Result<()> {
    match step {
        ScenarioStep::Put { page_id, offset, data, node } => {
            let node_key = node.as_deref().unwrap_or(default_node);
            let compute = nodes.get(node_key)
                .ok_or_else(|| anyhow::anyhow!("Unknown node: {node_key}"))?;
            let vdl = compute.put(*page_id, *offset, data.as_bytes().to_vec()).await?;
            println!("  [{node_key}] PUT pg{page_id} @{offset} {:?} -> VDL={vdl}", data);
        }
        ScenarioStep::Get { page_id, node } => {
            let node_key = node.as_deref().unwrap_or(default_node);
            let compute = nodes.get(node_key)
                .ok_or_else(|| anyhow::anyhow!("Unknown node: {node_key}"))?;
            match compute.get(*page_id).await {
                Ok(page) => {
                    let end = page.iter().position(|&b| b == 0).unwrap_or(mini_aurora_common::PAGE_SIZE);
                    let preview = if end == 0 {
                        "(empty)".to_string()
                    } else {
                        format!("{:?}", String::from_utf8_lossy(&page[..end.min(40)]))
                    };
                    println!("  [{node_key}] GET pg{page_id} -> {preview}");
                }
                Err(e) => println!("  [{node_key}] GET pg{page_id} -> Error: {e}"),
            }
        }
        ScenarioStep::Refresh { node } => {
            let node_key = node.as_deref().unwrap_or(default_node);
            let compute = nodes.get(node_key)
                .ok_or_else(|| anyhow::anyhow!("Unknown node: {node_key}"))?;
            let rp = compute.refresh_read_point().await?;
            println!("  [{node_key}] REFRESH -> rp={rp}");
        }
        ScenarioStep::SleepMs { value } => {
            println!("  sleep {value}ms");
            tokio::time::sleep(Duration::from_millis(*value)).await;
        }
        ScenarioStep::Repeat { count, steps } => {
            println!("  repeat {count}x:");
            for i in 0..*count {
                if *count <= 10 || i % (*count / 5).max(1) == 0 {
                    println!("    iteration {}/{count}", i + 1);
                }
                execute_steps(steps, nodes, default_node).await?;
            }
        }
    }
    Ok(())
}
