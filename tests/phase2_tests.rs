use std::sync::Arc;
use std::time::Duration;

use mini_aurora_common::{StorageApi, PAGE_SIZE};
use mini_aurora_storage::config::{StoragePreset, TieredConfig};
use mini_aurora_storage::engine::StorageEngine;
use mini_aurora_compute::engine::ComputeEngine;
use tempfile::TempDir;

fn setup_tiered(segment_size: u64) -> (TempDir, Arc<StorageEngine>) {
    let dir = TempDir::new().unwrap();
    let preset = StoragePreset::Tiered(TieredConfig {
        segment_size_bytes: segment_size,
        cold_latency: Duration::from_millis(0),
        base_dir: dir.path().to_path_buf(),
    });
    let engine = Arc::new(StorageEngine::open_with_preset(preset).unwrap());
    (dir, engine)
}

fn make_compute(storage: Arc<StorageEngine>) -> ComputeEngine {
    ComputeEngine::new(storage, 256)
}

// =========================================================================
// Test: Write and read through the full compute → tiered-storage → WAL path
// =========================================================================
#[tokio::test]
async fn test_tiered_compute_round_trip() {
    let (_dir, storage) = setup_tiered(2048);
    let compute = make_compute(storage);

    compute.put(1, 0, b"Hello Tiered".to_vec()).await.unwrap();
    let page = compute.get(1).await.unwrap();
    assert_eq!(&page[0..12], b"Hello Tiered");
}

// =========================================================================
// Test: Multiple writes trigger segment rotation, reads still work
// =========================================================================
#[tokio::test]
async fn test_tiered_segment_rotation_via_compute() {
    // Small segments to force rotation
    let (_dir, storage) = setup_tiered(512);
    let compute = make_compute(storage.clone());

    // Write enough data to fill multiple segments
    for i in 0u64..30 {
        compute.put(i % 5 + 1, 0, format!("data-{i}").into_bytes()).await.unwrap();
    }

    // Verify all pages readable
    for pid in 1u64..=5 {
        let page = compute.get(pid).await.unwrap();
        let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
        assert!(end > 0, "Page {pid} should not be empty");
    }

    // Verify durability state
    let state = storage.get_durability_state().await.unwrap();
    assert!(state.vcl >= 30, "VCL should be at least 30, got {}", state.vcl);
    assert!(state.vdl >= 30, "VDL should be at least 30, got {}", state.vdl);
}

// =========================================================================
// Test: Multi-page atomic write works with tiered storage
// =========================================================================
#[tokio::test]
async fn test_tiered_atomic_multi_page() {
    let (_dir, storage) = setup_tiered(2048);
    let compute = make_compute(storage);

    compute.put_multi(vec![
        (10, 0, b"Page Ten".to_vec()),
        (11, 0, b"Page Eleven".to_vec()),
        (12, 0, b"Page Twelve".to_vec()),
    ]).await.unwrap();

    let p10 = compute.get(10).await.unwrap();
    let p11 = compute.get(11).await.unwrap();
    let p12 = compute.get(12).await.unwrap();

    assert_eq!(&p10[0..8], b"Page Ten");
    assert_eq!(&p11[0..11], b"Page Eleven");
    assert_eq!(&p12[0..11], b"Page Twelve");
}

// =========================================================================
// Test: Overwrite same page many times across segments, latest version wins
// =========================================================================
#[tokio::test]
async fn test_tiered_overwrite_across_segments() {
    let (_dir, storage) = setup_tiered(512);
    let compute = make_compute(storage);

    for i in 0..20 {
        compute.put(1, 0, format!("version-{i:03}").into_bytes()).await.unwrap();
    }

    let page = compute.get(1).await.unwrap();
    assert_eq!(&page[0..11], b"version-019");
}

// =========================================================================
// Test: Read isolation between two compute nodes with tiered storage
// =========================================================================
#[tokio::test]
async fn test_tiered_read_isolation() {
    let (_dir, storage) = setup_tiered(2048);
    let node_a = ComputeEngine::new(storage.clone(), 256);
    let node_b = ComputeEngine::new(storage.clone(), 256);

    // Node A writes
    node_a.put(1, 0, b"from-A".to_vec()).await.unwrap();

    // Node B can't see it yet (read_point = 0)
    assert!(node_b.get(1).await.is_err());

    // Node B refreshes and now sees it
    node_b.refresh_read_point().await.unwrap();
    let page = node_b.get(1).await.unwrap();
    assert_eq!(&page[0..6], b"from-A");
}
