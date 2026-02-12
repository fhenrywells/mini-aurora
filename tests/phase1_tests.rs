use std::sync::Arc;

use mini_aurora_common::{PageId, RedoRecord, StorageApi};
use mini_aurora_compute::engine::ComputeEngine;
use mini_aurora_storage::engine::StorageEngine;
use mini_aurora_wal::recovery::recover;
use mini_aurora_wal::writer::WalWriter;
use tempfile::TempDir;

fn setup() -> (TempDir, Arc<StorageEngine>) {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("test.wal");
    let engine = Arc::new(StorageEngine::open(&wal_path).unwrap());
    (dir, engine)
}

fn make_compute(storage: Arc<StorageEngine>) -> ComputeEngine {
    ComputeEngine::new(storage, 256)
}

// =========================================================================
// Test 1: Write single redo record, read back page — verify bytes at offset
// =========================================================================
#[tokio::test]
async fn test_single_record_read_back() {
    let (_dir, storage) = setup();
    let compute = make_compute(storage);

    let data = b"Hello, Aurora!".to_vec();
    compute.put(1, 100, data.clone()).await.unwrap();

    let page = compute.get(1).await.unwrap();
    assert_eq!(&page[100..100 + data.len()], data.as_slice());

    // Rest of page should be zero
    assert_eq!(&page[0..100], &[0u8; 100]);
    assert_eq!(page[100 + data.len()], 0);
}

// =========================================================================
// Test 2: Write multiple records to same page — verify they compose
// =========================================================================
#[tokio::test]
async fn test_multiple_records_compose() {
    let (_dir, storage) = setup();
    let compute = make_compute(storage);

    compute.put(1, 0, vec![0x11, 0x22, 0x33, 0x44]).await.unwrap();
    compute.put(1, 8, vec![0xAA, 0xBB]).await.unwrap();
    compute.put(1, 2, vec![0xFF]).await.unwrap(); // overwrite byte 2

    let page = compute.get(1).await.unwrap();
    assert_eq!(page[0], 0x11);
    assert_eq!(page[1], 0x22);
    assert_eq!(page[2], 0xFF); // overwritten
    assert_eq!(page[3], 0x44);
    assert_eq!(page[8], 0xAA);
    assert_eq!(page[9], 0xBB);
}

// =========================================================================
// Test 3: Write MTR (3 records, last is CPL) — verify atomicity
// =========================================================================
#[tokio::test]
async fn test_mtr_atomicity() {
    let (_dir, storage) = setup();
    let compute = make_compute(storage);

    // Write across 3 pages atomically
    compute
        .put_multi(vec![
            (1, 0, vec![0x01]),
            (2, 0, vec![0x02]),
            (3, 0, vec![0x03]),
        ])
        .await
        .unwrap();

    // All three pages should have data
    let p1 = compute.get(1).await.unwrap();
    let p2 = compute.get(2).await.unwrap();
    let p3 = compute.get(3).await.unwrap();
    assert_eq!(p1[0], 0x01);
    assert_eq!(p2[0], 0x02);
    assert_eq!(p3[0], 0x03);
}

// =========================================================================
// Test 4: Simulate crash (truncate WAL mid-record) — verify recovery
// =========================================================================
#[tokio::test]
async fn test_crash_mid_record_recovery() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("test.wal");

    // Write 2 complete MTRs
    {
        let engine = StorageEngine::open(&wal_path).unwrap();
        let records1 = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 0,
            data: vec![0xAA; 100],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        }];
        engine.append_redo(records1).await.unwrap();

        let records2 = vec![RedoRecord {
            lsn: 0,
            page_id: 2,
            offset: 0,
            data: vec![0xBB; 50],
            prev_lsn: 0,
            mtr_id: 2,
            is_mtr_end: true,
        }];
        engine.append_redo(records2).await.unwrap();
    }

    // Simulate crash: append partial garbage
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        file.write_all(&[0xFF; 20]).unwrap(); // partial/corrupted entry
    }

    // Recover and verify
    let engine = StorageEngine::open(&wal_path).unwrap();
    assert_eq!(engine.current_vdl(), 2);

    let page1 = engine.get_page(1, 1).await.unwrap();
    assert_eq!(&page1[0..4], &[0xAA; 4]);

    let page2 = engine.get_page(2, 2).await.unwrap();
    assert_eq!(&page2[0..4], &[0xBB; 4]);
}

// =========================================================================
// Test 5: Simulate crash mid-MTR — verify incomplete MTR rolled back
// =========================================================================
#[tokio::test]
async fn test_crash_mid_mtr_recovery() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("test.wal");

    // Write a complete MTR (records 1-2), then an incomplete MTR (record 3, no CPL)
    {
        let mut writer = WalWriter::open(&wal_path).unwrap();
        let complete_mtr = vec![
            RedoRecord { lsn: 1, page_id: 1, offset: 0, data: vec![0xAA], prev_lsn: 0, mtr_id: 1, is_mtr_end: false },
            RedoRecord { lsn: 2, page_id: 2, offset: 0, data: vec![0xBB], prev_lsn: 0, mtr_id: 1, is_mtr_end: true },
        ];
        writer.append_batch(&complete_mtr).unwrap();

        let incomplete_mtr = vec![
            RedoRecord { lsn: 3, page_id: 3, offset: 0, data: vec![0xCC], prev_lsn: 0, mtr_id: 2, is_mtr_end: false },
        ];
        writer.append_batch(&incomplete_mtr).unwrap();
        writer.sync().unwrap();
    }

    // Recover — should truncate at VDL=2
    let result = recover(&wal_path).unwrap();
    assert_eq!(result.durability.vdl, 2);

    // Reopen engine and verify only complete MTR is visible
    let engine = StorageEngine::open(&wal_path).unwrap();
    let page1 = engine.get_page(1, 2).await.unwrap();
    assert_eq!(page1[0], 0xAA);

    let page2 = engine.get_page(2, 2).await.unwrap();
    assert_eq!(page2[0], 0xBB);

    // Page 3 should not exist (incomplete MTR was rolled back)
    let result = engine.get_page(3, 2).await;
    assert!(result.is_err());
}

// =========================================================================
// Test 6: Page cache hit/miss behavior
// =========================================================================
#[tokio::test]
async fn test_page_cache_behavior() {
    let (_dir, storage) = setup();
    let compute = make_compute(storage);

    compute.put(1, 0, vec![0x42]).await.unwrap();

    // First read: cache miss → materializes from WAL
    let page1 = compute.get(1).await.unwrap();
    assert_eq!(page1[0], 0x42);

    // Second read: should hit cache (same result)
    let page2 = compute.get(1).await.unwrap();
    assert_eq!(page2[0], 0x42);
    assert_eq!(page1, page2);
}

// =========================================================================
// Test 7: Large number of pages — verify index correctness
// =========================================================================
#[tokio::test]
async fn test_many_pages() {
    let (_dir, storage) = setup();
    let compute = make_compute(storage);

    let num_pages = 100;

    // Write one record to each page
    for i in 0..num_pages {
        compute
            .put(i as PageId, 0, vec![(i & 0xFF) as u8])
            .await
            .unwrap();
    }

    // Verify each page
    for i in 0..num_pages {
        let page = compute.get(i as PageId).await.unwrap();
        assert_eq!(page[0], (i & 0xFF) as u8, "page {i} mismatch");
    }
}

// =========================================================================
// Test 8: Versioned reads — read same page at different LSNs
// =========================================================================
#[tokio::test]
async fn test_versioned_reads() {
    let (_dir, storage) = setup();

    // Write 3 versions of page 1
    let records1 = vec![RedoRecord {
        lsn: 0,
        page_id: 1,
        offset: 0,
        data: vec![0x01],
        prev_lsn: 0,
        mtr_id: 1,
        is_mtr_end: true,
    }];
    storage.append_redo(records1).await.unwrap();

    let records2 = vec![RedoRecord {
        lsn: 0,
        page_id: 1,
        offset: 0,
        data: vec![0x02],
        prev_lsn: 0,
        mtr_id: 2,
        is_mtr_end: true,
    }];
    storage.append_redo(records2).await.unwrap();

    let records3 = vec![RedoRecord {
        lsn: 0,
        page_id: 1,
        offset: 0,
        data: vec![0x03],
        prev_lsn: 0,
        mtr_id: 3,
        is_mtr_end: true,
    }];
    storage.append_redo(records3).await.unwrap();

    // Read at each version
    let v1 = storage.get_page(1, 1).await.unwrap();
    let v2 = storage.get_page(1, 2).await.unwrap();
    let v3 = storage.get_page(1, 3).await.unwrap();

    assert_eq!(v1[0], 0x01);
    assert_eq!(v2[0], 0x02);
    assert_eq!(v3[0], 0x03);
}

// =========================================================================
// Test 9: Recovery preserves data across restart
// =========================================================================
#[tokio::test]
async fn test_restart_preserves_data() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("test.wal");

    // Session 1: write data
    {
        let engine = Arc::new(StorageEngine::open(&wal_path).unwrap());
        let compute = make_compute(engine);
        compute.put(1, 0, b"persistent".to_vec()).await.unwrap();
        compute.put(2, 0, b"data".to_vec()).await.unwrap();
    }

    // Session 2: verify data survives
    {
        let engine = Arc::new(StorageEngine::open(&wal_path).unwrap());
        let compute = make_compute(engine.clone());

        // Refresh read point from storage
        compute.refresh_read_point().await.unwrap();

        let page1 = compute.get(1).await.unwrap();
        assert_eq!(&page1[0..10], b"persistent");

        let page2 = compute.get(2).await.unwrap();
        assert_eq!(&page2[0..4], b"data");
    }
}

// =========================================================================
// Test 10: Durability state tracking
// =========================================================================
#[tokio::test]
async fn test_durability_tracking() {
    let (_dir, storage) = setup();

    let state = storage.get_durability_state().await.unwrap();
    assert_eq!(state.vcl, 0);
    assert_eq!(state.vdl, 0);

    // Write with CPL
    let records = vec![
        RedoRecord { lsn: 0, page_id: 1, offset: 0, data: vec![1], prev_lsn: 0, mtr_id: 1, is_mtr_end: false },
        RedoRecord { lsn: 0, page_id: 2, offset: 0, data: vec![2], prev_lsn: 0, mtr_id: 1, is_mtr_end: false },
        RedoRecord { lsn: 0, page_id: 3, offset: 0, data: vec![3], prev_lsn: 0, mtr_id: 1, is_mtr_end: true },
    ];
    storage.append_redo(records).await.unwrap();

    let state = storage.get_durability_state().await.unwrap();
    assert_eq!(state.vcl, 3);
    assert_eq!(state.vdl, 3);

    // Write without CPL (incomplete MTR)
    let records = vec![
        RedoRecord { lsn: 0, page_id: 4, offset: 0, data: vec![4], prev_lsn: 0, mtr_id: 2, is_mtr_end: false },
    ];
    storage.append_redo(records).await.unwrap();

    let state = storage.get_durability_state().await.unwrap();
    assert_eq!(state.vcl, 4);
    assert_eq!(state.vdl, 3); // VDL unchanged — no new CPL
}

// =========================================================================
// Test 11: Multiple pages interleaved
// =========================================================================
#[tokio::test]
async fn test_interleaved_pages() {
    let (_dir, storage) = setup();
    let compute = make_compute(storage);

    // Interleave writes to two pages
    compute.put(1, 0, vec![0x10]).await.unwrap();
    compute.put(2, 0, vec![0x20]).await.unwrap();
    compute.put(1, 1, vec![0x11]).await.unwrap();
    compute.put(2, 1, vec![0x21]).await.unwrap();

    let p1 = compute.get(1).await.unwrap();
    let p2 = compute.get(2).await.unwrap();

    assert_eq!(p1[0], 0x10);
    assert_eq!(p1[1], 0x11);
    assert_eq!(p2[0], 0x20);
    assert_eq!(p2[1], 0x21);
}
