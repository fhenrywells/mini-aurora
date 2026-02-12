use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use mini_aurora_common::{
    DurabilityState, Lsn, Page, PageId, RedoRecord, StorageApi, StorageError,
};
use mini_aurora_pagestore::materialize::materialize_page;
use mini_aurora_pagestore::page_cache::PageCache;
use mini_aurora_wal::reader::WalReader;
use mini_aurora_wal::recovery::{recover, RecoveryResult};
use mini_aurora_wal::writer::WalWriter;

/// Storage engine combining WAL + page store. Implements `StorageApi`.
///
/// All state is protected by a single mutex for simplicity in Phase 1.
/// Phase 2+ can shard the lock or use lock-free structures.
pub struct StorageEngine {
    inner: Mutex<Inner>,
}

struct Inner {
    wal_path: PathBuf,
    writer: WalWriter,
    /// Page → latest LSN that touched this page.
    page_index: HashMap<PageId, Lsn>,
    /// LSN → file offset in the WAL (for chain-walking reads).
    lsn_offsets: HashMap<Lsn, u64>,
    /// Next LSN to assign.
    next_lsn: Lsn,
    /// Current durability state.
    durability: DurabilityState,
    /// In-memory page cache.
    page_cache: PageCache,
}

impl StorageEngine {
    /// Open or create a storage engine backed by the WAL at `wal_path`.
    /// Performs recovery on startup.
    pub fn open(wal_path: &Path) -> Result<Self, StorageError> {
        // Ensure the WAL file exists
        if !wal_path.exists() {
            std::fs::File::create(wal_path)?;
        }

        // Run recovery
        let RecoveryResult {
            durability,
            page_index,
            lsn_offsets,
        } = recover(wal_path)?;

        let next_lsn = if durability.vdl == 0 {
            1
        } else {
            durability.vdl + 1
        };

        let writer = WalWriter::open(wal_path)?;
        let page_cache = PageCache::new(1024);

        Ok(Self {
            inner: Mutex::new(Inner {
                wal_path: wal_path.to_path_buf(),
                writer,
                page_index,
                lsn_offsets,
                next_lsn,
                durability,
                page_cache,
            }),
        })
    }

    /// Get the current VDL (useful for tests and the compute layer).
    pub fn current_vdl(&self) -> Lsn {
        self.inner.lock().unwrap().durability.vdl
    }
}

#[async_trait]
impl StorageApi for StorageEngine {
    async fn append_redo(&self, mut records: Vec<RedoRecord>) -> Result<Lsn, StorageError> {
        let mut inner = self.inner.lock().unwrap();

        // Assign LSNs and update prev_lsn chains
        for record in &mut records {
            record.lsn = inner.next_lsn;
            inner.next_lsn += 1;

            // Set prev_lsn to the current latest LSN for this page
            record.prev_lsn = inner.page_index.get(&record.page_id).copied().unwrap_or(0);
        }

        // Get the file position before writing (for offset tracking)
        // We need to calculate offsets based on what we know
        let mut current_offset = {
            let metadata = std::fs::metadata(&inner.wal_path)?;
            metadata.len()
        };

        // Write to WAL
        inner.writer.append_batch(&records)?;
        inner.writer.sync()?;

        // Update in-memory indexes
        for record in &records {
            inner.lsn_offsets.insert(record.lsn, current_offset);
            // Advance offset past this entry
            current_offset += mini_aurora_common::LOG_ENTRY_HEADER_SIZE as u64
                + record.data.len() as u64;

            let entry = inner.page_index.entry(record.page_id).or_insert(0);
            if record.lsn > *entry {
                *entry = record.lsn;
            }
        }

        // Update durability watermarks
        // In Phase 1 (single node), VCL = highest LSN written
        let highest_lsn = records.last().map(|r| r.lsn).unwrap_or(inner.durability.vcl);
        inner.durability.vcl = highest_lsn;

        // VDL = highest CPL ≤ VCL
        if let Some(cpl) = records.iter().rev().find(|r| r.is_mtr_end) {
            if cpl.lsn > inner.durability.vdl {
                inner.durability.vdl = cpl.lsn;
            }
        }

        Ok(inner.durability.vdl)
    }

    async fn get_page(&self, page_id: PageId, read_point: Lsn) -> Result<Page, StorageError> {
        let inner = self.inner.lock().unwrap();

        // Check read_point doesn't exceed VDL
        if read_point > inner.durability.vdl {
            return Err(StorageError::LsnBeyondDurable {
                requested: read_point,
                durable: inner.durability.vdl,
            });
        }

        // Check page cache first
        if let Some(page) = inner.page_cache.get(page_id, read_point) {
            return Ok(page);
        }

        // Find the latest LSN for this page
        let latest_lsn = inner.page_index.get(&page_id).copied().unwrap_or(0);
        if latest_lsn == 0 {
            return Err(StorageError::PageNotFound {
                page_id,
                lsn: read_point,
            });
        }

        // Collect the redo chain and materialize
        let mut reader = WalReader::open(&inner.wal_path)?;
        let chain = reader.collect_page_chain(
            page_id,
            latest_lsn,
            read_point,
            &inner.lsn_offsets,
        )?;

        if chain.is_empty() {
            return Err(StorageError::PageNotFound {
                page_id,
                lsn: read_point,
            });
        }

        let page = materialize_page(page_id, &chain)?;

        // Cache the result
        inner.page_cache.insert(page_id, read_point, page);

        Ok(page)
    }

    async fn get_durability_state(&self) -> Result<DurabilityState, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.durability.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_engine(dir: &TempDir) -> StorageEngine {
        let wal_path = dir.path().join("test.wal");
        StorageEngine::open(&wal_path).unwrap()
    }

    #[tokio::test]
    async fn test_write_and_read_single_page() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

        // Write a record to page 1
        let records = vec![RedoRecord {
            lsn: 0, // will be assigned
            page_id: 1,
            offset: 0,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        }];

        let vdl = engine.append_redo(records).await.unwrap();
        assert_eq!(vdl, 1);

        // Read it back
        let page = engine.get_page(1, 1).await.unwrap();
        assert_eq!(&page[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(page[4], 0); // rest is zero
    }

    #[tokio::test]
    async fn test_multiple_records_same_page() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

        // First write
        let records1 = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 0,
            data: vec![0x11, 0x22],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        }];
        engine.append_redo(records1).await.unwrap();

        // Second write
        let records2 = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 4,
            data: vec![0x33, 0x44],
            prev_lsn: 0,
            mtr_id: 2,
            is_mtr_end: true,
        }];
        let vdl = engine.append_redo(records2).await.unwrap();

        // Read at latest
        let page = engine.get_page(1, vdl).await.unwrap();
        assert_eq!(&page[0..2], &[0x11, 0x22]);
        assert_eq!(&page[4..6], &[0x33, 0x44]);
    }

    #[tokio::test]
    async fn test_read_at_earlier_lsn() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

        // MTR 1: write to page 1
        let records1 = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 0,
            data: vec![0xAA],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        }];
        engine.append_redo(records1).await.unwrap();

        // MTR 2: overwrite page 1
        let records2 = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 0,
            data: vec![0xBB],
            prev_lsn: 0,
            mtr_id: 2,
            is_mtr_end: true,
        }];
        engine.append_redo(records2).await.unwrap();

        // Read at LSN 1 should see the old value
        let page_v1 = engine.get_page(1, 1).await.unwrap();
        assert_eq!(page_v1[0], 0xAA);

        // Read at LSN 2 should see the new value
        let page_v2 = engine.get_page(1, 2).await.unwrap();
        assert_eq!(page_v2[0], 0xBB);
    }

    #[tokio::test]
    async fn test_read_beyond_vdl_fails() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

        let records = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 0,
            data: vec![0x01],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        }];
        engine.append_redo(records).await.unwrap();

        let result = engine.get_page(1, 100).await;
        assert!(matches!(result, Err(StorageError::LsnBeyondDurable { .. })));
    }

    #[tokio::test]
    async fn test_durability_state() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

        let state = engine.get_durability_state().await.unwrap();
        assert_eq!(state.vcl, 0);
        assert_eq!(state.vdl, 0);

        // Write a complete MTR
        let records = vec![
            RedoRecord { lsn: 0, page_id: 1, offset: 0, data: vec![1], prev_lsn: 0, mtr_id: 1, is_mtr_end: false },
            RedoRecord { lsn: 0, page_id: 2, offset: 0, data: vec![2], prev_lsn: 0, mtr_id: 1, is_mtr_end: true },
        ];
        engine.append_redo(records).await.unwrap();

        let state = engine.get_durability_state().await.unwrap();
        assert_eq!(state.vcl, 2);
        assert_eq!(state.vdl, 2);
    }

    #[tokio::test]
    async fn test_recovery_preserves_data() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write some data
        {
            let engine = StorageEngine::open(&wal_path).unwrap();
            let records = vec![RedoRecord {
                lsn: 0,
                page_id: 1,
                offset: 0,
                data: vec![0xCA, 0xFE],
                prev_lsn: 0,
                mtr_id: 1,
                is_mtr_end: true,
            }];
            engine.append_redo(records).await.unwrap();
        }

        // Reopen (simulates restart) and verify data is still there
        {
            let engine = StorageEngine::open(&wal_path).unwrap();
            let page = engine.get_page(1, 1).await.unwrap();
            assert_eq!(&page[0..2], &[0xCA, 0xFE]);
        }
    }
}
