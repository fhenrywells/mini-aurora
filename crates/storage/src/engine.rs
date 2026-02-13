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
use mini_aurora_wal::segment::{LsnLocation, SegmentManager, Tier};
use mini_aurora_wal::writer::WalWriter;

use crate::config::{StoragePreset, TieredConfig};

/// Storage engine combining WAL + page store. Implements `StorageApi`.
///
/// Supports two backends:
/// - `SingleFile`: original single WAL file (Phase 1 design)
/// - `Segmented`: multiple WAL segments with hot/cold tiering (Phase 2)
pub struct StorageEngine {
    inner: Mutex<Inner>,
}

struct Inner {
    backend: WalBackend,
    /// Page → latest LSN that touched this page.
    page_index: HashMap<PageId, Lsn>,
    /// Next LSN to assign.
    next_lsn: Lsn,
    /// Current durability state.
    durability: DurabilityState,
    /// In-memory page cache.
    page_cache: PageCache,
}

enum WalBackend {
    SingleFile {
        wal_path: PathBuf,
        writer: WalWriter,
        lsn_offsets: HashMap<Lsn, u64>,
    },
    Segmented {
        manager: SegmentManager,
        lsn_offsets: HashMap<Lsn, LsnLocation>,
    },
}

impl StorageEngine {
    /// Open or create a storage engine backed by a single WAL file.
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
                backend: WalBackend::SingleFile {
                    wal_path: wal_path.to_path_buf(),
                    writer,
                    lsn_offsets,
                },
                page_index,
                next_lsn,
                durability,
                page_cache,
            }),
        })
    }

    /// Open a storage engine with the specified preset.
    pub fn open_with_preset(preset: StoragePreset) -> Result<Self, StorageError> {
        match preset {
            StoragePreset::Base => {
                // Base preset uses a default WAL path
                let wal_path = PathBuf::from("/tmp/mini-aurora-base.wal");
                Self::open(&wal_path)
            }
            StoragePreset::Tiered(config) => Self::open_tiered(config),
        }
    }

    /// Open a storage engine with segmented WAL and hot/cold tiering.
    fn open_tiered(config: TieredConfig) -> Result<Self, StorageError> {
        let mut manager = SegmentManager::open(
            &config.base_dir,
            config.segment_size_bytes,
            config.cold_latency,
        )?;

        let recovery = manager.recover()?;

        let next_lsn = if recovery.durability.vdl == 0 {
            1
        } else {
            recovery.durability.vdl + 1
        };

        let page_cache = PageCache::new(1024);

        Ok(Self {
            inner: Mutex::new(Inner {
                backend: WalBackend::Segmented {
                    manager,
                    lsn_offsets: recovery.lsn_offsets,
                },
                page_index: recovery.page_index,
                next_lsn,
                durability: recovery.durability,
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

        // Write to WAL (backend-specific)
        match &mut inner.backend {
            WalBackend::SingleFile { wal_path, writer, lsn_offsets } => {
                let mut current_offset = {
                    let metadata = std::fs::metadata(wal_path.as_path())?;
                    metadata.len()
                };

                writer.append_batch(&records)?;
                writer.sync()?;

                for record in &records {
                    lsn_offsets.insert(record.lsn, current_offset);
                    current_offset += mini_aurora_common::LOG_ENTRY_HEADER_SIZE as u64
                        + record.data.len() as u64;
                }
            }
            WalBackend::Segmented { manager, lsn_offsets } => {
                let locations = manager.append_batch(&records)?;
                manager.sync()?;

                for (record, loc) in records.iter().zip(locations.iter()) {
                    lsn_offsets.insert(record.lsn, *loc);
                }
            }
        }

        // Update page index
        for record in &records {
            let entry = inner.page_index.entry(record.page_id).or_insert(0);
            if record.lsn > *entry {
                *entry = record.lsn;
            }
        }

        // Update durability watermarks
        let highest_lsn = records.last().map(|r| r.lsn).unwrap_or(inner.durability.vcl);
        inner.durability.vcl = highest_lsn;

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

        // Collect the redo chain and materialize (backend-specific)
        let chain = match &inner.backend {
            WalBackend::SingleFile { wal_path, lsn_offsets, .. } => {
                let mut reader = WalReader::open(wal_path)?;
                reader.collect_page_chain(page_id, latest_lsn, read_point, lsn_offsets)?
            }
            WalBackend::Segmented { manager, lsn_offsets } => {
                collect_segmented_chain(page_id, latest_lsn, read_point, lsn_offsets, manager)?
            }
        };

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

/// Walk the prev_lsn chain across segments to collect redo records.
fn collect_segmented_chain(
    page_id: PageId,
    start_lsn: Lsn,
    target_lsn: Lsn,
    lsn_offsets: &HashMap<Lsn, LsnLocation>,
    manager: &SegmentManager,
) -> Result<Vec<RedoRecord>, StorageError> {
    let mut chain = Vec::new();
    let mut current_lsn = start_lsn;
    let mut last_segment_id: Option<u32> = None;
    let mut reader_cache: Option<(WalReader, Tier)> = None;

    while current_lsn != 0 {
        // Skip records beyond target_lsn
        if current_lsn > target_lsn {
            let loc = match lsn_offsets.get(&current_lsn) {
                Some(loc) => loc,
                None => break,
            };
            // Open reader for this segment to get prev_lsn
            let (mut reader, _tier) = manager.open_segment_reader(loc.segment_id)?;
            reader.seek_to(loc.file_offset)?;
            match reader.read_entry()? {
                mini_aurora_wal::reader::ReadResult::Entry(hdr, _data) => {
                    current_lsn = hdr.prev_lsn;
                    continue;
                }
                _ => break,
            }
        }

        let loc = match lsn_offsets.get(&current_lsn) {
            Some(loc) => loc,
            None => break,
        };

        // Reuse reader if same segment, otherwise open new one
        let need_new_reader = last_segment_id != Some(loc.segment_id);
        if need_new_reader {
            let (reader, tier) = manager.open_segment_reader(loc.segment_id)?;
            // Inject cold latency on first read from a cold segment
            if tier == Tier::Cold {
                manager.inject_cold_latency();
            }
            reader_cache = Some((reader, tier));
            last_segment_id = Some(loc.segment_id);
        }

        let (ref mut reader, _) = reader_cache.as_mut().unwrap();
        reader.seek_to(loc.file_offset)?;
        match reader.read_entry()? {
            mini_aurora_wal::reader::ReadResult::Entry(hdr, data) => {
                debug_assert_eq!(hdr.page_id, page_id);
                let record = mini_aurora_wal::reader::header_to_record(&hdr, data);
                let prev = hdr.prev_lsn;
                chain.push(record);
                current_lsn = prev;
            }
            _ => break,
        }
    }

    chain.reverse(); // oldest first for replay
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_engine(dir: &TempDir) -> StorageEngine {
        let wal_path = dir.path().join("test.wal");
        StorageEngine::open(&wal_path).unwrap()
    }

    fn new_tiered_engine(dir: &TempDir) -> StorageEngine {
        let config = TieredConfig {
            segment_size_bytes: 64 * 1024, // 64KB
            cold_latency: std::time::Duration::ZERO,
            base_dir: dir.path().to_path_buf(),
        };
        StorageEngine::open_with_preset(StoragePreset::Tiered(config)).unwrap()
    }

    #[tokio::test]
    async fn test_write_and_read_single_page() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

        let records = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 0,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        }];

        let vdl = engine.append_redo(records).await.unwrap();
        assert_eq!(vdl, 1);

        let page = engine.get_page(1, 1).await.unwrap();
        assert_eq!(&page[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(page[4], 0);
    }

    #[tokio::test]
    async fn test_multiple_records_same_page() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

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

        let page = engine.get_page(1, vdl).await.unwrap();
        assert_eq!(&page[0..2], &[0x11, 0x22]);
        assert_eq!(&page[4..6], &[0x33, 0x44]);
    }

    #[tokio::test]
    async fn test_read_at_earlier_lsn() {
        let dir = TempDir::new().unwrap();
        let engine = new_engine(&dir);

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

        let page_v1 = engine.get_page(1, 1).await.unwrap();
        assert_eq!(page_v1[0], 0xAA);

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

        {
            let engine = StorageEngine::open(&wal_path).unwrap();
            let page = engine.get_page(1, 1).await.unwrap();
            assert_eq!(&page[0..2], &[0xCA, 0xFE]);
        }
    }

    // --- Tiered storage tests ---

    #[tokio::test]
    async fn test_tiered_write_and_read() {
        let dir = TempDir::new().unwrap();
        let engine = new_tiered_engine(&dir);

        let records = vec![RedoRecord {
            lsn: 0,
            page_id: 1,
            offset: 0,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        }];

        let vdl = engine.append_redo(records).await.unwrap();
        assert_eq!(vdl, 1);

        let page = engine.get_page(1, 1).await.unwrap();
        assert_eq!(&page[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[tokio::test]
    async fn test_tiered_multiple_records() {
        let dir = TempDir::new().unwrap();
        let engine = new_tiered_engine(&dir);

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

        let page = engine.get_page(1, vdl).await.unwrap();
        assert_eq!(&page[0..2], &[0x11, 0x22]);
        assert_eq!(&page[4..6], &[0x33, 0x44]);
    }

    #[tokio::test]
    async fn test_tiered_versioned_reads() {
        let dir = TempDir::new().unwrap();
        let engine = new_tiered_engine(&dir);

        let r1 = vec![RedoRecord {
            lsn: 0, page_id: 1, offset: 0, data: vec![0xAA],
            prev_lsn: 0, mtr_id: 1, is_mtr_end: true,
        }];
        engine.append_redo(r1).await.unwrap();

        let r2 = vec![RedoRecord {
            lsn: 0, page_id: 1, offset: 0, data: vec![0xBB],
            prev_lsn: 0, mtr_id: 2, is_mtr_end: true,
        }];
        engine.append_redo(r2).await.unwrap();

        let page_v1 = engine.get_page(1, 1).await.unwrap();
        assert_eq!(page_v1[0], 0xAA);

        let page_v2 = engine.get_page(1, 2).await.unwrap();
        assert_eq!(page_v2[0], 0xBB);
    }
}
