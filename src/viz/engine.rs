use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mini_aurora_common::{
    DurabilityState, Lsn, Page, PageId, RedoRecord, StorageApi, StorageError,
    LOG_ENTRY_HEADER_SIZE, empty_page, PAGE_SIZE,
};
use mini_aurora_pagestore::page_cache::PageCache;
use mini_aurora_wal::reader::{ReadResult, WalReader, header_to_record};
use mini_aurora_wal::recovery::{recover, RecoveryResult};
use mini_aurora_wal::segment::{LsnLocation, SegmentManager, Tier};
use mini_aurora_wal::writer::WalWriter;

use super::events::VizEvent;
use super::renderer::{VizRenderer, data_preview};

/// Storage engine with visualization events emitted between each internal step.
///
/// Mirrors `StorageEngine` from `crates/storage/src/engine.rs` but interleaves
/// `VizEvent` emissions so every sub-operation is observable.
pub struct VizStorageEngine {
    inner: Mutex<VizInner>,
    renderer: Arc<Mutex<VizRenderer>>,
}

enum VizWalBackend {
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

struct VizInner {
    backend: VizWalBackend,
    page_index: HashMap<PageId, Lsn>,
    next_lsn: Lsn,
    durability: DurabilityState,
    page_cache: PageCache,
}

impl VizStorageEngine {
    /// Open or create a storage engine backed by a single WAL file.
    pub fn open(
        wal_path: &Path,
        renderer: Arc<Mutex<VizRenderer>>,
    ) -> Result<Self, StorageError> {
        if !wal_path.exists() {
            std::fs::File::create(wal_path)?;
        }

        let RecoveryResult {
            durability,
            page_index,
            lsn_offsets,
        } = recover(wal_path)?;

        let next_lsn = if durability.vdl == 0 { 1 } else { durability.vdl + 1 };

        let writer = WalWriter::open(wal_path)?;
        let page_cache = PageCache::new(1024);

        Ok(Self {
            inner: Mutex::new(VizInner {
                backend: VizWalBackend::SingleFile {
                    wal_path: wal_path.to_path_buf(),
                    writer,
                    lsn_offsets,
                },
                page_index,
                next_lsn,
                durability,
                page_cache,
            }),
            renderer,
        })
    }

    /// Open a storage engine with segmented WAL and hot/cold tiering.
    pub fn open_tiered(
        base_dir: &Path,
        segment_size_bytes: u64,
        cold_latency: std::time::Duration,
        renderer: Arc<Mutex<VizRenderer>>,
    ) -> Result<Self, StorageError> {
        let mut manager = SegmentManager::open(base_dir, segment_size_bytes, cold_latency)?;
        let recovery = manager.recover()?;

        let next_lsn = if recovery.durability.vdl == 0 { 1 } else { recovery.durability.vdl + 1 };
        let page_cache = PageCache::new(1024);

        Ok(Self {
            inner: Mutex::new(VizInner {
                backend: VizWalBackend::Segmented {
                    manager,
                    lsn_offsets: recovery.lsn_offsets,
                },
                page_index: recovery.page_index,
                next_lsn,
                durability: recovery.durability,
                page_cache,
            }),
            renderer,
        })
    }

    fn emit(&self, event: VizEvent) {
        self.renderer.lock().unwrap().render(&event);
    }

    /// Build and emit a state snapshot event.
    pub fn emit_state_snapshot(
        &self,
        node_label: String,
        read_point: Lsn,
        next_mtr: u64,
        buffer_pool_pages: Vec<PageId>,
    ) {
        let inner = self.inner.lock().unwrap();

        let wal_file_size = match &inner.backend {
            VizWalBackend::SingleFile { wal_path, .. } => {
                std::fs::metadata(wal_path).map(|m| m.len()).unwrap_or(0)
            }
            VizWalBackend::Segmented { .. } => 0, // not tracked per-file for segmented
        };

        let lsn_offset_count = match &inner.backend {
            VizWalBackend::SingleFile { lsn_offsets, .. } => lsn_offsets.len(),
            VizWalBackend::Segmented { lsn_offsets, .. } => lsn_offsets.len(),
        };

        let wal_lsn_range = if inner.next_lsn > 1 {
            Some((1, inner.next_lsn - 1))
        } else {
            None
        };

        let event = VizEvent::StateSnapshot {
            node_label,
            read_point,
            next_mtr,
            buffer_pool_pages,
            next_lsn: inner.next_lsn,
            vcl: inner.durability.vcl,
            vdl: inner.durability.vdl,
            page_index: inner.page_index.clone(),
            lsn_offset_count,
            page_cache_count: inner.page_cache.len(),
            wal_file_size,
            wal_lsn_range,
        };
        drop(inner);
        self.emit(event);
    }
}

#[async_trait]
impl StorageApi for VizStorageEngine {
    async fn append_redo(&self, mut records: Vec<RedoRecord>) -> Result<Lsn, StorageError> {
        let mut inner = self.inner.lock().unwrap();

        let first_lsn = inner.next_lsn;

        // Step: Assign LSNs
        for record in &mut records {
            record.lsn = inner.next_lsn;
            inner.next_lsn += 1;
        }
        let last_lsn = inner.next_lsn - 1;

        self.renderer.lock().unwrap().render(&VizEvent::AssignLsns { first_lsn, last_lsn });

        // Step: Link prev_lsn chains
        for record in &mut records {
            record.prev_lsn = inner.page_index.get(&record.page_id).copied().unwrap_or(0);
            self.renderer.lock().unwrap().render(&VizEvent::LinkPrevLsn {
                lsn: record.lsn,
                page_id: record.page_id,
                prev_lsn: record.prev_lsn,
            });
        }

        // Step: WAL append (backend-specific)
        match &mut inner.backend {
            VizWalBackend::SingleFile { wal_path, writer, lsn_offsets } => {
                let wal_offset = std::fs::metadata(wal_path.as_path())
                    .map(|m| m.len())
                    .unwrap_or(0);
                let total_bytes: u64 = records.iter()
                    .map(|r| LOG_ENTRY_HEADER_SIZE as u64 + r.data.len() as u64)
                    .sum();

                writer.append_batch(&records)?;

                self.renderer.lock().unwrap().render(&VizEvent::WalAppend {
                    first_lsn,
                    last_lsn,
                    offset: wal_offset,
                    bytes: total_bytes,
                });

                writer.sync()?;
                self.renderer.lock().unwrap().render(&VizEvent::WalSync);

                let mut current_offset = wal_offset;
                for record in &records {
                    lsn_offsets.insert(record.lsn, current_offset);
                    self.renderer.lock().unwrap().render(&VizEvent::UpdateLsnOffset {
                        lsn: record.lsn,
                        file_offset: current_offset,
                    });
                    current_offset += LOG_ENTRY_HEADER_SIZE as u64 + record.data.len() as u64;
                }
            }
            VizWalBackend::Segmented { manager, lsn_offsets } => {
                let total_bytes: u64 = records.iter()
                    .map(|r| LOG_ENTRY_HEADER_SIZE as u64 + r.data.len() as u64)
                    .sum();

                let locations = manager.append_batch(&records)?;

                self.renderer.lock().unwrap().render(&VizEvent::WalAppend {
                    first_lsn,
                    last_lsn,
                    offset: 0,
                    bytes: total_bytes,
                });

                manager.sync()?;
                self.renderer.lock().unwrap().render(&VizEvent::WalSync);

                for (record, loc) in records.iter().zip(locations.iter()) {
                    lsn_offsets.insert(record.lsn, *loc);
                    self.renderer.lock().unwrap().render(&VizEvent::UpdateLsnOffset {
                        lsn: record.lsn,
                        file_offset: loc.file_offset,
                    });
                }
            }
        }

        // Step: Update page index
        for record in &records {
            let entry = inner.page_index.entry(record.page_id).or_insert(0);
            if record.lsn > *entry {
                *entry = record.lsn;
            }
            self.renderer.lock().unwrap().render(&VizEvent::UpdatePageIndex {
                page_id: record.page_id,
                latest_lsn: record.lsn,
            });
        }

        // Step: Advance VCL
        let old_vcl = inner.durability.vcl;
        let highest_lsn = records.last().map(|r| r.lsn).unwrap_or(inner.durability.vcl);
        inner.durability.vcl = highest_lsn;
        self.renderer.lock().unwrap().render(&VizEvent::AdvanceVcl {
            old: old_vcl,
            new: inner.durability.vcl,
        });

        // Step: Advance VDL
        let old_vdl = inner.durability.vdl;
        if let Some(cpl) = records.iter().rev().find(|r| r.is_mtr_end) {
            if cpl.lsn > inner.durability.vdl {
                inner.durability.vdl = cpl.lsn;
            }
        }
        self.renderer.lock().unwrap().render(&VizEvent::AdvanceVdl {
            old: old_vdl,
            new: inner.durability.vdl,
        });

        Ok(inner.durability.vdl)
    }

    async fn get_page(&self, page_id: PageId, read_point: Lsn) -> Result<Page, StorageError> {
        let inner = self.inner.lock().unwrap();

        if read_point > inner.durability.vdl {
            return Err(StorageError::LsnBeyondDurable {
                requested: read_point,
                durable: inner.durability.vdl,
            });
        }

        // Step: Page cache lookup
        if let Some(page) = inner.page_cache.get(page_id, read_point) {
            self.renderer.lock().unwrap().render(&VizEvent::PageCacheLookup {
                page_id,
                read_point,
                hit: true,
            });
            return Ok(page);
        }
        self.renderer.lock().unwrap().render(&VizEvent::PageCacheLookup {
            page_id,
            read_point,
            hit: false,
        });

        // Step: Page index lookup
        let latest_lsn = inner.page_index.get(&page_id).copied().unwrap_or(0);
        self.renderer.lock().unwrap().render(&VizEvent::PageIndexLookup {
            page_id,
            latest_lsn: if latest_lsn == 0 { None } else { Some(latest_lsn) },
        });

        if latest_lsn == 0 {
            return Err(StorageError::PageNotFound {
                page_id,
                lsn: read_point,
            });
        }

        // Step: Chain walk (backend-specific)
        let chain = match &inner.backend {
            VizWalBackend::SingleFile { wal_path, lsn_offsets, .. } => {
                self.walk_single_file_chain(page_id, latest_lsn, read_point, wal_path, lsn_offsets)?
            }
            VizWalBackend::Segmented { manager, lsn_offsets } => {
                self.walk_segmented_chain(page_id, latest_lsn, read_point, lsn_offsets, manager)?
            }
        };

        if chain.is_empty() {
            return Err(StorageError::PageNotFound {
                page_id,
                lsn: read_point,
            });
        }

        let lsns: Vec<Lsn> = chain.iter().map(|r| r.lsn).collect();
        self.renderer.lock().unwrap().render(&VizEvent::ChainCollected {
            page_id,
            chain_len: chain.len(),
            lsns,
        });

        // Step: Materialize page
        let mut page = empty_page();
        for record in &chain {
            let start = record.offset as usize;
            let end = start + record.data.len();
            if end > PAGE_SIZE {
                return Err(StorageError::PageOverflow {
                    offset: record.offset,
                    len: record.data.len(),
                });
            }
            page[start..end].copy_from_slice(&record.data);

            self.renderer.lock().unwrap().render(&VizEvent::MaterializeApply {
                page_id,
                lsn: record.lsn,
                offset: record.offset,
                data_len: record.data.len(),
                data_preview: data_preview(&record.data, 20),
            });
        }

        self.renderer.lock().unwrap().render(&VizEvent::MaterializeComplete { page_id, read_point });

        // Step: Cache the result
        inner.page_cache.insert(page_id, read_point, page);
        self.renderer.lock().unwrap().render(&VizEvent::PageCacheInsert { page_id, read_point });

        Ok(page)
    }

    async fn get_durability_state(&self) -> Result<DurabilityState, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.durability.clone())
    }
}

// Private chain-walk helpers
impl VizStorageEngine {
    fn walk_single_file_chain(
        &self,
        page_id: PageId,
        latest_lsn: Lsn,
        read_point: Lsn,
        wal_path: &Path,
        lsn_offsets: &HashMap<Lsn, u64>,
    ) -> Result<Vec<RedoRecord>, StorageError> {
        let mut chain = Vec::new();
        let mut current_lsn = latest_lsn;
        let mut reader = WalReader::open(wal_path)?;

        while current_lsn != 0 {
            if current_lsn > read_point {
                let offset = match lsn_offsets.get(&current_lsn) {
                    Some(&off) => off,
                    None => break,
                };
                reader.seek_to(offset)?;
                match reader.read_entry()? {
                    ReadResult::Entry(hdr, _data) => {
                        self.renderer.lock().unwrap().render(&VizEvent::ChainWalkStep {
                            page_id,
                            lsn: current_lsn,
                            prev_lsn: hdr.prev_lsn,
                            skipped: true,
                        });
                        current_lsn = hdr.prev_lsn;
                        continue;
                    }
                    _ => break,
                }
            }

            let offset = match lsn_offsets.get(&current_lsn) {
                Some(&off) => off,
                None => break,
            };
            reader.seek_to(offset)?;
            match reader.read_entry()? {
                ReadResult::Entry(hdr, data) => {
                    self.renderer.lock().unwrap().render(&VizEvent::ChainWalkStep {
                        page_id,
                        lsn: hdr.lsn,
                        prev_lsn: hdr.prev_lsn,
                        skipped: false,
                    });
                    let record = header_to_record(&hdr, data);
                    let prev = hdr.prev_lsn;
                    chain.push(record);
                    current_lsn = prev;
                }
                _ => break,
            }
        }

        chain.reverse();
        Ok(chain)
    }

    fn walk_segmented_chain(
        &self,
        page_id: PageId,
        latest_lsn: Lsn,
        read_point: Lsn,
        lsn_offsets: &HashMap<Lsn, LsnLocation>,
        manager: &SegmentManager,
    ) -> Result<Vec<RedoRecord>, StorageError> {
        let mut chain = Vec::new();
        let mut current_lsn = latest_lsn;
        let mut last_segment_id: Option<u32> = None;
        let mut reader_cache: Option<(WalReader, Tier)> = None;

        while current_lsn != 0 {
            if current_lsn > read_point {
                let loc = match lsn_offsets.get(&current_lsn) {
                    Some(loc) => loc,
                    None => break,
                };
                let (mut reader, _tier) = manager.open_segment_reader(loc.segment_id)?;
                reader.seek_to(loc.file_offset)?;
                match reader.read_entry()? {
                    ReadResult::Entry(hdr, _data) => {
                        self.renderer.lock().unwrap().render(&VizEvent::ChainWalkStep {
                            page_id,
                            lsn: current_lsn,
                            prev_lsn: hdr.prev_lsn,
                            skipped: true,
                        });
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

            let need_new_reader = last_segment_id != Some(loc.segment_id);
            if need_new_reader {
                let (reader, tier) = manager.open_segment_reader(loc.segment_id)?;
                if tier == Tier::Cold {
                    let latency_ms = manager.cold_latency().as_millis() as u64;
                    manager.inject_cold_latency();
                    self.renderer.lock().unwrap().render(&VizEvent::ColdTierRead {
                        segment_id: loc.segment_id,
                        latency_ms,
                    });
                }
                reader_cache = Some((reader, tier));
                last_segment_id = Some(loc.segment_id);
            }

            let (ref mut reader, _) = reader_cache.as_mut().unwrap();
            reader.seek_to(loc.file_offset)?;
            match reader.read_entry()? {
                ReadResult::Entry(hdr, data) => {
                    self.renderer.lock().unwrap().render(&VizEvent::ChainWalkStep {
                        page_id,
                        lsn: hdr.lsn,
                        prev_lsn: hdr.prev_lsn,
                        skipped: false,
                    });
                    let record = header_to_record(&hdr, data);
                    let prev = hdr.prev_lsn;
                    chain.push(record);
                    current_lsn = prev;
                }
                _ => break,
            }
        }

        chain.reverse();
        Ok(chain)
    }
}
