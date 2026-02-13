use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use mini_aurora_common::{DurabilityState, Lsn, PageId, RedoRecord, LOG_ENTRY_HEADER_SIZE};
use serde::{Deserialize, Serialize};

use crate::reader::WalReader;
use crate::writer::WalWriter;

pub type SegmentId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier {
    Hot,
    Cold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: SegmentId,
    pub filename: String,
    pub tier: Tier,
    pub lsn_range: (Lsn, Lsn),
    pub size_bytes: u64,
    pub sealed: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct LsnLocation {
    pub segment_id: SegmentId,
    pub file_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub segments: Vec<SegmentMeta>,
    /// The ID of the currently active (unsealed) segment.
    pub active_segment_id: SegmentId,
}

impl Manifest {
    fn new() -> Self {
        Self {
            segments: Vec::new(),
            active_segment_id: 1,
        }
    }
}

/// Manages multiple WAL segment files with hot/cold tiering.
pub struct SegmentManager {
    hot_dir: PathBuf,
    cold_dir: PathBuf,
    manifest: Manifest,
    active_writer: WalWriter,
    active_segment_id: SegmentId,
    active_first_lsn: Option<Lsn>,
    active_bytes_written: u64,
    max_segment_bytes: u64,
    cold_latency: Duration,
    base_dir: PathBuf,
}

impl SegmentManager {
    /// Open or create a segmented WAL in `base_dir`.
    pub fn open(base_dir: &Path, max_segment_bytes: u64, cold_latency: Duration) -> Result<Self, std::io::Error> {
        let hot_dir = base_dir.join("hot");
        let cold_dir = base_dir.join("cold");
        fs::create_dir_all(&hot_dir)?;
        fs::create_dir_all(&cold_dir)?;

        let manifest_path = base_dir.join("manifest.json");
        let manifest: Manifest = if manifest_path.exists() {
            let content = fs::read_to_string(&manifest_path)?;
            serde_json::from_str(&content).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
        } else {
            Manifest::new()
        };

        // Open (or create) the active segment for appending.
        // WalWriter::open uses create+append mode, so existing data is preserved.
        let active_segment_id = manifest.active_segment_id;
        let active_path = hot_dir.join(segment_filename(active_segment_id));
        let active_writer = WalWriter::open(&active_path)?;
        let active_bytes_written = fs::metadata(&active_path).map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            hot_dir,
            cold_dir,
            manifest,
            active_writer,
            active_segment_id,
            active_first_lsn: None,
            active_bytes_written,
            max_segment_bytes,
            cold_latency,
            base_dir: base_dir.to_path_buf(),
        })
    }

    /// Append a batch of records, rotating segments as needed.
    /// Returns LsnLocations for each record.
    pub fn append_batch(&mut self, records: &[RedoRecord]) -> Result<Vec<LsnLocation>, std::io::Error> {
        let mut locations = Vec::with_capacity(records.len());

        for record in records {
            // Check if we need to rotate before writing
            let entry_size = LOG_ENTRY_HEADER_SIZE as u64 + record.data.len() as u64;
            if self.active_bytes_written > 0 && self.active_bytes_written + entry_size > self.max_segment_bytes {
                self.rotate()?;
            }

            let file_offset = self.active_bytes_written;

            if self.active_first_lsn.is_none() {
                self.active_first_lsn = Some(record.lsn);
            }

            locations.push(LsnLocation {
                segment_id: self.active_segment_id,
                file_offset,
            });

            self.active_writer.append(record)?;
            self.active_bytes_written += entry_size;
        }

        Ok(locations)
    }

    /// Fsync the active segment.
    pub fn sync(&mut self) -> Result<(), std::io::Error> {
        self.active_writer.sync()
    }

    /// Seal the current segment and open a new one.
    /// Returns (sealed_id, new_id).
    pub fn rotate(&mut self) -> Result<(SegmentId, SegmentId), std::io::Error> {
        let sealed_id = self.active_segment_id;

        // Sync before sealing
        self.active_writer.sync()?;

        // Record the sealed segment in the manifest
        let sealed_filename = segment_filename(sealed_id);
        let first_lsn = self.active_first_lsn.unwrap_or(0);
        let sealed_meta = SegmentMeta {
            id: sealed_id,
            filename: sealed_filename,
            tier: Tier::Hot,
            lsn_range: (first_lsn, first_lsn), // updated by caller via update_sealed_lsn_range
            size_bytes: self.active_bytes_written,
            sealed: true,
        };
        self.manifest.segments.push(sealed_meta);

        // Open new segment
        let new_id = sealed_id + 1;
        self.manifest.active_segment_id = new_id;
        self.save_manifest()?;

        let new_path = self.hot_dir.join(segment_filename(new_id));
        self.active_writer = WalWriter::open(&new_path)?;

        self.active_segment_id = new_id;
        self.active_first_lsn = None;
        self.active_bytes_written = 0;

        Ok((sealed_id, new_id))
    }

    /// Update the LSN range on a sealed segment.
    pub fn update_sealed_lsn_range(&mut self, segment_id: SegmentId, lsn_range: (Lsn, Lsn)) {
        for seg in &mut self.manifest.segments {
            if seg.id == segment_id {
                seg.lsn_range = lsn_range;
            }
        }
    }

    /// Open a reader for a given segment. Returns the reader and its tier.
    pub fn open_segment_reader(&self, segment_id: SegmentId) -> Result<(WalReader, Tier), std::io::Error> {
        // Check if it's the active segment
        if segment_id == self.active_segment_id {
            let path = self.hot_dir.join(segment_filename(segment_id));
            let reader = WalReader::open(&path)?;
            return Ok((reader, Tier::Hot));
        }

        // Check manifest for sealed segments
        for seg in &self.manifest.segments {
            if seg.id == segment_id {
                let path = match seg.tier {
                    Tier::Hot => self.hot_dir.join(&seg.filename),
                    Tier::Cold => self.cold_dir.join(&seg.filename),
                };
                let reader = WalReader::open(&path)?;
                return Ok((reader, seg.tier));
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("segment {} not found", segment_id),
        ))
    }

    /// Move old sealed segments from hot to cold.
    /// Keeps the most recent `keep_hot` sealed segments in hot tier.
    pub fn cool_segments(&mut self, keep_hot: usize) -> Result<Vec<SegmentId>, std::io::Error> {
        let mut cooled = Vec::new();
        let sealed_hot: Vec<usize> = self.manifest.segments.iter()
            .enumerate()
            .filter(|(_, s)| s.sealed && s.tier == Tier::Hot)
            .map(|(i, _)| i)
            .collect();

        if sealed_hot.len() <= keep_hot {
            return Ok(cooled);
        }

        let to_cool = sealed_hot.len() - keep_hot;
        for &idx in sealed_hot.iter().take(to_cool) {
            let seg = &self.manifest.segments[idx];
            let src = self.hot_dir.join(&seg.filename);
            let dst = self.cold_dir.join(&seg.filename);
            fs::rename(&src, &dst)?;
            let seg_id = seg.id;
            self.manifest.segments[idx].tier = Tier::Cold;
            cooled.push(seg_id);
        }

        if !cooled.is_empty() {
            self.save_manifest()?;
        }

        Ok(cooled)
    }

    /// Inject cold-tier latency (blocking sleep).
    pub fn inject_cold_latency(&self) {
        if !self.cold_latency.is_zero() {
            thread::sleep(self.cold_latency);
        }
    }

    /// Get the cold latency duration.
    pub fn cold_latency(&self) -> Duration {
        self.cold_latency
    }

    /// Recover: scan all segments to rebuild indexes.
    pub fn recover(&mut self) -> Result<RecoveryData, std::io::Error> {
        let mut page_index: HashMap<PageId, Lsn> = HashMap::new();
        let mut lsn_offsets: HashMap<Lsn, LsnLocation> = HashMap::new();
        let mut all_lsns = std::collections::BTreeSet::new();
        let mut cpls = std::collections::BTreeSet::new();

        // Collect sealed segment info first to avoid borrow conflict
        let sealed_info: Vec<(PathBuf, SegmentId)> = self.manifest.segments.iter().map(|seg| {
            let path = match seg.tier {
                Tier::Hot => self.hot_dir.join(&seg.filename),
                Tier::Cold => self.cold_dir.join(&seg.filename),
            };
            (path, seg.id)
        }).collect();

        // Scan sealed segments in order
        for (path, seg_id) in &sealed_info {
            if !path.exists() {
                continue;
            }
            let mut reader = WalReader::open(path)?;
            self.scan_segment(&mut reader, *seg_id, &mut all_lsns, &mut cpls, &mut lsn_offsets, &mut page_index)?;
        }

        // Scan active segment
        let active_path = self.hot_dir.join(segment_filename(self.active_segment_id));
        if active_path.exists() {
            let mut reader = WalReader::open(&active_path)?;
            self.scan_segment(&mut reader, self.active_segment_id, &mut all_lsns, &mut cpls, &mut lsn_offsets, &mut page_index)?;
            self.active_bytes_written = fs::metadata(&active_path).map(|m| m.len()).unwrap_or(0);
        }

        // Compute VCL and VDL
        let vcl = compute_vcl(&all_lsns);
        let vdl = cpls.iter().rev().find(|&&lsn| lsn <= vcl).copied().unwrap_or(0);

        Ok(RecoveryData {
            durability: DurabilityState { vcl, vdl },
            page_index,
            lsn_offsets,
        })
    }

    fn scan_segment(
        &mut self,
        reader: &mut WalReader,
        segment_id: SegmentId,
        all_lsns: &mut std::collections::BTreeSet<Lsn>,
        cpls: &mut std::collections::BTreeSet<Lsn>,
        lsn_offsets: &mut HashMap<Lsn, LsnLocation>,
        page_index: &mut HashMap<PageId, Lsn>,
    ) -> Result<(), std::io::Error> {
        loop {
            let file_offset = reader.stream_position()?;
            match reader.read_entry()? {
                crate::reader::ReadResult::Entry(hdr, _data) => {
                    all_lsns.insert(hdr.lsn);
                    if hdr.is_mtr_end() {
                        cpls.insert(hdr.lsn);
                    }
                    lsn_offsets.insert(hdr.lsn, LsnLocation {
                        segment_id,
                        file_offset,
                    });
                    let entry = page_index.entry(hdr.page_id).or_insert(0);
                    if hdr.lsn > *entry {
                        *entry = hdr.lsn;
                    }
                    if segment_id == self.active_segment_id && self.active_first_lsn.is_none() {
                        self.active_first_lsn = Some(hdr.lsn);
                    }
                }
                _ => break,
            }
        }
        Ok(())
    }

    fn save_manifest(&self) -> Result<(), std::io::Error> {
        let manifest_path = self.base_dir.join("manifest.json");
        let tmp_path = self.base_dir.join("manifest.json.tmp");
        let content = serde_json::to_string_pretty(&self.manifest)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(&tmp_path, content)?;
        fs::rename(&tmp_path, &manifest_path)?;
        Ok(())
    }
}

/// Data returned from segment recovery.
pub struct RecoveryData {
    pub durability: DurabilityState,
    pub page_index: HashMap<PageId, Lsn>,
    pub lsn_offsets: HashMap<Lsn, LsnLocation>,
}

fn segment_filename(id: SegmentId) -> String {
    format!("wal_{:06}.seg", id)
}

fn compute_vcl(lsns: &std::collections::BTreeSet<Lsn>) -> Lsn {
    let mut expected = 1u64;
    for &lsn in lsns {
        if lsn != expected {
            return expected - 1;
        }
        expected += 1;
    }
    expected - 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_record(lsn: Lsn, page_id: PageId, prev_lsn: Lsn, is_end: bool) -> RedoRecord {
        RedoRecord {
            lsn,
            page_id,
            offset: 0,
            data: vec![lsn as u8; 10],
            prev_lsn,
            mtr_id: 1,
            is_mtr_end: is_end,
        }
    }

    #[test]
    fn test_segment_manager_basic() {
        let dir = TempDir::new().unwrap();
        let mut mgr = SegmentManager::open(dir.path(), 4096, Duration::ZERO).unwrap();

        let records = vec![
            make_record(1, 1, 0, false),
            make_record(2, 2, 0, true),
        ];

        let locs = mgr.append_batch(&records).unwrap();
        mgr.sync().unwrap();

        assert_eq!(locs.len(), 2);
        assert_eq!(locs[0].segment_id, locs[1].segment_id);
    }

    #[test]
    fn test_segment_rotation() {
        let dir = TempDir::new().unwrap();
        // Very small segment size to force rotation (each record ~51 bytes)
        let mut mgr = SegmentManager::open(dir.path(), 100, Duration::ZERO).unwrap();

        let r1 = make_record(1, 1, 0, true);
        let r2 = make_record(2, 2, 0, true);
        let r3 = make_record(3, 1, 1, true);

        let loc1 = mgr.append_batch(&[r1]).unwrap();
        let loc2 = mgr.append_batch(&[r2]).unwrap();
        let loc3 = mgr.append_batch(&[r3]).unwrap();
        mgr.sync().unwrap();

        // r1: active_bytes=0 → no rotation, writes to segment 1, active_bytes=51
        // r2: 51+51=102 > 100 → rotation. Writes to segment 2.
        // r3: 51+51=102 > 100 → rotation. Writes to segment 3.
        assert_eq!(loc1[0].segment_id + 1, loc2[0].segment_id);
        assert_eq!(loc2[0].segment_id + 1, loc3[0].segment_id);
    }

    #[test]
    fn test_segment_recovery() {
        let dir = TempDir::new().unwrap();

        {
            let mut mgr = SegmentManager::open(dir.path(), 100, Duration::ZERO).unwrap();
            let records: Vec<RedoRecord> = (1..=5).map(|i| {
                make_record(i, (i % 3) + 1, if i > 1 { i - 1 } else { 0 }, i == 5)
            }).collect();

            for r in &records {
                mgr.append_batch(&[r.clone()]).unwrap();
            }
            mgr.sync().unwrap();
        }

        // Reopen and recover
        let mut mgr = SegmentManager::open(dir.path(), 100, Duration::ZERO).unwrap();
        let data = mgr.recover().unwrap();

        assert_eq!(data.durability.vcl, 5);
        assert_eq!(data.durability.vdl, 5);
        assert_eq!(data.lsn_offsets.len(), 5);
        assert!(!data.page_index.is_empty());
    }

    #[test]
    fn test_cool_segments() {
        let dir = TempDir::new().unwrap();
        let mut mgr = SegmentManager::open(dir.path(), 60, Duration::ZERO).unwrap();

        for i in 1..=10u64 {
            let r = make_record(i, 1, if i > 1 { i - 1 } else { 0 }, true);
            mgr.append_batch(&[r]).unwrap();
        }
        mgr.sync().unwrap();

        let sealed_count = mgr.manifest.segments.iter().filter(|s| s.sealed).count();
        if sealed_count > 1 {
            let cooled = mgr.cool_segments(1).unwrap();
            assert!(!cooled.is_empty());

            let cold_files: Vec<_> = fs::read_dir(&mgr.cold_dir).unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(!cold_files.is_empty());
        }
    }

    #[test]
    fn test_segment_reader() {
        let dir = TempDir::new().unwrap();
        let mut mgr = SegmentManager::open(dir.path(), 4096, Duration::ZERO).unwrap();

        let records = vec![
            make_record(1, 1, 0, true),
            make_record(2, 2, 0, true),
        ];
        let locs = mgr.append_batch(&records).unwrap();
        mgr.sync().unwrap();

        // Read back from the active segment
        let (mut reader, tier) = mgr.open_segment_reader(locs[0].segment_id).unwrap();
        assert_eq!(tier, Tier::Hot);

        reader.seek_to(locs[0].file_offset).unwrap();
        match reader.read_entry().unwrap() {
            crate::reader::ReadResult::Entry(hdr, _data) => {
                assert_eq!(hdr.lsn, 1);
                assert_eq!(hdr.page_id, 1);
            }
            _ => panic!("expected entry"),
        }
    }
}
