use std::collections::{BTreeSet, HashMap};
use std::fs::OpenOptions;
use std::path::Path;

use mini_aurora_common::{DurabilityState, Lsn, PageId};

use crate::reader::{ReadResult, WalReader};

/// Result of WAL recovery: the durable state plus rebuilt indexes.
#[derive(Debug)]
pub struct RecoveryResult {
    /// Durability watermarks after recovery.
    pub durability: DurabilityState,
    /// Page → latest durable LSN index (only includes records ≤ VDL).
    pub page_index: HashMap<PageId, Lsn>,
    /// LSN → file offset mapping (only includes records ≤ VDL).
    pub lsn_offsets: HashMap<Lsn, u64>,
}

/// Perform WAL recovery:
/// 1. Scan forward, collecting all valid entries.
/// 2. Compute VCL (highest contiguous LSN).
/// 3. Compute VDL (highest CPL ≤ VCL).
/// 4. Truncate the WAL at VDL (discard incomplete MTRs).
/// 5. Rebuild page→LSN index from surviving records.
pub fn recover(wal_path: &Path) -> Result<RecoveryResult, std::io::Error> {
    let mut reader = WalReader::open(wal_path)?;

    // Phase 1: Scan all valid entries
    let mut all_lsns = BTreeSet::new();
    let mut cpls = BTreeSet::new(); // MTR completion points
    let mut entries: Vec<ScannedEntry> = Vec::new();

    reader.seek_start()?;
    loop {
        let file_offset = reader.stream_position()?;
        match reader.read_entry()? {
            ReadResult::Entry(hdr, _data) => {
                all_lsns.insert(hdr.lsn);
                if hdr.is_mtr_end() {
                    cpls.insert(hdr.lsn);
                }
                entries.push(ScannedEntry {
                    lsn: hdr.lsn,
                    page_id: hdr.page_id,
                    file_offset,
                    is_mtr_end: hdr.is_mtr_end(),
                });
            }
            ReadResult::Eof | ReadResult::Corrupted { .. } => break,
        }
    }

    // Phase 2: Compute VCL — highest LSN N where all 1..=N are present
    let vcl = compute_vcl(&all_lsns);

    // Phase 3: Compute VDL — highest CPL ≤ VCL
    let vdl = cpls
        .iter()
        .rev()
        .find(|&&lsn| lsn <= vcl)
        .copied()
        .unwrap_or(0);

    // Phase 4: Truncate WAL at VDL
    // Find the file offset just past the VDL entry
    let truncate_at = if vdl == 0 {
        0
    } else {
        find_end_of_entry(wal_path, vdl, &entries)?
    };

    let file = OpenOptions::new().write(true).open(wal_path)?;
    file.set_len(truncate_at)?;
    drop(file);

    // Phase 5: Rebuild indexes from surviving entries (LSN ≤ VDL)
    let mut page_index: HashMap<PageId, Lsn> = HashMap::new();
    let mut lsn_offsets: HashMap<Lsn, u64> = HashMap::new();

    for entry in &entries {
        if entry.lsn > vdl {
            break;
        }
        lsn_offsets.insert(entry.lsn, entry.file_offset);
        let latest = page_index.entry(entry.page_id).or_insert(0);
        if entry.lsn > *latest {
            *latest = entry.lsn;
        }
    }

    Ok(RecoveryResult {
        durability: DurabilityState { vcl, vdl },
        page_index,
        lsn_offsets,
    })
}

#[derive(Debug)]
struct ScannedEntry {
    lsn: Lsn,
    page_id: PageId,
    file_offset: u64,
    #[allow(dead_code)]
    is_mtr_end: bool,
}

/// Compute VCL: highest N such that all LSNs 1..=N are present.
fn compute_vcl(lsns: &BTreeSet<Lsn>) -> Lsn {
    let mut expected = 1u64;
    for &lsn in lsns {
        if lsn != expected {
            return expected - 1;
        }
        expected += 1;
    }
    expected - 1
}

/// Find the byte offset in the WAL file immediately after the entry with the given LSN.
fn find_end_of_entry(
    wal_path: &Path,
    target_lsn: Lsn,
    entries: &[ScannedEntry],
) -> Result<u64, std::io::Error> {
    // Find the entry with this LSN
    for (i, entry) in entries.iter().enumerate() {
        if entry.lsn == target_lsn {
            // The end is the start of the next entry, or we need to compute from the entry
            if i + 1 < entries.len() {
                return Ok(entries[i + 1].file_offset);
            } else {
                // Last entry — use file size (the reader stopped after this entry)
                let metadata = std::fs::metadata(wal_path)?;
                return Ok(metadata.len());
            }
        }
    }
    Ok(0)
}

// Helper methods on WalReader for recovery
impl WalReader {
    pub fn seek_start(&mut self) -> Result<(), std::io::Error> {
        use std::io::Seek;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn stream_position(&mut self) -> Result<u64, std::io::Error> {
        use std::io::Seek;
        self.file.stream_position()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::WalWriter;
    use mini_aurora_common::{RedoRecord, LOG_ENTRY_HEADER_SIZE};
    use tempfile::NamedTempFile;

    fn make_record(lsn: Lsn, page_id: PageId, prev_lsn: Lsn, mtr_id: u64, is_end: bool) -> RedoRecord {
        RedoRecord {
            lsn,
            page_id,
            offset: 0,
            data: vec![lsn as u8; 4],
            prev_lsn,
            mtr_id,
            is_mtr_end: is_end,
        }
    }

    #[test]
    fn test_clean_recovery() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Complete MTR: records 1,2,3 (3 is CPL)
        let records = vec![
            make_record(1, 1, 0, 1, false),
            make_record(2, 2, 0, 1, false),
            make_record(3, 1, 1, 1, true),
        ];

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let result = recover(&path).unwrap();
        assert_eq!(result.durability.vcl, 3);
        assert_eq!(result.durability.vdl, 3);
        assert_eq!(result.page_index[&1], 3);
        assert_eq!(result.page_index[&2], 2);
    }

    #[test]
    fn test_recovery_incomplete_mtr() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Complete MTR (1,2,3), then incomplete MTR (4,5 — no CPL)
        let records = vec![
            make_record(1, 1, 0, 1, false),
            make_record(2, 2, 0, 1, false),
            make_record(3, 1, 1, 1, true),  // CPL for MTR 1
            make_record(4, 3, 0, 2, false),
            make_record(5, 1, 3, 2, false), // no CPL — incomplete MTR
        ];

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let result = recover(&path).unwrap();
        // VCL = 5 (all present), VDL = 3 (last CPL ≤ VCL)
        assert_eq!(result.durability.vcl, 5);
        assert_eq!(result.durability.vdl, 3);

        // After truncation, only records 1-3 survive
        assert_eq!(result.lsn_offsets.len(), 3);
        assert!(!result.lsn_offsets.contains_key(&4));
        assert!(!result.lsn_offsets.contains_key(&5));
    }

    #[test]
    fn test_recovery_truncated_record() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let records = vec![
            make_record(1, 1, 0, 1, true),  // complete MTR
            make_record(2, 2, 0, 2, true),  // complete MTR
        ];

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Simulate crash: truncate mid-way through record 2
        let entry_size = LOG_ENTRY_HEADER_SIZE as u64 + 4; // data is 4 bytes
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(entry_size + 10).unwrap(); // 10 bytes into record 2
        drop(file);

        let result = recover(&path).unwrap();
        // Only record 1 is readable; VCL=1, VDL=1
        assert_eq!(result.durability.vcl, 1);
        assert_eq!(result.durability.vdl, 1);
        assert_eq!(result.lsn_offsets.len(), 1);
    }

    #[test]
    fn test_recovery_gap_in_lsns() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Simulate gap: LSNs 1, 2, 4 (missing 3)
        // In practice this shouldn't happen on single machine but tests VCL logic
        let records = vec![
            make_record(1, 1, 0, 1, true),
            make_record(2, 2, 0, 2, true),
        ];

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Write record with LSN=4 (skip 3)
        let gap_record = make_record(4, 1, 1, 3, true);
        let mut writer = WalWriter::open(&path).unwrap();
        writer.append(&gap_record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let result = recover(&path).unwrap();
        // VCL = 2 (gap at 3), VDL = 2 (highest CPL ≤ 2)
        assert_eq!(result.durability.vcl, 2);
        assert_eq!(result.durability.vdl, 2);
    }

    #[test]
    fn test_recovery_empty_wal() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        // Create empty file
        std::fs::write(&path, b"").unwrap();

        let result = recover(&path).unwrap();
        assert_eq!(result.durability.vcl, 0);
        assert_eq!(result.durability.vdl, 0);
        assert!(result.page_index.is_empty());
    }
}
