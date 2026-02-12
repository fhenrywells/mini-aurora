use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crc32fast::Hasher;
use mini_aurora_common::{LogEntryHeader, Lsn, PageId, RedoRecord, LOG_ENTRY_HEADER_SIZE};

/// Reads and iterates over WAL entries on disk.
pub struct WalReader {
    pub(crate) file: File,
}

/// Outcome of trying to read one log entry.
#[derive(Debug)]
pub enum ReadResult {
    /// Successfully decoded an entry.
    Entry(LogEntryHeader, Vec<u8>),
    /// Hit EOF or incomplete entry (truncated).
    Eof,
    /// CRC mismatch — corruption detected.
    Corrupted { file_offset: u64 },
}

impl WalReader {
    pub fn open(path: &Path) -> Result<Self, std::io::Error> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Seek to a specific file offset.
    pub fn seek_to(&mut self, offset: u64) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }

    /// Read one entry starting at the file's current position.
    pub fn read_entry(&mut self) -> Result<ReadResult, std::io::Error> {
        let file_offset = self.file.stream_position()?;

        // Read header bytes
        let mut hdr_buf = [0u8; LOG_ENTRY_HEADER_SIZE];
        match self.file.read_exact(&mut hdr_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(ReadResult::Eof);
            }
            Err(e) => return Err(e),
        }

        let header = decode_header(&hdr_buf);

        // Read data payload
        let mut data = vec![0u8; header.data_len as usize];
        match self.file.read_exact(&mut data) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(ReadResult::Eof);
            }
            Err(e) => return Err(e),
        }

        // Verify CRC
        let expected_crc = compute_crc(&hdr_buf[..LOG_ENTRY_HEADER_SIZE - 4], &data);
        if header.crc32 != expected_crc {
            return Ok(ReadResult::Corrupted { file_offset });
        }

        Ok(ReadResult::Entry(header, data))
    }

    /// Scan the entire WAL forward, returning all valid entries.
    pub fn scan_all(&mut self) -> Result<Vec<(LogEntryHeader, Vec<u8>)>, std::io::Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();
        loop {
            match self.read_entry()? {
                ReadResult::Entry(hdr, data) => entries.push((hdr, data)),
                ReadResult::Eof | ReadResult::Corrupted { .. } => break,
            }
        }
        Ok(entries)
    }

    /// Build a page→latest-LSN index by scanning the entire WAL.
    /// Also returns a mapping from LSN → file offset for chain-walking.
    pub fn build_index(
        &mut self,
    ) -> Result<(HashMap<PageId, Lsn>, HashMap<Lsn, u64>), std::io::Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut page_latest: HashMap<PageId, Lsn> = HashMap::new();
        let mut lsn_offset: HashMap<Lsn, u64> = HashMap::new();

        loop {
            let offset = self.file.stream_position()?;
            match self.read_entry()? {
                ReadResult::Entry(hdr, _data) => {
                    lsn_offset.insert(hdr.lsn, offset);
                    let entry = page_latest.entry(hdr.page_id).or_insert(0);
                    if hdr.lsn > *entry {
                        *entry = hdr.lsn;
                    }
                }
                ReadResult::Eof | ReadResult::Corrupted { .. } => break,
            }
        }
        Ok((page_latest, lsn_offset))
    }

    /// Collect all redo records for a given page up to (and including) `target_lsn`,
    /// by walking the `prev_lsn` chain backwards, then reversing.
    ///
    /// Requires the LSN→file-offset index for seeking.
    pub fn collect_page_chain(
        &mut self,
        page_id: PageId,
        start_lsn: Lsn,
        target_lsn: Lsn,
        lsn_offset: &HashMap<Lsn, u64>,
    ) -> Result<Vec<RedoRecord>, std::io::Error> {
        let mut chain = Vec::new();
        let mut current_lsn = start_lsn;

        // Walk backwards through the chain, but only include records ≤ target_lsn
        while current_lsn != 0 {
            if current_lsn > target_lsn {
                // Need to find the right starting point — scan forward to find
                // the latest LSN for this page that is ≤ target_lsn.
                // This means start_lsn was too high; we need to walk from there.
                // Actually, we should still walk backward to find records ≤ target_lsn.
                let offset = match lsn_offset.get(&current_lsn) {
                    Some(&off) => off,
                    None => break,
                };
                self.file.seek(SeekFrom::Start(offset))?;
                match self.read_entry()? {
                    ReadResult::Entry(hdr, _data) => {
                        current_lsn = hdr.prev_lsn;
                        continue;
                    }
                    _ => break,
                }
            }

            let offset = match lsn_offset.get(&current_lsn) {
                Some(&off) => off,
                None => break,
            };
            self.file.seek(SeekFrom::Start(offset))?;
            match self.read_entry()? {
                ReadResult::Entry(hdr, data) => {
                    debug_assert_eq!(hdr.page_id, page_id);
                    let record = header_to_record(&hdr, data);
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
}

/// Decode a header from raw bytes.
pub fn decode_header(buf: &[u8; LOG_ENTRY_HEADER_SIZE]) -> LogEntryHeader {
    LogEntryHeader {
        lsn: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        page_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        offset: u16::from_le_bytes(buf[16..18].try_into().unwrap()),
        data_len: u16::from_le_bytes(buf[18..20].try_into().unwrap()),
        prev_lsn: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        mtr_id: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        flags: buf[36],
        crc32: u32::from_le_bytes(buf[37..41].try_into().unwrap()),
    }
}

/// Convert a header + data back into a RedoRecord.
pub fn header_to_record(hdr: &LogEntryHeader, data: Vec<u8>) -> RedoRecord {
    RedoRecord {
        lsn: hdr.lsn,
        page_id: hdr.page_id,
        offset: hdr.offset,
        data,
        prev_lsn: hdr.prev_lsn,
        mtr_id: hdr.mtr_id,
        is_mtr_end: hdr.is_mtr_end(),
    }
}

fn compute_crc(header_without_crc: &[u8], data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header_without_crc);
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::WalWriter;
    use tempfile::NamedTempFile;

    #[test]
    fn test_roundtrip_single() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let record = RedoRecord {
            lsn: 1,
            page_id: 42,
            offset: 100,
            data: vec![0xDE, 0xAD],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        };

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append(&record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let mut reader = WalReader::open(&path).unwrap();
        let entries = reader.scan_all().unwrap();
        assert_eq!(entries.len(), 1);

        let (hdr, data) = &entries[0];
        assert_eq!(hdr.lsn, 1);
        assert_eq!(hdr.page_id, 42);
        assert_eq!(hdr.offset, 100);
        assert_eq!(hdr.data_len, 2);
        assert_eq!(data, &[0xDE, 0xAD]);
        assert!(hdr.is_mtr_end());
    }

    #[test]
    fn test_roundtrip_batch() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let records: Vec<RedoRecord> = (1..=3)
            .map(|i| RedoRecord {
                lsn: i,
                page_id: 10,
                offset: (i as u16) * 8,
                data: vec![i as u8; 6],
                prev_lsn: if i == 1 { 0 } else { i - 1 },
                mtr_id: 1,
                is_mtr_end: i == 3,
            })
            .collect();

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let mut reader = WalReader::open(&path).unwrap();
        let entries = reader.scan_all().unwrap();
        assert_eq!(entries.len(), 3);

        for (i, (hdr, data)) in entries.iter().enumerate() {
            let lsn = (i + 1) as u64;
            assert_eq!(hdr.lsn, lsn);
            assert_eq!(hdr.page_id, 10);
            assert_eq!(data.len(), 6);
        }
    }

    #[test]
    fn test_build_index() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let records = vec![
            RedoRecord { lsn: 1, page_id: 1, offset: 0, data: vec![1], prev_lsn: 0, mtr_id: 1, is_mtr_end: false },
            RedoRecord { lsn: 2, page_id: 2, offset: 0, data: vec![2], prev_lsn: 0, mtr_id: 1, is_mtr_end: false },
            RedoRecord { lsn: 3, page_id: 1, offset: 4, data: vec![3], prev_lsn: 1, mtr_id: 1, is_mtr_end: true },
        ];

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let mut reader = WalReader::open(&path).unwrap();
        let (page_latest, lsn_offset) = reader.build_index().unwrap();

        assert_eq!(page_latest[&1], 3);
        assert_eq!(page_latest[&2], 2);
        assert_eq!(lsn_offset.len(), 3);
    }

    #[test]
    fn test_collect_page_chain() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Page 1: LSN 1 → LSN 3 → LSN 5
        // Page 2: LSN 2 → LSN 4
        let records = vec![
            RedoRecord { lsn: 1, page_id: 1, offset: 0, data: vec![0xA], prev_lsn: 0, mtr_id: 1, is_mtr_end: true },
            RedoRecord { lsn: 2, page_id: 2, offset: 0, data: vec![0xB], prev_lsn: 0, mtr_id: 2, is_mtr_end: true },
            RedoRecord { lsn: 3, page_id: 1, offset: 1, data: vec![0xC], prev_lsn: 1, mtr_id: 3, is_mtr_end: true },
            RedoRecord { lsn: 4, page_id: 2, offset: 1, data: vec![0xD], prev_lsn: 2, mtr_id: 4, is_mtr_end: true },
            RedoRecord { lsn: 5, page_id: 1, offset: 2, data: vec![0xE], prev_lsn: 3, mtr_id: 5, is_mtr_end: true },
        ];

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let mut reader = WalReader::open(&path).unwrap();
        let (_page_latest, lsn_offset) = reader.build_index().unwrap();

        // Collect full chain for page 1 up to LSN 5
        let chain = reader.collect_page_chain(1, 5, 5, &lsn_offset).unwrap();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].lsn, 1);
        assert_eq!(chain[1].lsn, 3);
        assert_eq!(chain[2].lsn, 5);

        // Collect chain for page 1 up to LSN 3 (should skip LSN 5)
        let chain = reader.collect_page_chain(1, 5, 3, &lsn_offset).unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].lsn, 1);
        assert_eq!(chain[1].lsn, 3);
    }

    #[test]
    fn test_detect_truncated_entry() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let record = RedoRecord {
            lsn: 1,
            page_id: 1,
            offset: 0,
            data: vec![0xAA; 100],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        };

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append(&record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Truncate the file mid-data
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(LOG_ENTRY_HEADER_SIZE as u64 + 10).unwrap(); // only 10 of 100 data bytes
        drop(file);

        let mut reader = WalReader::open(&path).unwrap();
        let entries = reader.scan_all().unwrap();
        assert_eq!(entries.len(), 0); // truncated entry should be skipped
    }
}
