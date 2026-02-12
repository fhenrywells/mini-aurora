use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crc32fast::Hasher;
use mini_aurora_common::{RedoRecord, LOG_ENTRY_HEADER_SIZE};

/// Append-only WAL writer. Each entry is a fixed-size header followed by
/// variable-length data, protected by a CRC32 checksum.
pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    /// Open (or create) a WAL file for appending.
    pub fn open(path: &Path) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Append a single redo record to the WAL and flush to disk.
    pub fn append(&mut self, record: &RedoRecord) -> Result<(), std::io::Error> {
        let header_bytes = encode_header(record);
        let crc = compute_crc(&header_bytes[..LOG_ENTRY_HEADER_SIZE - 4], &record.data);

        // Write header (without CRC) + CRC + data
        self.writer
            .write_all(&header_bytes[..LOG_ENTRY_HEADER_SIZE - 4])?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&record.data)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Append a batch of redo records, flushing once at the end.
    pub fn append_batch(&mut self, records: &[RedoRecord]) -> Result<(), std::io::Error> {
        for record in records {
            let header_bytes = encode_header(record);
            let crc = compute_crc(&header_bytes[..LOG_ENTRY_HEADER_SIZE - 4], &record.data);
            self.writer
                .write_all(&header_bytes[..LOG_ENTRY_HEADER_SIZE - 4])?;
            self.writer.write_all(&crc.to_le_bytes())?;
            self.writer.write_all(&record.data)?;
        }
        self.writer.flush()?;
        Ok(())
    }

    /// Fsync the underlying file to ensure durability.
    pub fn sync(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }
}

/// Encode a redo record into the on-disk header bytes (LOG_ENTRY_HEADER_SIZE).
/// The last 4 bytes (CRC) are zeroed — caller fills them in.
fn encode_header(record: &RedoRecord) -> [u8; LOG_ENTRY_HEADER_SIZE] {
    let mut buf = [0u8; LOG_ENTRY_HEADER_SIZE];
    let data_len = record.data.len() as u16;
    let flags: u8 = if record.is_mtr_end { 1 } else { 0 };

    buf[0..8].copy_from_slice(&record.lsn.to_le_bytes());
    buf[8..16].copy_from_slice(&record.page_id.to_le_bytes());
    buf[16..18].copy_from_slice(&record.offset.to_le_bytes());
    buf[18..20].copy_from_slice(&data_len.to_le_bytes());
    buf[20..28].copy_from_slice(&record.prev_lsn.to_le_bytes());
    buf[28..36].copy_from_slice(&record.mtr_id.to_le_bytes());
    buf[36] = flags;
    // bytes 37..41 = CRC32 (left as zero; caller writes separately)
    buf
}

/// Compute CRC32 over the header bytes (excluding the CRC field) and the data.
fn compute_crc(header_without_crc: &[u8], data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header_without_crc);
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_single_record() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let record = RedoRecord {
            lsn: 1,
            page_id: 42,
            offset: 100,
            data: vec![0xAB, 0xCD],
            prev_lsn: 0,
            mtr_id: 1,
            is_mtr_end: true,
        };

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append(&record).unwrap();
        writer.sync().unwrap();

        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(
            metadata.len(),
            (LOG_ENTRY_HEADER_SIZE + record.data.len()) as u64
        );
    }

    #[test]
    fn test_write_batch() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let records: Vec<RedoRecord> = (1..=5)
            .map(|i| RedoRecord {
                lsn: i,
                page_id: 1,
                offset: (i as u16) * 10,
                data: vec![i as u8; 4],
                prev_lsn: if i == 1 { 0 } else { i - 1 },
                mtr_id: 1,
                is_mtr_end: i == 5,
            })
            .collect();

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append_batch(&records).unwrap();
        writer.sync().unwrap();

        let expected_size: u64 = records
            .iter()
            .map(|r| (LOG_ENTRY_HEADER_SIZE + r.data.len()) as u64)
            .sum();
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), expected_size);
    }
}
