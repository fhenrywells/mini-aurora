use mini_aurora_common::{PageId, RedoRecord};

/// Builder for a mini-transaction (MTR).
///
/// Collects redo records and marks the last one as the CPL (consistency point).
pub struct MiniTransaction {
    mtr_id: u64,
    records: Vec<RedoRecord>,
}

impl MiniTransaction {
    pub fn new(mtr_id: u64) -> Self {
        Self {
            mtr_id,
            records: Vec::new(),
        }
    }

    /// Add a page write to this mini-transaction.
    /// LSN and prev_lsn will be assigned by the storage engine.
    pub fn write(&mut self, page_id: PageId, offset: u16, data: Vec<u8>) {
        self.records.push(RedoRecord {
            lsn: 0,     // assigned by storage
            page_id,
            offset,
            data,
            prev_lsn: 0, // assigned by storage
            mtr_id: self.mtr_id,
            is_mtr_end: false,
        });
    }

    /// Finalize the MTR: marks the last record as the CPL and returns all records.
    /// Returns `None` if the MTR is empty.
    pub fn finish(mut self) -> Option<Vec<RedoRecord>> {
        if let Some(last) = self.records.last_mut() {
            last.is_mtr_end = true;
            Some(self.records)
        } else {
            None
        }
    }

    pub fn mtr_id(&self) -> u64 {
        self.mtr_id
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_mtr() {
        let mtr = MiniTransaction::new(1);
        assert!(mtr.finish().is_none());
    }

    #[test]
    fn test_single_write() {
        let mut mtr = MiniTransaction::new(1);
        mtr.write(42, 0, vec![0xAA]);
        let records = mtr.finish().unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].is_mtr_end);
        assert_eq!(records[0].mtr_id, 1);
    }

    #[test]
    fn test_multi_write_mtr() {
        let mut mtr = MiniTransaction::new(7);
        mtr.write(1, 0, vec![0x01]);
        mtr.write(2, 0, vec![0x02]);
        mtr.write(3, 0, vec![0x03]);

        let records = mtr.finish().unwrap();
        assert_eq!(records.len(), 3);
        assert!(!records[0].is_mtr_end);
        assert!(!records[1].is_mtr_end);
        assert!(records[2].is_mtr_end); // only last is CPL
        for r in &records {
            assert_eq!(r.mtr_id, 7);
        }
    }
}
