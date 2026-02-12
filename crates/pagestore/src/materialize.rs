use mini_aurora_common::{empty_page, Page, PageId, RedoRecord, StorageError, PAGE_SIZE};

/// Materialize a page by replaying a chain of redo records onto a zeroed page.
///
/// Records must be in LSN order (oldest first). Each record writes its `data`
/// at the specified `offset` within the page.
pub fn materialize_page(page_id: PageId, records: &[RedoRecord]) -> Result<Page, StorageError> {
    let mut page = empty_page();

    for record in records {
        debug_assert_eq!(record.page_id, page_id);
        apply_redo(&mut page, record)?;
    }

    Ok(page)
}

/// Apply a single redo record to a page image.
fn apply_redo(page: &mut Page, record: &RedoRecord) -> Result<(), StorageError> {
    let start = record.offset as usize;
    let end = start + record.data.len();

    if end > PAGE_SIZE {
        return Err(StorageError::PageOverflow {
            offset: record.offset,
            len: record.data.len(),
        });
    }

    page[start..end].copy_from_slice(&record.data);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_redo(lsn: u64, page_id: PageId, offset: u16, data: Vec<u8>, prev_lsn: u64) -> RedoRecord {
        RedoRecord {
            lsn,
            page_id,
            offset,
            data,
            prev_lsn,
            mtr_id: 1,
            is_mtr_end: true,
        }
    }

    #[test]
    fn test_single_record() {
        let records = vec![make_redo(1, 1, 0, vec![0xAA, 0xBB, 0xCC], 0)];
        let page = materialize_page(1, &records).unwrap();
        assert_eq!(page[0], 0xAA);
        assert_eq!(page[1], 0xBB);
        assert_eq!(page[2], 0xCC);
        assert_eq!(page[3], 0x00); // rest is zero
    }

    #[test]
    fn test_multiple_records_compose() {
        let records = vec![
            make_redo(1, 1, 0, vec![0x11, 0x22], 0),
            make_redo(2, 1, 4, vec![0x33, 0x44], 1),
            make_redo(3, 1, 0, vec![0xFF], 2), // overwrites first byte
        ];
        let page = materialize_page(1, &records).unwrap();
        assert_eq!(page[0], 0xFF); // overwritten
        assert_eq!(page[1], 0x22); // from first record
        assert_eq!(page[4], 0x33);
        assert_eq!(page[5], 0x44);
    }

    #[test]
    fn test_write_at_end_of_page() {
        let records = vec![make_redo(1, 1, (PAGE_SIZE - 2) as u16, vec![0xEE, 0xFF], 0)];
        let page = materialize_page(1, &records).unwrap();
        assert_eq!(page[PAGE_SIZE - 2], 0xEE);
        assert_eq!(page[PAGE_SIZE - 1], 0xFF);
    }

    #[test]
    fn test_overflow_rejected() {
        let records = vec![make_redo(1, 1, (PAGE_SIZE - 1) as u16, vec![0xAA, 0xBB], 0)];
        let result = materialize_page(1, &records);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_chain_returns_zeroed_page() {
        let page = materialize_page(1, &[]).unwrap();
        assert_eq!(page, empty_page());
    }
}
