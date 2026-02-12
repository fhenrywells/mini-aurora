use std::sync::Arc;
use tokio::sync::Mutex;

use mini_aurora_common::{Lsn, Page, PageId, StorageApi, StorageError, PAGE_SIZE};

use crate::buffer_pool::BufferPool;
use crate::transaction::MiniTransaction;

/// Compute engine — the "SQL layer" in Aurora's architecture.
///
/// Generates redo records and sends them to storage. Maintains a local buffer
/// pool for read caching. In Phase 1 this talks to storage in-process; in
/// Phase 2+ it goes over RPC.
pub struct ComputeEngine {
    storage: Arc<dyn StorageApi>,
    inner: Mutex<ComputeInner>,
}

struct ComputeInner {
    buffer_pool: BufferPool,
    next_mtr_id: u64,
    /// Current read point (tracks VDL for consistent reads).
    read_point: Lsn,
}

impl ComputeEngine {
    pub fn new(storage: Arc<dyn StorageApi>, buffer_pool_capacity: usize) -> Self {
        Self {
            storage,
            inner: Mutex::new(ComputeInner {
                buffer_pool: BufferPool::new(buffer_pool_capacity),
                next_mtr_id: 1,
                read_point: 0,
            }),
        }
    }

    /// Write bytes to a page at a given offset. This is a single-record MTR.
    pub async fn put(
        &self,
        page_id: PageId,
        offset: u16,
        data: Vec<u8>,
    ) -> Result<Lsn, StorageError> {
        if offset as usize + data.len() > PAGE_SIZE {
            return Err(StorageError::PageOverflow {
                offset,
                len: data.len(),
            });
        }

        let mtr_id = {
            let mut inner = self.inner.lock().await;
            let id = inner.next_mtr_id;
            inner.next_mtr_id += 1;
            id
        };

        let mut mtr = MiniTransaction::new(mtr_id);
        mtr.write(page_id, offset, data);
        let records = mtr.finish().unwrap();

        let vdl = self.storage.append_redo(records).await?;

        // Update read point and invalidate cache for this page
        {
            let mut inner = self.inner.lock().await;
            inner.read_point = vdl;
            inner.buffer_pool.invalidate(page_id);
        }

        Ok(vdl)
    }

    /// Execute a multi-record mini-transaction.
    /// `writes` is a list of (page_id, offset, data) tuples.
    pub async fn put_multi(
        &self,
        writes: Vec<(PageId, u16, Vec<u8>)>,
    ) -> Result<Lsn, StorageError> {
        for &(_, offset, ref data) in &writes {
            if offset as usize + data.len() > PAGE_SIZE {
                return Err(StorageError::PageOverflow {
                    offset,
                    len: data.len(),
                });
            }
        }

        let mtr_id = {
            let mut inner = self.inner.lock().await;
            let id = inner.next_mtr_id;
            inner.next_mtr_id += 1;
            id
        };

        let mut mtr = MiniTransaction::new(mtr_id);
        for (page_id, offset, data) in &writes {
            mtr.write(*page_id, *offset, data.clone());
        }

        let records = match mtr.finish() {
            Some(r) => r,
            None => return Ok(self.inner.lock().await.read_point),
        };

        let vdl = self.storage.append_redo(records).await?;

        // Update read point and invalidate caches
        {
            let mut inner = self.inner.lock().await;
            inner.read_point = vdl;
            for (page_id, _, _) in &writes {
                inner.buffer_pool.invalidate(*page_id);
            }
        }

        Ok(vdl)
    }

    /// Read a page at the current read point.
    pub async fn get(&self, page_id: PageId) -> Result<Page, StorageError> {
        let read_point = self.inner.lock().await.read_point;
        self.get_at(page_id, read_point).await
    }

    /// Read a page at a specific LSN.
    pub async fn get_at(&self, page_id: PageId, lsn: Lsn) -> Result<Page, StorageError> {
        // Check buffer pool first
        {
            let mut inner = self.inner.lock().await;
            if let Some(page) = inner.buffer_pool.get(page_id, lsn) {
                return Ok(*page);
            }
        }

        // Fetch from storage
        let page = self.storage.get_page(page_id, lsn).await?;

        // Cache in buffer pool
        {
            let mut inner = self.inner.lock().await;
            inner.buffer_pool.insert(page_id, lsn, page);
        }

        Ok(page)
    }

    /// Get the current read point (VDL as seen by this compute node).
    pub async fn read_point(&self) -> Lsn {
        self.inner.lock().await.read_point
    }

    /// Refresh the read point from storage's durability state.
    pub async fn refresh_read_point(&self) -> Result<Lsn, StorageError> {
        let state = self.storage.get_durability_state().await?;
        let mut inner = self.inner.lock().await;
        inner.read_point = state.vdl;
        Ok(state.vdl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mini_aurora_common::{DurabilityState, RedoRecord};
    use std::sync::Mutex as StdMutex;

    /// Mock storage for compute engine tests.
    struct MockStorage {
        inner: StdMutex<MockInner>,
    }

    struct MockInner {
        records: Vec<RedoRecord>,
        next_lsn: Lsn,
        vdl: Lsn,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                inner: StdMutex::new(MockInner {
                    records: Vec::new(),
                    next_lsn: 1,
                    vdl: 0,
                }),
            }
        }
    }

    #[async_trait::async_trait]
    impl StorageApi for MockStorage {
        async fn append_redo(&self, mut records: Vec<RedoRecord>) -> Result<Lsn, StorageError> {
            let mut inner = self.inner.lock().unwrap();
            for record in &mut records {
                record.lsn = inner.next_lsn;
                inner.next_lsn += 1;
            }
            if let Some(cpl) = records.iter().rev().find(|r| r.is_mtr_end) {
                inner.vdl = cpl.lsn;
            }
            inner.records.extend(records);
            Ok(inner.vdl)
        }

        async fn get_page(&self, page_id: PageId, read_point: Lsn) -> Result<Page, StorageError> {
            let inner = self.inner.lock().unwrap();
            let mut page = [0u8; PAGE_SIZE];
            for record in &inner.records {
                if record.page_id == page_id && record.lsn <= read_point {
                    let start = record.offset as usize;
                    let end = start + record.data.len();
                    page[start..end].copy_from_slice(&record.data);
                }
            }
            Ok(page)
        }

        async fn get_durability_state(&self) -> Result<DurabilityState, StorageError> {
            let inner = self.inner.lock().unwrap();
            Ok(DurabilityState {
                vcl: inner.vdl,
                vdl: inner.vdl,
            })
        }
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let storage = Arc::new(MockStorage::new());
        let engine = ComputeEngine::new(storage, 100);

        engine.put(1, 0, vec![0xAA, 0xBB]).await.unwrap();
        let page = engine.get(1).await.unwrap();
        assert_eq!(&page[0..2], &[0xAA, 0xBB]);
    }

    #[tokio::test]
    async fn test_put_multi() {
        let storage = Arc::new(MockStorage::new());
        let engine = ComputeEngine::new(storage, 100);

        engine
            .put_multi(vec![
                (1, 0, vec![0x11]),
                (2, 0, vec![0x22]),
                (3, 0, vec![0x33]),
            ])
            .await
            .unwrap();

        let p1 = engine.get(1).await.unwrap();
        let p2 = engine.get(2).await.unwrap();
        let p3 = engine.get(3).await.unwrap();
        assert_eq!(p1[0], 0x11);
        assert_eq!(p2[0], 0x22);
        assert_eq!(p3[0], 0x33);
    }

    #[tokio::test]
    async fn test_read_point_advances() {
        let storage = Arc::new(MockStorage::new());
        let engine = ComputeEngine::new(storage, 100);

        assert_eq!(engine.read_point().await, 0);

        engine.put(1, 0, vec![0x01]).await.unwrap();
        assert_eq!(engine.read_point().await, 1);

        engine.put(2, 0, vec![0x02]).await.unwrap();
        assert_eq!(engine.read_point().await, 2);
    }

    #[tokio::test]
    async fn test_overflow_rejected() {
        let storage = Arc::new(MockStorage::new());
        let engine = ComputeEngine::new(storage, 100);

        let result = engine.put(1, PAGE_SIZE as u16 - 1, vec![0; 2]).await;
        assert!(result.is_err());
    }
}
