use std::sync::{Arc, Mutex};

use mini_aurora_common::{Lsn, Page, PageId, StorageApi, StorageError, PAGE_SIZE};
use mini_aurora_compute::buffer_pool::BufferPool;
use mini_aurora_compute::transaction::MiniTransaction;
use tokio::sync::Mutex as TokioMutex;

use super::engine::VizStorageEngine;
use super::events::VizEvent;
use super::renderer::VizRenderer;

/// Compute engine with visualization events emitted between each internal step.
///
/// Mirrors `ComputeEngine` from `crates/compute/src/engine.rs` but uses
/// `VizStorageEngine` and emits events through a shared renderer.
pub struct VizComputeEngine {
    storage: Arc<VizStorageEngine>,
    inner: TokioMutex<ComputeInner>,
    renderer: Arc<Mutex<VizRenderer>>,
}

struct ComputeInner {
    buffer_pool: BufferPool,
    next_mtr_id: u64,
    read_point: Lsn,
}

impl VizComputeEngine {
    pub fn new(
        storage: Arc<VizStorageEngine>,
        buffer_pool_capacity: usize,
        renderer: Arc<Mutex<VizRenderer>>,
    ) -> Self {
        Self {
            storage,
            inner: TokioMutex::new(ComputeInner {
                buffer_pool: BufferPool::new(buffer_pool_capacity),
                next_mtr_id: 1,
                read_point: 0,
            }),
            renderer,
        }
    }

    fn emit(&self, event: &VizEvent) {
        self.renderer.lock().unwrap().render(event);
    }

    fn render_op_header(&self, op: &str) {
        self.renderer.lock().unwrap().render_operation_header(op);
    }

    fn reset_steps(&self) {
        self.renderer.lock().unwrap().reset_steps(None);
    }

    /// Write bytes to a page at a given offset. Single-record MTR.
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

        self.render_op_header(&format!(
            "PUT pg{page_id} offset={offset} {:?}",
            String::from_utf8_lossy(&data)
        ));
        self.reset_steps();

        let mtr_id = {
            let mut inner = self.inner.lock().await;
            let id = inner.next_mtr_id;
            inner.next_mtr_id += 1;
            id
        };

        // Step: Create MTR
        let mut mtr = MiniTransaction::new(mtr_id);
        mtr.write(page_id, offset, data);
        let records = mtr.finish().unwrap();

        self.emit(&VizEvent::MtrCreated {
            mtr_id,
            num_records: records.len(),
        });

        // StorageEngine handles its own event emissions
        let vdl = self.storage.append_redo(records).await?;

        // Step: Invalidate buffer pool + update read point
        let old_read_point = {
            let mut inner = self.inner.lock().await;
            let old = inner.read_point;
            inner.read_point = vdl;
            inner.buffer_pool.invalidate(page_id);
            old
        };

        self.emit(&VizEvent::BufferPoolInvalidate { page_id });
        self.emit(&VizEvent::UpdateReadPoint {
            old: old_read_point,
            new: vdl,
        });

        // Show final state diagram
        self.emit_state_snapshot().await;

        Ok(vdl)
    }

    /// Execute a multi-record mini-transaction.
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

        let pages_str: Vec<String> = writes.iter().map(|(pid, _, _)| format!("pg{pid}")).collect();
        self.render_op_header(&format!("PUT MULTI [{}]", pages_str.join(", ")));
        self.reset_steps();

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

        self.emit(&VizEvent::MtrCreated {
            mtr_id,
            num_records: records.len(),
        });

        let vdl = self.storage.append_redo(records).await?;

        let old_read_point = {
            let mut inner = self.inner.lock().await;
            let old = inner.read_point;
            inner.read_point = vdl;
            for (page_id, _, _) in &writes {
                inner.buffer_pool.invalidate(*page_id);
            }
            old
        };

        for (page_id, _, _) in &writes {
            self.emit(&VizEvent::BufferPoolInvalidate { page_id: *page_id });
        }

        self.emit(&VizEvent::UpdateReadPoint {
            old: old_read_point,
            new: vdl,
        });

        self.emit_state_snapshot().await;

        Ok(vdl)
    }

    /// Read a page at the current read point.
    pub async fn get(&self, page_id: PageId) -> Result<Page, StorageError> {
        let read_point = self.inner.lock().await.read_point;

        self.render_op_header(&format!("GET pg{page_id} @L{read_point}"));
        self.reset_steps();

        // Step: Buffer pool lookup
        {
            let mut inner = self.inner.lock().await;
            if let Some(page) = inner.buffer_pool.get(page_id, read_point) {
                self.emit(&VizEvent::BufferPoolLookup {
                    page_id,
                    read_point,
                    hit: true,
                });
                return Ok(*page);
            }
        }

        self.emit(&VizEvent::BufferPoolLookup {
            page_id,
            read_point,
            hit: false,
        });

        // Fetch from storage (VizStorageEngine emits its own events)
        let page = self.storage.get_page(page_id, read_point).await?;

        // Step: Buffer pool insert
        {
            let mut inner = self.inner.lock().await;
            inner.buffer_pool.insert(page_id, read_point, page);
        }

        self.emit(&VizEvent::BufferPoolInsert { page_id, read_point });

        self.emit_state_snapshot().await;

        Ok(page)
    }

    pub async fn read_point(&self) -> Lsn {
        self.inner.lock().await.read_point
    }

    pub async fn refresh_read_point(&self) -> Result<Lsn, StorageError> {
        let state = self.storage.get_durability_state().await?;
        let mut inner = self.inner.lock().await;
        inner.read_point = state.vdl;
        Ok(state.vdl)
    }

    async fn emit_state_snapshot(&self) {
        let inner = self.inner.lock().await;
        self.storage.emit_state_snapshot(
            inner.read_point,
            inner.next_mtr_id,
            Vec::new(), // BufferPool doesn't expose keys
        );
    }
}
