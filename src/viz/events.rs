use std::collections::HashMap;
use std::time::Duration;

use mini_aurora_common::{Lsn, PageId};

/// Every discrete internal operation that the visualization can display.
#[derive(Debug, Clone, serde::Serialize)]
pub enum VizEvent {
    // ── PUT path ──────────────────────────────────────────────────────

    /// A new mini-transaction was created.
    MtrCreated { mtr_id: u64, num_records: usize },

    /// LSNs assigned to the records in this batch.
    AssignLsns { first_lsn: Lsn, last_lsn: Lsn },

    /// A record's prev_lsn was linked to the chain for its page.
    LinkPrevLsn { lsn: Lsn, page_id: PageId, prev_lsn: Lsn },

    /// Records appended to the WAL file.
    WalAppend { first_lsn: Lsn, last_lsn: Lsn, offset: u64, bytes: u64 },

    /// WAL fsync completed.
    WalSync,

    /// Page index updated: page_id now points to latest_lsn.
    UpdatePageIndex { page_id: PageId, latest_lsn: Lsn },

    /// LSN→offset mapping recorded.
    UpdateLsnOffset { lsn: Lsn, file_offset: u64 },

    /// VCL advanced.
    AdvanceVcl { old: Lsn, new: Lsn },

    /// VDL advanced (new consistency point).
    AdvanceVdl { old: Lsn, new: Lsn },

    /// Compute read_point updated after a write.
    UpdateReadPoint { old: Lsn, new: Lsn },

    /// Buffer pool entry invalidated after writing to a page.
    BufferPoolInvalidate { page_id: PageId },

    // ── GET path ──────────────────────────────────────────────────────

    /// Buffer pool lookup on the compute side.
    BufferPoolLookup { page_id: PageId, read_point: Lsn, hit: bool },

    /// Storage-side page cache lookup.
    PageCacheLookup { page_id: PageId, read_point: Lsn, hit: bool },

    /// Looked up the page index to find the latest LSN for a page.
    PageIndexLookup { page_id: PageId, latest_lsn: Option<Lsn> },

    /// One step of the prev_lsn chain walk (backwards through WAL).
    ChainWalkStep { page_id: PageId, lsn: Lsn, prev_lsn: Lsn, skipped: bool },

    /// Chain collection complete.
    ChainCollected { page_id: PageId, chain_len: usize, lsns: Vec<Lsn> },

    /// Applying one redo record during materialization.
    MaterializeApply { page_id: PageId, lsn: Lsn, offset: u16, data_len: usize, data_preview: String },

    /// Page materialization complete.
    MaterializeComplete { page_id: PageId, read_point: Lsn },

    /// Materialized page inserted into storage page cache.
    PageCacheInsert { page_id: PageId, read_point: Lsn },

    /// Page inserted into compute buffer pool.
    BufferPoolInsert { page_id: PageId, read_point: Lsn },

    // ── Tiered storage ────────────────────────────────────────────────

    /// A WAL segment was sealed and a new one opened.
    SegmentRotation { sealed_id: u32, new_id: u32, sealed_lsn_range: (u64, u64) },

    /// A read hit a cold-tier segment, incurring extra latency.
    ColdTierRead { segment_id: u32, latency_ms: u64 },

    /// A segment was moved from hot to cold tier.
    SegmentCooled { segment_id: u32 },

    // ── State ─────────────────────────────────────────────────────────

    /// Full system state snapshot for diagram rendering.
    StateSnapshot {
        // Compute state
        node_label: String,
        read_point: Lsn,
        next_mtr: u64,
        buffer_pool_pages: Vec<PageId>,
        // Storage state
        next_lsn: Lsn,
        vcl: Lsn,
        vdl: Lsn,
        page_index: HashMap<PageId, Lsn>,
        lsn_offset_count: usize,
        page_cache_count: u64,
        // WAL state
        wal_file_size: u64,
        wal_lsn_range: Option<(Lsn, Lsn)>,
    },
}

/// Configuration for the visualization system.
#[derive(Debug, Clone)]
pub struct VizConfig {
    /// Delay between each rendered step.
    pub step_delay: Duration,
    /// Whether to use ANSI color codes.
    pub color: bool,
    /// Whether visualization is enabled (can be toggled at runtime).
    pub enabled: bool,
}

impl Default for VizConfig {
    fn default() -> Self {
        Self {
            step_delay: Duration::from_millis(300),
            color: true,
            enabled: true,
        }
    }
}
