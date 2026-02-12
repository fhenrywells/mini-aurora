use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Monotonically increasing log sequence number (1-based; 0 means "no LSN").
pub type Lsn = u64;

/// Logical page identifier.
pub type PageId = u64;

/// 8 KiB page — matches typical database page size.
pub const PAGE_SIZE: usize = 8192;

/// A fixed-size page image.
pub type Page = [u8; PAGE_SIZE];

/// Return a zeroed page (blank slate for materialization).
pub fn empty_page() -> Page {
    [0u8; PAGE_SIZE]
}

// ---------------------------------------------------------------------------
// Redo record — the unit of change in Aurora's "log is the database" model
// ---------------------------------------------------------------------------

/// Physical redo record that storage nodes can apply without understanding SQL.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RedoRecord {
    /// Log sequence number assigned at write time.
    pub lsn: Lsn,
    /// Which page this record modifies.
    pub page_id: PageId,
    /// Byte offset within the page where `data` should be written.
    pub offset: u16,
    /// Payload — the bytes to write at `offset`.
    pub data: Vec<u8>,
    /// Previous LSN that touched this same page (forms a per-page chain).
    pub prev_lsn: Lsn,
    /// Mini-transaction group identifier.
    pub mtr_id: u64,
    /// When `true`, this record is the Consistency Point LSN (CPL) of its MTR.
    pub is_mtr_end: bool,
}

// ---------------------------------------------------------------------------
// On-disk log entry header (fixed 47 bytes)
// ---------------------------------------------------------------------------

/// Fixed-size header written before each redo payload in the WAL file.
///
/// Layout (little-endian):
///   lsn       : u64  (8)
///   page_id   : u64  (8)
///   offset    : u16  (2)
///   data_len  : u16  (2)
///   prev_lsn  : u64  (8)
///   mtr_id    : u64  (8)
///   flags     : u8   (1)   — bit 0 = is_mtr_end
///   crc32     : u32  (4)   — CRC of header bytes (excl. crc field) + data
///   ─────────────────────
///   total     : 41 bytes
pub const LOG_ENTRY_HEADER_SIZE: usize = 41;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntryHeader {
    pub lsn: Lsn,
    pub page_id: PageId,
    pub offset: u16,
    pub data_len: u16,
    pub prev_lsn: Lsn,
    pub mtr_id: u64,
    pub flags: u8,
    pub crc32: u32,
}

impl LogEntryHeader {
    pub fn is_mtr_end(&self) -> bool {
        self.flags & 1 != 0
    }
}

// ---------------------------------------------------------------------------
// Durability watermarks
// ---------------------------------------------------------------------------

/// Aurora-style LSN watermarks that track durability progress.
#[derive(Clone, Debug, Default)]
pub struct DurabilityState {
    /// Volume Complete LSN — highest LSN where all prior LSNs are present.
    pub vcl: Lsn,
    /// Volume Durable LSN — highest CPL (MTR-end) whose LSN ≤ VCL.
    pub vdl: Lsn,
}

// ---------------------------------------------------------------------------
// StorageApi trait — the compute ↔ storage boundary
// ---------------------------------------------------------------------------

#[async_trait]
pub trait StorageApi: Send + Sync {
    /// Append a batch of redo records. Returns the new durable LSN.
    async fn append_redo(&self, records: Vec<RedoRecord>) -> Result<Lsn, StorageError>;

    /// Read a page materialized up to the given read-point LSN.
    async fn get_page(&self, page_id: PageId, read_point: Lsn) -> Result<Page, StorageError>;

    /// Get current durability state (VCL, VDL).
    async fn get_durability_state(&self) -> Result<DurabilityState, StorageError>;
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL corruption: {0}")]
    Corruption(String),

    #[error("page {page_id} not found at or below LSN {lsn}")]
    PageNotFound { page_id: PageId, lsn: Lsn },

    #[error("requested LSN {requested} exceeds durable LSN {durable}")]
    LsnBeyondDurable { requested: Lsn, durable: Lsn },

    #[error("redo record data overflows page: offset={offset} len={len}")]
    PageOverflow { offset: u16, len: usize },

    #[error("{0}")]
    Other(String),
}

impl fmt::Display for DurabilityState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DurabilityState(VCL={}, VDL={})", self.vcl, self.vdl)
    }
}
