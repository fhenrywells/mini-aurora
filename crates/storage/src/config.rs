use std::path::PathBuf;
use std::time::Duration;

/// Which storage backend to use.
pub enum StoragePreset {
    /// Single WAL file (original design).
    Base,
    /// Segmented WAL with hot/cold tiering.
    Tiered(TieredConfig),
}

/// Configuration for the tiered storage variant.
pub struct TieredConfig {
    /// Maximum bytes per segment before rotation.
    pub segment_size_bytes: u64,
    /// Artificial latency injected on cold-tier reads.
    pub cold_latency: Duration,
    /// Base directory for segment files (hot/ and cold/ subdirs created within).
    pub base_dir: PathBuf,
}
