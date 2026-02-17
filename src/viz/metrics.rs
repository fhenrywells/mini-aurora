use std::fmt;
use std::time::Instant;

use super::events::VizEvent;

/// Collects aggregate metrics from visualization events.
pub struct MetricsCollector {
    write_count: u64,
    read_count: u64,
    page_cache_hits: u64,
    page_cache_misses: u64,
    buffer_pool_hits: u64,
    buffer_pool_misses: u64,
    materialize_count: u64,
    materialize_total_us: u64,
    wal_bytes_written: u64,
    vcl: u64,
    vdl: u64,
    segment_rotations: u64,
    segments_cooled: u64,
    cold_tier_reads: u64,
    cold_latency_total_ms: u64,
    start_time: Instant,
}

/// Snapshot of collected metrics.
pub struct MetricsSummary {
    pub write_count: u64,
    pub read_count: u64,
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,
    pub buffer_pool_hits: u64,
    pub buffer_pool_misses: u64,
    pub materialize_count: u64,
    pub wal_bytes_written: u64,
    pub vcl: u64,
    pub vdl: u64,
    pub segment_rotations: u64,
    pub segments_cooled: u64,
    pub cold_tier_reads: u64,
    pub cold_latency_total_ms: u64,
    pub uptime_secs: f64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            write_count: 0,
            read_count: 0,
            page_cache_hits: 0,
            page_cache_misses: 0,
            buffer_pool_hits: 0,
            buffer_pool_misses: 0,
            materialize_count: 0,
            materialize_total_us: 0,
            wal_bytes_written: 0,
            vcl: 0,
            vdl: 0,
            segment_rotations: 0,
            segments_cooled: 0,
            cold_tier_reads: 0,
            cold_latency_total_ms: 0,
            start_time: Instant::now(),
        }
    }

    /// Record a single event, updating counters.
    pub fn record_event(&mut self, event: &VizEvent) {
        match event {
            VizEvent::WalAppend { bytes, .. } => {
                self.write_count += 1;
                self.wal_bytes_written += bytes;
            }
            VizEvent::PageCacheLookup { hit, .. } => {
                if *hit {
                    self.page_cache_hits += 1;
                } else {
                    self.page_cache_misses += 1;
                }
            }
            VizEvent::BufferPoolLookup { hit, .. } => {
                if *hit {
                    self.buffer_pool_hits += 1;
                } else {
                    self.buffer_pool_misses += 1;
                    self.read_count += 1;
                }
            }
            VizEvent::MaterializeComplete { .. } => {
                self.materialize_count += 1;
            }
            VizEvent::AdvanceVcl { new, .. } => {
                self.vcl = *new;
            }
            VizEvent::AdvanceVdl { new, .. } => {
                self.vdl = *new;
            }
            VizEvent::SegmentRotation { .. } => {
                self.segment_rotations += 1;
            }
            VizEvent::SegmentCooled { .. } => {
                self.segments_cooled += 1;
            }
            VizEvent::ColdTierRead { latency_ms, .. } => {
                self.cold_tier_reads += 1;
                self.cold_latency_total_ms += latency_ms;
            }
            _ => {}
        }
    }

    /// Return an aggregate summary.
    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            write_count: self.write_count,
            read_count: self.read_count,
            page_cache_hits: self.page_cache_hits,
            page_cache_misses: self.page_cache_misses,
            buffer_pool_hits: self.buffer_pool_hits,
            buffer_pool_misses: self.buffer_pool_misses,
            materialize_count: self.materialize_count,
            wal_bytes_written: self.wal_bytes_written,
            vcl: self.vcl,
            vdl: self.vdl,
            segment_rotations: self.segment_rotations,
            segments_cooled: self.segments_cooled,
            cold_tier_reads: self.cold_tier_reads,
            cold_latency_total_ms: self.cold_latency_total_ms,
            uptime_secs: self.start_time.elapsed().as_secs_f64(),
        }
    }

    /// Reset all counters and restart the clock.
    pub fn reset(&mut self) {
        *self = Self::new();
    }
}

impl fmt::Display for MetricsSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cache_total = self.page_cache_hits + self.page_cache_misses;
        let cache_pct = if cache_total > 0 {
            (self.page_cache_hits as f64 / cache_total as f64 * 100.0) as u64
        } else {
            0
        };
        let wal_kb = self.wal_bytes_written / 1024;
        write!(
            f,
            "Writes: {} | Reads: {} | Cache hit: {}% | Materializations: {}\n\
             WAL: {} KB | VCL={} VDL={} | Uptime: {:.1}s",
            self.write_count,
            self.read_count,
            cache_pct,
            self.materialize_count,
            wal_kb,
            self.vcl,
            self.vdl,
            self.uptime_secs,
        )
    }
}
