use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use super::events::VizEvent;

/// Writes one line-delimited JSON entry per event to a file.
pub struct JsonTracer {
    writer: BufWriter<File>,
    seq: u64,
    start: Instant,
}

impl JsonTracer {
    /// Open a new trace file (creates or truncates).
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            seq: 0,
            start: Instant::now(),
        })
    }

    /// Write one event as a JSON line.
    pub fn trace(&mut self, event: &VizEvent) {
        self.seq += 1;
        let timestamp_us = self.start.elapsed().as_micros() as u64;

        // Build the JSON line manually to avoid a wrapper struct
        let event_json = match serde_json::to_string(event) {
            Ok(j) => j,
            Err(_) => return,
        };

        let line = format!(
            "{{\"seq\":{},\"timestamp_us\":{},\"event\":{}}}\n",
            self.seq, timestamp_us, event_json
        );

        let _ = self.writer.write_all(line.as_bytes());
        let _ = self.writer.flush();
    }
}
