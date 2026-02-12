use std::thread;

use mini_aurora_common::{Lsn, PageId};

use super::events::{VizConfig, VizEvent};

/// Renders VizEvents as ANSI-colored ASCII diagrams and step descriptions.
pub struct VizRenderer {
    config: VizConfig,
    step_num: usize,
    total_steps: Option<usize>,
}

// ANSI escape helpers
struct Ansi {
    color: bool,
}

impl Ansi {
    fn new(color: bool) -> Self {
        Self { color }
    }
    fn green(&self, s: &str) -> String {
        if self.color { format!("\x1b[32m{s}\x1b[0m") } else { s.to_string() }
    }
    fn yellow(&self, s: &str) -> String {
        if self.color { format!("\x1b[33m{s}\x1b[0m") } else { s.to_string() }
    }
    fn cyan(&self, s: &str) -> String {
        if self.color { format!("\x1b[36m{s}\x1b[0m") } else { s.to_string() }
    }
    fn bold(&self, s: &str) -> String {
        if self.color { format!("\x1b[1m{s}\x1b[0m") } else { s.to_string() }
    }
    fn dim(&self, s: &str) -> String {
        if self.color { format!("\x1b[2m{s}\x1b[0m") } else { s.to_string() }
    }
}

impl VizRenderer {
    pub fn new(config: VizConfig) -> Self {
        Self {
            config,
            step_num: 0,
            total_steps: None,
        }
    }

    pub fn config_mut(&mut self) -> &mut VizConfig {
        &mut self.config
    }

    /// Reset step counter for a new operation.
    pub fn reset_steps(&mut self, total: Option<usize>) {
        self.step_num = 0;
        self.total_steps = total;
    }

    /// Render a single event. Sleeps for step_delay after rendering.
    pub fn render(&mut self, event: &VizEvent) {
        if !self.config.enabled {
            return;
        }

        match event {
            VizEvent::StateSnapshot { .. } => {
                self.render_diagram(event);
            }
            _ => {
                self.step_num += 1;
                self.render_step(event);
            }
        }

        if !self.config.step_delay.is_zero() {
            thread::sleep(self.config.step_delay);
        }
    }

    fn render_step(&self, event: &VizEvent) {
        let a = Ansi::new(self.config.color);

        let step_label = match self.total_steps {
            Some(total) => format!("Step {}/{total}", self.step_num),
            None => format!("Step {}", self.step_num),
        };

        let (category, detail) = match event {
            VizEvent::MtrCreated { mtr_id, num_records } => (
                "Compute: Create MTR",
                format!("MTR #{mtr_id} with {num_records} record(s)"),
            ),
            VizEvent::AssignLsns { first_lsn, last_lsn } => (
                "Storage: Assign LSNs",
                if first_lsn == last_lsn {
                    format!("Assigned LSN {first_lsn}")
                } else {
                    format!("Assigned LSNs {first_lsn}..{last_lsn}")
                },
            ),
            VizEvent::LinkPrevLsn { lsn, page_id, prev_lsn } => (
                "Storage: Link prev_lsn",
                if *prev_lsn == 0 {
                    format!("L{lsn} (pg{page_id}): first record for this page")
                } else {
                    format!("L{lsn} (pg{page_id}): prev_lsn → L{prev_lsn}")
                },
            ),
            VizEvent::WalAppend { first_lsn, last_lsn, offset, bytes } => (
                "WAL: Append",
                if first_lsn == last_lsn {
                    format!("Writing LSN {first_lsn} at offset {offset} ({bytes} bytes)")
                } else {
                    format!("Writing LSNs {first_lsn}..{last_lsn} at offset {offset} ({bytes} bytes)")
                },
            ),
            VizEvent::WalSync => (
                "WAL: Sync",
                "fsync to disk — records are now durable".to_string(),
            ),
            VizEvent::UpdatePageIndex { page_id, latest_lsn } => (
                "Storage: Update page index",
                format!("pg{page_id} → L{latest_lsn}"),
            ),
            VizEvent::UpdateLsnOffset { lsn, file_offset } => (
                "Storage: Update LSN offset",
                format!("L{lsn} → file offset {file_offset}"),
            ),
            VizEvent::AdvanceVcl { old, new } => (
                "Storage: Advance VCL",
                format!("VCL: {} → {}", a.dim(&old.to_string()), a.yellow(&new.to_string())),
            ),
            VizEvent::AdvanceVdl { old, new } => (
                "Storage: Advance VDL",
                format!("VDL: {} → {} (new consistency point)", a.dim(&old.to_string()), a.yellow(&new.to_string())),
            ),
            VizEvent::UpdateReadPoint { old, new } => (
                "Compute: Update read_point",
                format!("read_point: {} → {}", a.dim(&old.to_string()), a.yellow(&new.to_string())),
            ),
            VizEvent::BufferPoolInvalidate { page_id } => (
                "Compute: Buffer pool invalidate",
                format!("Evicted pg{page_id} (stale after write)"),
            ),
            VizEvent::BufferPoolLookup { page_id, read_point, hit } => (
                "Compute: Buffer pool lookup",
                if *hit {
                    format!("pg{page_id} @L{read_point} → {}", a.green("HIT"))
                } else {
                    format!("pg{page_id} @L{read_point} → {}", a.yellow("MISS"))
                },
            ),
            VizEvent::PageCacheLookup { page_id, read_point, hit } => (
                "Storage: Page cache lookup",
                if *hit {
                    format!("pg{page_id} @L{read_point} → {}", a.green("HIT"))
                } else {
                    format!("pg{page_id} @L{read_point} → {}", a.yellow("MISS"))
                },
            ),
            VizEvent::PageIndexLookup { page_id, latest_lsn } => (
                "Storage: Page index lookup",
                match latest_lsn {
                    Some(lsn) => format!("pg{page_id} → latest L{lsn}"),
                    None => format!("pg{page_id} → not found"),
                },
            ),
            VizEvent::ChainWalkStep { page_id, lsn, prev_lsn, skipped } => (
                "WAL: Chain walk",
                if *skipped {
                    format!("pg{page_id}: L{lsn} > read_point, following prev_lsn → L{prev_lsn}")
                } else {
                    format!(
                        "pg{page_id}: {} L{lsn} (prev_lsn={})",
                        a.green("collecting"),
                        if *prev_lsn == 0 { "none".to_string() } else { format!("L{prev_lsn}") }
                    )
                },
            ),
            VizEvent::ChainCollected { page_id, chain_len, lsns } => (
                "WAL: Chain complete",
                format!(
                    "pg{page_id}: {chain_len} record(s) [{}]",
                    lsns.iter().map(|l| format!("L{l}")).collect::<Vec<_>>().join(" → ")
                ),
            ),
            VizEvent::MaterializeApply { page_id, lsn, offset, data_len, data_preview } => (
                "Pagestore: Apply redo",
                format!(
                    "pg{page_id}: L{lsn} — write {data_len}B at offset {offset} {:?}",
                    data_preview
                ),
            ),
            VizEvent::MaterializeComplete { page_id, read_point } => (
                "Pagestore: Materialize complete",
                format!("pg{page_id} materialized @L{read_point}"),
            ),
            VizEvent::PageCacheInsert { page_id, read_point } => (
                "Storage: Page cache insert",
                format!("Cached pg{page_id} @L{read_point}"),
            ),
            VizEvent::BufferPoolInsert { page_id, read_point } => (
                "Compute: Buffer pool insert",
                format!("Cached pg{page_id} @L{read_point}"),
            ),
            VizEvent::StateSnapshot { .. } => unreachable!(),
        };

        let header = format!("── {step_label} ── {category} ");
        let pad_len = 72usize.saturating_sub(header.len());
        let line = format!("{header}{}", "─".repeat(pad_len));
        println!("{}", a.bold(&line));
        println!("  {detail}");
        println!();
    }

    fn render_diagram(&self, event: &VizEvent) {
        let VizEvent::StateSnapshot {
            read_point,
            next_mtr,
            buffer_pool_pages,
            next_lsn,
            vcl,
            vdl,
            page_index,
            lsn_offset_count,
            page_cache_count,
            wal_file_size,
            wal_lsn_range,
        } = event else { return };

        let a = Ansi::new(self.config.color);

        // Format page index entries (sorted)
        let mut pi_entries: Vec<(&PageId, &Lsn)> = page_index.iter().collect();
        pi_entries.sort_by_key(|(&pid, _)| pid);
        let pi_str = if pi_entries.is_empty() {
            "(empty)".to_string()
        } else {
            pi_entries
                .iter()
                .map(|(pid, lsn)| format!("{pid}→L{lsn}"))
                .collect::<Vec<_>>()
                .join("  ")
        };

        // Format buffer pool pages
        let bp_str = if buffer_pool_pages.is_empty() {
            "(empty)".to_string()
        } else {
            format!(
                "[{}]",
                buffer_pool_pages
                    .iter()
                    .map(|p| format!("pg{p}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        // WAL LSN range display
        let wal_lsns = match wal_lsn_range {
            Some((first, last)) => {
                let entries: Vec<String> = (*first..=*last).map(|l| format!("L{l}")).collect();
                format!("[{}]", entries.join("|"))
            }
            None => "(empty)".to_string(),
        };

        println!();
        println!("{}", a.cyan("┌─────────── COMPUTE ───────────┐    ┌──────────── STORAGE ─────────────┐"));
        println!(
            "{}  read_point: {:<17}{}  next_lsn: {:<23}{}",
            a.cyan("│"),
            a.yellow(&read_point.to_string()),
            a.cyan("│    │"),
            a.yellow(&next_lsn.to_string()),
            a.cyan("│"),
        );
        println!(
            "{}  next_mtr: {:<19}{}  VCL: {}  VDL: {:<20}{}",
            a.cyan("│"),
            next_mtr,
            a.cyan("│    │"),
            a.yellow(&vcl.to_string()),
            a.yellow(&vdl.to_string()),
            a.cyan("│"),
        );
        println!(
            "{}  buffer_pool: {:<16}{}{}",
            a.cyan("│"),
            bp_str,
            a.cyan("│    │"),
            format!(
                "                                  {}",
                a.cyan("│")
            ),
        );
        println!(
            "{}{}{}  page_index:{}",
            a.cyan("│"),
            " ".repeat(31),
            a.cyan("│    │"),
            format!(
                "                     {}",
                a.cyan("│")
            ),
        );
        println!(
            "{}{}{}    {:<30}{}",
            a.cyan("│"),
            " ".repeat(31),
            a.cyan("│    │"),
            pi_str,
            a.cyan("│"),
        );
        println!(
            "{}{}{}  lsn_offsets: {} entries{:<10}{}",
            a.cyan("│"),
            " ".repeat(31),
            a.cyan("│    │"),
            lsn_offset_count,
            "",
            a.cyan("│"),
        );
        println!(
            "{}{}{}  page_cache: {} entries{:<11}{}",
            a.cyan("│"),
            " ".repeat(31),
            a.cyan("│    │"),
            page_cache_count,
            "",
            a.cyan("│"),
        );
        println!(
            "{}───────────────┬───────────────{}    {}──────────────────────────────────{}",
            a.cyan("└"),
            a.cyan("┘"),
            a.cyan("└"),
            a.cyan("┘"),
        );
        println!(
            "                {} StorageApi                       {}",
            a.cyan("│"),
            a.cyan("│"),
        );
        println!(
            "                {}                                  {}",
            a.cyan("▼"),
            a.cyan("▼"),
        );
        println!(
            "{}",
            a.cyan("┌──────────────────────────── WAL (disk) ───────────────────────────────┐")
        );
        println!(
            "{}  {:<42} file: {} bytes{}",
            a.cyan("│"),
            a.green(&wal_lsns),
            wal_file_size,
            format!("{:>width$}{}", "", a.cyan("│"), width = 72usize
                .saturating_sub(4 + 42 + 7 + wal_file_size.to_string().len() + 6)),
        );
        println!(
            "{}",
            a.cyan("└──────────────────────────────────────────────────────────────────────┘")
        );
        println!();
    }

    /// Render a header for a new operation (put/get).
    pub fn render_operation_header(&self, op: &str) {
        if !self.config.enabled {
            return;
        }
        let a = Ansi::new(self.config.color);
        println!();
        let line = format!("═══ {op} ");
        let pad = 72usize.saturating_sub(line.len());
        println!("{}", a.bold(&a.cyan(&format!("{line}{}", "═".repeat(pad)))));
        println!();
    }
}

/// Helper: format a data preview (first N bytes as string or hex).
pub fn data_preview(data: &[u8], max_len: usize) -> String {
    let slice = if data.len() > max_len { &data[..max_len] } else { data };
    match std::str::from_utf8(slice) {
        Ok(s) => {
            if data.len() > max_len {
                format!("{s}...")
            } else {
                s.to_string()
            }
        }
        Err(_) => {
            let hex: Vec<String> = slice.iter().map(|b| format!("{b:02X}")).collect();
            if data.len() > max_len {
                format!("0x{}...", hex.join(""))
            } else {
                format!("0x{}", hex.join(""))
            }
        }
    }
}
