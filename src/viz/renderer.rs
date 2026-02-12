use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::thread;

use mini_aurora_common::{Lsn, PageId};

use super::events::{VizConfig, VizEvent};

const PANEL_HEIGHT: usize = 15;
const PANEL_INNER: usize = 24;
// Panel total = 2 (borders) + PANEL_INNER = 26, plus 2 gutter = 28

/// Per-node compute state tracked in the renderer.
#[derive(Clone)]
struct NodeState {
    read_point: Lsn,
    next_mtr: u64,
    buffer_pool: Vec<PageId>,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            read_point: 0,
            next_mtr: 1,
            buffer_pool: Vec::new(),
        }
    }
}

/// Shared (storage + WAL) state tracked incrementally from events.
#[derive(Default)]
struct SharedState {
    next_lsn: Lsn,
    vcl: Lsn,
    vdl: Lsn,
    page_index: HashMap<PageId, Lsn>,
    lsn_offset_count: usize,
    page_cache_count: u64,
    wal_file_size: u64,
    wal_lsn_range: Option<(Lsn, Lsn)>,
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
    fn bold_green(&self, s: &str) -> String {
        if self.color { format!("\x1b[1;32m{s}\x1b[0m") } else { s.to_string() }
    }
}

/// Two-column renderer with live in-place updates.
///
/// Left column: scrolling event log (compact one-liners).
/// Right column: client-server diagram (Node A / Node B / STORAGE / WAL) updated after every event.
pub struct VizRenderer {
    config: VizConfig,
    step_num: usize,
    nodes: BTreeMap<String, NodeState>,
    active_node: String,
    interaction: String,
    shared: SharedState,
    event_log: Vec<String>,
    operation_header: String,
    term_width: usize,
}

impl VizRenderer {
    pub fn new(config: VizConfig) -> Self {
        let term_width = std::env::var("COLUMNS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(80usize)
            .max(80);

        Self {
            config,
            step_num: 0,
            nodes: BTreeMap::new(),
            active_node: String::new(),
            interaction: String::new(),
            shared: SharedState {
                next_lsn: 1,
                ..Default::default()
            },
            event_log: Vec::new(),
            operation_header: String::new(),
            term_width,
        }
    }

    pub fn config_mut(&mut self) -> &mut VizConfig {
        &mut self.config
    }

    /// Register a compute node so it appears in the panel.
    pub fn register_node(&mut self, label: &str) {
        self.nodes.entry(label.to_string()).or_default();
    }

    /// Set the active node for subsequent events. Clears the interaction text.
    pub fn set_active_node(&mut self, label: &str) {
        self.active_node = label.to_string();
        self.interaction.clear();
    }

    /// Reset step counter for a new operation.
    pub fn reset_steps(&mut self, _total: Option<usize>) {
        self.step_num = 0;
        self.event_log.clear();
    }

    /// Set the operation header and trigger initial frame draw.
    pub fn render_operation_header(&mut self, op: &str) {
        if !self.config.enabled {
            return;
        }
        self.operation_header = op.to_string();
        self.event_log.clear();
        self.step_num = 0;
        self.draw_frame();
    }

    /// Render a single event: update state, log it, redraw frame, sleep.
    pub fn render(&mut self, event: &VizEvent) {
        if !self.config.enabled {
            return;
        }

        match event {
            VizEvent::StateSnapshot { .. } => {
                self.update_state(event);
                // Full state replacement — redraw but no new log entry
                self.draw_frame();
            }
            _ => {
                self.update_state(event);
                self.step_num += 1;
                let line = Self::format_one_liner(event);
                self.event_log.push(line);
                self.draw_frame();
            }
        }

        if !self.config.step_delay.is_zero() {
            thread::sleep(self.config.step_delay);
        }
    }

    /// Map an event to the interaction line text for the active node.
    fn interaction_text(event: &VizEvent) -> String {
        match event {
            VizEvent::MtrCreated { .. } => "\u{00b7} prepare MTR".to_string(),
            VizEvent::AssignLsns { .. } => "\u{2193} assign LSNs".to_string(),
            VizEvent::LinkPrevLsn { .. } => "\u{2193} link prev".to_string(),
            VizEvent::WalAppend { .. } => "\u{2193} WAL append".to_string(),
            VizEvent::WalSync => "\u{2193} WAL fsync".to_string(),
            VizEvent::UpdatePageIndex { .. } => "\u{2193} update idx".to_string(),
            VizEvent::UpdateLsnOffset { .. } => "\u{2193} lsn offset".to_string(),
            VizEvent::AdvanceVcl { new, .. } => format!("\u{2191} VCL={new}"),
            VizEvent::AdvanceVdl { new, .. } => format!("\u{2191} VDL={new}"),
            VizEvent::UpdateReadPoint { new, .. } => format!("\u{00b7} rp\u{2192}{new}"),
            VizEvent::BufferPoolInvalidate { page_id } => format!("\u{00b7} evict pg{page_id}"),
            VizEvent::BufferPoolLookup { hit, .. } => {
                if *hit { "\u{00b7} buf: HIT".to_string() } else { "\u{00b7} buf: MISS".to_string() }
            }
            VizEvent::PageCacheLookup { hit, .. } => {
                if *hit { "\u{2193} cache: HIT".to_string() } else { "\u{2193} cache: MISS".to_string() }
            }
            VizEvent::PageIndexLookup { .. } => "\u{2193} idx lookup".to_string(),
            VizEvent::ChainWalkStep { lsn, skipped, .. } => {
                if *skipped {
                    format!("\u{2193} skip L{lsn}")
                } else {
                    format!("\u{2193} collect L{lsn}")
                }
            }
            VizEvent::ChainCollected { .. } => "\u{2193} chain done".to_string(),
            VizEvent::MaterializeApply { lsn, .. } => format!("\u{2193} apply L{lsn}"),
            VizEvent::MaterializeComplete { .. } => "\u{2193} materialized".to_string(),
            VizEvent::PageCacheInsert { .. } => "\u{2191} cache insert".to_string(),
            VizEvent::BufferPoolInsert { .. } => "\u{2191} page \u{2192} buf".to_string(),
            VizEvent::StateSnapshot { .. } => String::new(),
        }
    }

    /// Update internal state from an event.
    fn update_state(&mut self, event: &VizEvent) {
        // Update interaction text for non-snapshot events
        if !matches!(event, VizEvent::StateSnapshot { .. }) {
            self.interaction = Self::interaction_text(event);
        }

        match event {
            // Per-node events: route to active node
            VizEvent::MtrCreated { mtr_id, .. } => {
                if let Some(node) = self.nodes.get_mut(&self.active_node) {
                    node.next_mtr = mtr_id + 1;
                }
            }
            VizEvent::UpdateReadPoint { new, .. } => {
                if let Some(node) = self.nodes.get_mut(&self.active_node) {
                    node.read_point = *new;
                }
            }
            VizEvent::BufferPoolInvalidate { page_id } => {
                if let Some(node) = self.nodes.get_mut(&self.active_node) {
                    node.buffer_pool.retain(|p| p != page_id);
                }
            }
            VizEvent::BufferPoolInsert { page_id, .. } => {
                if let Some(node) = self.nodes.get_mut(&self.active_node) {
                    if !node.buffer_pool.contains(page_id) {
                        node.buffer_pool.push(*page_id);
                    }
                }
            }

            // Shared state events
            VizEvent::AssignLsns { last_lsn, .. } => {
                self.shared.next_lsn = last_lsn + 1;
            }
            VizEvent::WalAppend { offset, bytes, first_lsn, last_lsn } => {
                self.shared.wal_file_size = offset + bytes;
                self.shared.wal_lsn_range = Some(match self.shared.wal_lsn_range {
                    Some((first, _)) => (first, *last_lsn),
                    None => (*first_lsn, *last_lsn),
                });
            }
            VizEvent::UpdatePageIndex { page_id, latest_lsn } => {
                self.shared.page_index.insert(*page_id, *latest_lsn);
            }
            VizEvent::UpdateLsnOffset { .. } => {
                self.shared.lsn_offset_count += 1;
            }
            VizEvent::AdvanceVcl { new, .. } => {
                self.shared.vcl = *new;
            }
            VizEvent::AdvanceVdl { new, .. } => {
                self.shared.vdl = *new;
            }
            VizEvent::PageCacheInsert { .. } => {
                self.shared.page_cache_count += 1;
            }
            VizEvent::StateSnapshot {
                node_label,
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
            } => {
                // Update per-node state
                if let Some(node) = self.nodes.get_mut(node_label) {
                    node.read_point = *read_point;
                    node.next_mtr = *next_mtr;
                    node.buffer_pool = buffer_pool_pages.clone();
                }
                // Update shared state
                self.shared.next_lsn = *next_lsn;
                self.shared.vcl = *vcl;
                self.shared.vdl = *vdl;
                self.shared.page_index = page_index.clone();
                self.shared.lsn_offset_count = *lsn_offset_count;
                self.shared.page_cache_count = *page_cache_count;
                self.shared.wal_file_size = *wal_file_size;
                self.shared.wal_lsn_range = *wal_lsn_range;
            }

            // Events that don't change tracked state
            _ => {}
        }
    }

    /// Convert an event to a compact one-liner (plain text, no ANSI).
    fn format_one_liner(event: &VizEvent) -> String {
        match event {
            VizEvent::MtrCreated { mtr_id, num_records } => {
                format!("MTR #{mtr_id} ({num_records} record)")
            }
            VizEvent::AssignLsns { first_lsn, last_lsn } => {
                if first_lsn == last_lsn {
                    format!("Assign LSN {first_lsn}")
                } else {
                    format!("Assign LSNs {first_lsn}..{last_lsn}")
                }
            }
            VizEvent::LinkPrevLsn { lsn, page_id, prev_lsn } => {
                if *prev_lsn == 0 {
                    format!("Link L{lsn}(pg{page_id}) prev=none")
                } else {
                    format!("Link L{lsn}(pg{page_id}) prev=L{prev_lsn}")
                }
            }
            VizEvent::WalAppend { first_lsn, last_lsn, offset, bytes } => {
                if first_lsn == last_lsn {
                    format!("WAL append L{first_lsn} @{offset} ({bytes}B)")
                } else {
                    format!("WAL append L{first_lsn}..{last_lsn} @{offset} ({bytes}B)")
                }
            }
            VizEvent::WalSync => "WAL sync (fsync)".to_string(),
            VizEvent::UpdatePageIndex { page_id, latest_lsn } => {
                format!("Page index pg{page_id}->L{latest_lsn}")
            }
            VizEvent::UpdateLsnOffset { lsn, file_offset } => {
                format!("LSN offset L{lsn}->{file_offset}")
            }
            VizEvent::AdvanceVcl { old, new } => format!("VCL: {old}->{new}"),
            VizEvent::AdvanceVdl { old, new } => format!("VDL: {old}->{new} (CPL)"),
            VizEvent::UpdateReadPoint { old, new } => {
                format!("read_point: {old}->{new}")
            }
            VizEvent::BufferPoolInvalidate { page_id } => {
                format!("Evict pg{page_id} from buffer")
            }
            VizEvent::BufferPoolLookup { page_id, read_point, hit } => {
                let tag = if *hit { "HIT" } else { "MISS" };
                format!("BufPool pg{page_id} @L{read_point}: {tag}")
            }
            VizEvent::PageCacheLookup { page_id, read_point, hit } => {
                let tag = if *hit { "HIT" } else { "MISS" };
                format!("PageCache pg{page_id} @L{read_point}: {tag}")
            }
            VizEvent::PageIndexLookup { page_id, latest_lsn } => match latest_lsn {
                Some(lsn) => format!("PageIdx pg{page_id}->L{lsn}"),
                None => format!("PageIdx pg{page_id}: not found"),
            },
            VizEvent::ChainWalkStep { lsn, skipped, .. } => {
                if *skipped {
                    format!("Chain: skip L{lsn}")
                } else {
                    format!("Chain: collect L{lsn}")
                }
            }
            VizEvent::ChainCollected { page_id, chain_len, lsns } => {
                let chain_str: Vec<String> = lsns.iter().map(|l| format!("L{l}")).collect();
                format!(
                    "Chain pg{page_id}: {chain_len} rec [{}]",
                    chain_str.join("->")
                )
            }
            VizEvent::MaterializeApply { lsn, offset, data_len, .. } => {
                format!("Apply L{lsn} {data_len}B @{offset}")
            }
            VizEvent::MaterializeComplete { page_id, read_point } => {
                format!("Materialized pg{page_id} @L{read_point}")
            }
            VizEvent::PageCacheInsert { page_id, read_point } => {
                format!("Cache pg{page_id} @L{read_point}")
            }
            VizEvent::BufferPoolInsert { page_id, read_point } => {
                format!("BufPool insert pg{page_id} @L{read_point}")
            }
            VizEvent::StateSnapshot { .. } => String::new(),
        }
    }

    /// Build the 15 right-panel lines from current state.
    ///
    /// Layout:
    ///   Row 0:    top border "Node A @L<rp>"
    ///   Row 1:    Node A state line
    ///   Row 2:    Node A interaction line
    ///   Row 3:    separator "Node B @L<rp>"
    ///   Row 4:    Node B state line
    ///   Row 5:    Node B interaction line
    ///   Row 6:    separator "STORAGE"
    ///   Row 7:    VCL/VDL/nxt
    ///   Row 8:    page index
    ///   Row 9:    cache/offsets
    ///   Row 10:   separator "WAL"
    ///   Row 11:   LSN entries
    ///   Row 12:   size
    ///   Row 13:   blank
    ///   Row 14:   bottom border
    fn build_panel_lines(&self) -> Vec<String> {
        let a = Ansi::new(self.config.color);
        let w = PANEL_INNER;
        let panel_w = PANEL_INNER + 2;

        // Collect nodes in order (BTreeMap gives sorted keys)
        let node_labels: Vec<&String> = self.nodes.keys().collect();

        // Helper: get node state or default
        let get_node = |idx: usize| -> (&str, &NodeState) {
            static DEFAULT: NodeState = NodeState {
                read_point: 0,
                next_mtr: 1,
                buffer_pool: Vec::new(),
            };
            if idx < node_labels.len() {
                let label = node_labels[idx];
                (label.as_str(), self.nodes.get(label).unwrap_or(&DEFAULT))
            } else {
                ("?", &DEFAULT)
            }
        };

        // Format buffer pool for a node
        let fmt_bp = |bp: &[PageId]| -> String {
            if bp.is_empty() {
                "(empty)".to_string()
            } else {
                bp.iter().map(|p| format!("pg{p}")).collect::<Vec<_>>().join(",")
            }
        };

        // Format page index
        let s = &self.shared;
        let mut pi_entries: Vec<_> = s.page_index.iter().collect();
        pi_entries.sort_by_key(|(&pid, _)| pid);
        let pi_str = if pi_entries.is_empty() {
            "(empty)".to_string()
        } else {
            pi_entries.iter().map(|(p, l)| format!("{p}\u{2192}L{l}")).collect::<Vec<_>>().join(" ")
        };

        // WAL display
        let wal_str = match s.wal_lsn_range {
            Some((first, last)) => {
                let entries: Vec<String> = (first..=last).map(|l| format!("L{l}")).collect();
                let joined = entries.join("|");
                if joined.len() > w - 2 {
                    format!("[L{first}..L{last}]")
                } else {
                    format!("[{joined}]")
                }
            }
            None => "(empty)".to_string(),
        };

        // Pad/truncate a plain-text row to PANEL_INNER
        let pad_row = |text: &str| -> String {
            let len = text.len();
            if len >= w {
                text[..w].to_string()
            } else {
                format!("{text}{}", " ".repeat(w - len))
            }
        };

        // Section header helpers
        let section_top = |label: &str| -> String {
            let label_part = format!("\u{250c}\u{2500}\u{2500} {} ", label);
            let label_chars = label_part.chars().count();
            let fill = "\u{2500}".repeat(panel_w.saturating_sub(label_chars + 1));
            a.cyan(&format!("{label_part}{fill}\u{2510}"))
        };
        let section_sep = |label: &str| -> String {
            let label_part = format!("\u{251c}\u{2500}\u{2500} {} ", label);
            let label_chars = label_part.chars().count();
            let fill = "\u{2500}".repeat(panel_w.saturating_sub(label_chars + 1));
            a.cyan(&format!("{label_part}{fill}\u{2524}"))
        };

        // Build node state + interaction lines
        let build_node_lines = |idx: usize| -> (String, String) {
            let (label, node) = get_node(idx);
            let bp = fmt_bp(&node.buffer_pool);
            let state_text = format!(" mtr:{} buf:{}", node.next_mtr, bp);

            let is_active = label == self.active_node;
            let interaction_text = if is_active && !self.interaction.is_empty() {
                format!("  {}", self.interaction)
            } else {
                "  (idle)".to_string()
            };

            let state_line = format!(
                "{}{}{}",
                a.cyan("\u{2502}"),
                a.yellow(&pad_row(&state_text)),
                a.cyan("\u{2502}")
            );
            let int_line = if is_active && !self.interaction.is_empty() {
                format!(
                    "{}{}{}",
                    a.cyan("\u{2502}"),
                    a.bold_green(&pad_row(&interaction_text)),
                    a.cyan("\u{2502}")
                )
            } else {
                format!(
                    "{}{}{}",
                    a.cyan("\u{2502}"),
                    a.dim(&pad_row(&interaction_text)),
                    a.cyan("\u{2502}")
                )
            };

            (state_line, int_line)
        };

        let mut lines = Vec::with_capacity(PANEL_HEIGHT);

        // --- Node A (row 0-2) ---
        let (label_a, node_a) = get_node(0);
        let header_a = format!("Node {} @L{}", label_a, node_a.read_point);
        lines.push(section_top(&header_a)); // row 0
        let (state_a, int_a) = build_node_lines(0);
        lines.push(state_a);  // row 1
        lines.push(int_a);    // row 2

        // --- Node B (row 3-5) ---
        if node_labels.len() > 1 {
            let (label_b, node_b) = get_node(1);
            let header_b = format!("Node {} @L{}", label_b, node_b.read_point);
            lines.push(section_sep(&header_b)); // row 3
            let (state_b, int_b) = build_node_lines(1);
            lines.push(state_b);  // row 4
            lines.push(int_b);    // row 5
        } else {
            // Single-node mode: fill with blanks
            lines.push(section_sep("(single node)"));
            lines.push(format!(
                "{}{}{}",
                a.cyan("\u{2502}"),
                pad_row(""),
                a.cyan("\u{2502}")
            ));
            lines.push(format!(
                "{}{}{}",
                a.cyan("\u{2502}"),
                pad_row(""),
                a.cyan("\u{2502}")
            ));
        }

        // --- STORAGE (row 6-9) ---
        lines.push(section_sep("STORAGE")); // row 6
        let vcl_line = format!(" VCL:{} VDL:{} nxt:{}", s.vcl, s.vdl, s.next_lsn);
        lines.push(format!(
            "{}{}{}",
            a.cyan("\u{2502}"),
            a.yellow(&pad_row(&vcl_line)),
            a.cyan("\u{2502}")
        )); // row 7
        let pg_line = format!(" pg: {pi_str}");
        lines.push(format!(
            "{}{}{}",
            a.cyan("\u{2502}"),
            a.yellow(&pad_row(&pg_line)),
            a.cyan("\u{2502}")
        )); // row 8
        let cache_line = format!(" cache:{} off:{}", s.page_cache_count, s.lsn_offset_count);
        lines.push(format!(
            "{}{}{}",
            a.cyan("\u{2502}"),
            a.yellow(&pad_row(&cache_line)),
            a.cyan("\u{2502}")
        )); // row 9

        // --- WAL (row 10-12) ---
        lines.push(section_sep("WAL")); // row 10
        let wal_display = format!(" {wal_str}");
        lines.push(format!(
            "{}{}{}",
            a.cyan("\u{2502}"),
            a.green(&pad_row(&wal_display)),
            a.cyan("\u{2502}")
        )); // row 11
        let wal_size = format!(" {} bytes", s.wal_file_size);
        lines.push(format!(
            "{}{}{}",
            a.cyan("\u{2502}"),
            a.green(&pad_row(&wal_size)),
            a.cyan("\u{2502}")
        )); // row 12

        // Row 13: blank
        lines.push(format!(
            "{}{}{}",
            a.cyan("\u{2502}"),
            pad_row(""),
            a.cyan("\u{2502}")
        ));
        // Row 14: bottom border
        lines.push(a.cyan(&format!("\u{2514}{}\u{2518}", "\u{2500}".repeat(panel_w - 2))));

        lines
    }

    /// Format a single event log entry for the left column.
    fn format_log_entry(&self, idx: usize, is_active: bool, width: usize) -> String {
        let a = Ansi::new(self.config.color);
        let step = idx + 1; // 1-based step numbers
        let text = &self.event_log[idx];

        // Build plain text: ">3. text" or " 3. text"
        let prefix = if is_active { ">" } else { " " };
        let plain = format!("{prefix}{step:>2}. {text}");

        // Truncate or pad to width
        let display = if plain.len() > width {
            format!("{}..", &plain[..width - 2])
        } else {
            format!("{plain:<width$}")
        };

        if is_active {
            a.bold_green(&display)
        } else {
            a.dim(&display)
        }
    }

    /// Build and flush the entire two-column frame.
    fn draw_frame(&self) {
        let a = Ansi::new(self.config.color);
        let mut buf = String::with_capacity(4096);

        // Cursor home (no full clear — we overwrite in-place)
        buf.push_str("\x1b[H");

        // Line 1: operation header (full width)
        let header = format!("\u{2550}\u{2550}\u{2550} {} ", self.operation_header);
        let header_visual_len = header.chars().count();
        let pad = self.term_width.saturating_sub(header_visual_len);
        let header_line = format!("{header}{}", "\u{2550}".repeat(pad));
        buf.push_str(&a.bold(&a.cyan(&header_line)));
        buf.push_str("\x1b[K\n"); // clear rest of line

        // Blank line
        buf.push_str("\x1b[K\n");

        // Build right panel
        let panel_lines = self.build_panel_lines();

        // Left column width: total - panel_width(26) - gutter(2)
        let panel_total = PANEL_INNER + 2; // borders
        let gutter = 2;
        let left_width = self.term_width.saturating_sub(panel_total + gutter);

        // Show last PANEL_HEIGHT entries from event log
        let log_start = self.event_log.len().saturating_sub(PANEL_HEIGHT);

        for row in 0..PANEL_HEIGHT {
            let log_idx = log_start + row;
            let left = if log_idx < self.event_log.len() {
                let is_active = log_idx == self.event_log.len() - 1;
                self.format_log_entry(log_idx, is_active, left_width)
            } else {
                " ".repeat(left_width)
            };

            buf.push_str(&left);
            buf.push_str(&" ".repeat(gutter));
            buf.push_str(&panel_lines[row]);
            buf.push_str("\x1b[K\n"); // clear rest of line
        }

        // Clear any leftover lines from previous taller frames
        buf.push_str("\x1b[J");

        let stdout = io::stdout();
        let mut handle = stdout.lock();
        let _ = handle.write_all(buf.as_bytes());
        let _ = handle.flush();
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
