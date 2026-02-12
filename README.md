# mini-aurora

A minimal implementation of Amazon Aurora's "log is the database" architecture, built in Rust for educational purposes.

## What is this?

Aurora's key insight is that the write-ahead log (WAL) _is_ the database — pages are materialized on-demand by replaying redo records rather than being written to disk as full page images. This project implements that idea from scratch across five crates that mirror Aurora's internal layering.

## Architecture

```
┌─────────── COMPUTE ───────────┐    ┌──────────── STORAGE ─────────────┐
│  Buffer Pool (page cache)     │    │  WAL Writer / Reader              │
│  Mini-Transactions (MTR)      │    │  Page Index (page → latest LSN)   │
│  Read Point (tracks VDL)      │    │  LSN Offsets (LSN → file pos)     │
│                               │    │  Page Cache (materialized pages)  │
│                               │    │  Durability: VCL / VDL            │
└───────────────┬───────────────┘    └──────────────┬───────────────────┘
                │ StorageApi trait                   │
                └──────────────┬────────────────────┘
                               ▼
                ┌──────────── WAL (disk) ────────────┐
                │  Append-only log with CRC32        │
                │  Per-page redo chains (prev_lsn)   │
                └────────────────────────────────────┘
```

### Crates

| Crate | Purpose |
|-------|---------|
| `common` | Core types (`RedoRecord`, `Lsn`, `PageId`, `StorageApi` trait, `DurabilityState`) |
| `wal` | Append-only WAL writer, reader with chain walking, crash recovery |
| `pagestore` | Page materialization (replay redo chain onto zeroed page) + LRU page cache |
| `storage` | Storage engine combining WAL + pagestore, implements `StorageApi` |
| `compute` | Compute engine with buffer pool, mini-transactions, read point tracking |

### Key Concepts Implemented

- **Redo logging** — every mutation is a `RedoRecord` with LSN, page ID, offset, and payload
- **Per-page chains** — each record's `prev_lsn` links to the prior record for the same page, enabling efficient materialization without full WAL scans
- **Mini-transactions (MTR)** — atomic groups of redo records; the last record is marked as the Consistency Point LSN (CPL)
- **Durability watermarks** — VCL (Volume Complete LSN) and VDL (Volume Durable LSN) track what's safely persisted
- **On-demand materialization** — pages are built by collecting the redo chain and replaying records onto a zeroed page
- **Versioned reads** — read a page at any past LSN by truncating the chain
- **Crash recovery** — scan WAL, compute VCL/VDL, truncate incomplete MTRs, rebuild indexes

## Running

```bash
# Standard demo (scripted write/read/versioning scenario)
cargo run -- demo

# Interactive REPL
cargo run -- repl

# Visualization demo — step-by-step walkthrough of every internal operation
cargo run -- viz-demo

# Visualization with options
cargo run -- viz-demo --delay 0          # instant (no pause between steps)
cargo run -- viz-demo --delay 1000       # 1 second between steps
cargo run -- viz-demo --no-color         # plain text, no ANSI codes

# Interactive REPL with visualization
cargo run -- viz-repl
# Additional REPL commands: viz on|off, delay <ms>
```

### Viz mode

The `viz-demo` and `viz-repl` modes render an ASCII system diagram and walk through each internal operation step-by-step:

- **PUT path**: MTR creation, LSN assignment, prev_lsn linking, WAL append + fsync, index updates, VCL/VDL advancement, buffer pool invalidation
- **GET path**: Buffer pool lookup, page cache lookup, page index lookup, chain walk (per-link), materialization (per-record), cache inserts

After each operation, a full system state diagram is displayed showing compute state, storage state, and WAL contents.

## Tests

```bash
cargo test --workspace
```

50 tests covering WAL read/write, crash recovery, page materialization, cache behavior, compute transactions, storage engine integration, versioned reads, and multi-page atomicity.
