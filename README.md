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

# Interactive REPL with visualization (multi-node, suggestions, bg workers)
cargo run -- viz-repl
cargo run -- viz-repl --delay 300
cargo run -- viz-repl --no-color
```

### Viz REPL

The viz-repl is a full interactive concurrency demo with two compute nodes, contextual suggestions, and background workers.

#### Commands

```
put <page> <offset> <text>          Write to a page
get <page>                          Read a page
refresh                             Advance read_point to latest VDL
node A|B                            Switch active compute node
state                               Show durability watermarks
bg <node> write|read|mixed <ms>     Start background worker
bg stop <node>                      Stop background worker
bg list                             Show running workers
viz on|off                          Toggle visualization
delay <ms>                          Set step delay
1, 2, 3                             Run suggested command
quit                                Exit
```

#### Multi-node

Two compute nodes (A and B) share a single storage engine. Each has its own buffer pool and read point. This demonstrates Aurora's read isolation — Node B won't see Node A's writes until it refreshes its read point.

```
A> put 1 0 Hello
A> node B
B> get 1            # fails — Node B's read_point is 0
B> refresh          # advance read_point to VDL
B> get 1            # succeeds — sees "Hello"
```

#### Suggestions

After each command, numbered shortcuts are displayed. Type `1`, `2`, or `3` to run a suggestion:

```
A> put 1 0 Hello
  [1] get 1                 — read the page
  [2] put 1 0 updated       — overwrite with new data
  [3] node B                — switch compute node
A> 1
>>> get 1
"Hello"
```

#### Background workers

Spawn concurrent workers to visualize interleaved operations in real time:

```
A> bg A write 500           # Node A writes a new page every 500ms
A> bg B mixed 500           # Node B alternates refresh/get every 500ms
A> bg list                  # show running workers
A> bg stop A
A> bg stop B
```

Worker kinds:
- **write** — PUT sequential pages (pg100, pg101, ...) with auto-incrementing IDs
- **read** — GET cycling through pages 1–10
- **mixed** — alternates `refresh` and `get`, demonstrating read isolation under concurrent writes

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
