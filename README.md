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

## Quick Start

```bash
# Scripted and interactive basics
cargo run -- demo
cargo run -- repl

# Step-by-step visualization and full concurrency REPL
cargo run -- viz-demo
cargo run -- viz-repl
```

## Viz REPL

The viz-repl is a full interactive concurrency demo with two compute nodes, contextual suggestions, and background workers.

### Commands

```
put <page> <offset> <text>          Write to a page
get <page>                          Read a page
refresh                             Advance read_point to latest VDL
node A|B                            Switch active compute node
state                               Show durability watermarks
metrics                             Print operation counts and latencies
bg <node> write|read|mixed <ms>     Start background worker
bg stop <node>                      Stop background worker
bg list                             Show running workers
viz on|off                          Toggle visualization
delay <ms>                          Set step delay
1, 2, 3                             Run suggested command
quit                                Exit
```

### Multi-node

Two compute nodes (A and B) share a single storage engine. Each has its own buffer pool and read point. Node B won't see Node A's writes until it refreshes its read point — this is Aurora's read isolation.

```
A> put 1 0 Hello
A> node B
B> get 1            # fails — Node B's read_point is 0
B> refresh          # advance read_point to VDL
B> get 1            # succeeds — sees "Hello"
```

### Suggestions

After each command, numbered shortcuts are displayed. Type `1`, `2`, or `3` to run one:

```
A> put 1 0 Hello
  [1] get 1                 — read the page
  [2] put 1 0 updated       — overwrite with new data
  [3] node B                — switch compute node
A> 1
>>> get 1
"Hello"
```

### Background workers

Spawn concurrent workers to visualize interleaved operations:

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

## Scenarios

The scenario runner executes a TOML file of put/get/refresh/sleep/repeat steps against the two-node engine and prints metrics at the end.

```bash
cargo run -- scenario scenarios/burst.toml
```

### Included scenarios

List them from the CLI:

```bash
cargo run -- scenario --list
```

| File | What it tests |
|------|---------------|
| `scenarios/burst.toml` | 50 rapid writes then reads — WAL throughput and cache churn |
| `scenarios/cold_reads.toml` | Repeated reads across a mixed page set — cache miss/hit behavior |
| `scenarios/noisy_neighbor.toml` | Node A does heavy writes while Node B reads with a stale read point |
| `scenarios/tiered_demo.toml` | Fills segments to trigger rotation, reads across hot and cold tiers |

Scenarios accept the same `--preset` and `--trace-json` flags as the viz-repl:

```bash
cargo run -- scenario scenarios/tiered_demo.toml --preset tiered --trace-json /tmp/trace.json
```

### Writing your own

A scenario file has a `[meta]` section and a list of `[[steps]]`:

```toml
[meta]
name = "My scenario"
description = "What this scenario demonstrates"

[[steps]]
op = "put"
page_id = 1
offset = 0
data = "hello"
node = "A"          # optional, defaults to "A"

[[steps]]
op = "get"
page_id = 1
node = "B"

[[steps]]
op = "refresh"
node = "B"

[[steps]]
op = "sleep_ms"
value = 100

[[steps]]
op = "repeat"
count = 10
steps = [
    { op = "put", page_id = 1, offset = 0, data = "loop" },
]
```

## Side-by-Side Comparison

The `compare` subcommand runs the **same scenario against both backends** (BASE and TIERED) headlessly and prints a unified 84-character-wide comparison table. Step results are aligned row-by-row; a `←` marker appears on any row where the two values differ.

```bash
cargo run -- compare scenarios/tiered_demo.toml --segment-size 512
```

Example output:

```
=== Compare: Tiered Storage Demo ===
Running BASE    ... done  (120 appends, 0.4s, all data on fast local storage)
Running TIERED  ... done  (120 appends, 2.3s total, 1700ms cold overhead, 10 segs sealed → cold storage)
╔══════════════════════════════════════════╤════════════════════╤════════════════════════╗
║ Step                                     │ BASE               │ TIERED                 ║
╠══════════════════════════════════════════╪════════════════════╪════════════════════════╣
║ [A] PUT pg1 @0                           │ VDL=1              │ VDL=1                  ║
║ repeat 10× (put pg1, put pg2)            │ (done)             │ (done)                 ║
║ ...                                      │ ...                │ ...                    ║
╠══════════════════════════════════════════╪════════════════════╪════════════════════════╣
║ WAL appends                              │ 120                │ 120                    ║
║ Segment rotations                        │ —                  │ 12                   ← ║
║ Segments cooled                          │ —                  │ 10                   ← ║
║ Cold tier reads                          │ —                  │ 34                   ← ║
║ Cache hit rate                           │ 72%                │ 72%                    ║
╚══════════════════════════════════════════╧════════════════════╧════════════════════════╝
```

Step rows with `←` indicate a correctness divergence — both backends should produce identical results for the same operations. The tiered-only metrics rows (`Segments sealed`, `Segments on cold storage`, `Cold storage reads`, `Cold read overhead`) are expected to show `←`: sealed segments represent data evicted to cheaper storage, and the cold read overhead shows exactly where the extra elapsed time went — not write performance, but simulated object-storage read latency.

`compare` always runs both backends; `--preset` is not accepted. Tuning flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--segment-size <bytes>` | 4096 | WAL segment size (tiered run) |
| `--cold-latency-ms <ms>` | 50 | Artificial cold-read latency (tiered run) |

Use small segment sizes to force more rotations and make the difference more visible:

```bash
cargo run -- compare scenarios/cold_reads.toml --segment-size 512 --cold-latency-ms 10
```

---

## Metrics & Tracing

### In-session metrics

Type `metrics` in the viz-repl (or run a scenario — metrics print automatically at the end) to see operation counts and latencies.

### JSON tracing

Pass `--trace-json <path>` to write every internal event (WAL append, chain walk, cache hit/miss, materialization, VCL/VDL advance) as newline-delimited JSON:

```bash
cargo run -- viz-repl --trace-json /tmp/trace.json
# or
cargo run -- scenario scenarios/burst.toml --trace-json /tmp/trace.json
```

Each line is a JSON object with a `kind`, timestamp, and event-specific fields — useful for post-hoc analysis with `jq`, pandas, or any JSON tooling.

## Storage Variants

### Base (default)

Single-file WAL. This is what runs when you don't pass `--preset`:

```bash
cargo run -- viz-repl                     # equivalent to --preset base
```

### Tiered

Segmented WAL with hot/cold tier simulation. Segments rotate at a configurable size; older segments are marked cold and reads from them incur artificial latency to mimic real tiered-storage behavior.

```bash
cargo run -- viz-repl --preset tiered
```

Tuning flags (tiered preset only):

| Flag | Default | Description |
|------|---------|-------------|
| `--segment-size <bytes>` | 4096 | Max bytes per WAL segment before rotation |
| `--cold-latency-ms <ms>` | 50 | Artificial read latency for cold segments |

Example with small segments and high cold latency:

```bash
cargo run -- viz-repl --preset tiered --segment-size 1024 --cold-latency-ms 200
```

Run the included tiered scenario to see segment rotation in action:

```bash
cargo run -- scenario scenarios/tiered_demo.toml --preset tiered --trace-json /tmp/tiered.json
```

## Tests

```bash
cargo test --workspace
```

63 tests covering WAL read/write, crash recovery, segmented WAL, page materialization, cache behavior, compute transactions, storage engine integration, versioned reads, and multi-page atomicity.

## Global Flags

| Flag | Applies to | Default | Description |
|------|-----------|---------|-------------|
| `--delay <ms>` | `viz-demo`, `viz-repl` | 300 | Pause between visualization steps |
| `--no-color` | `viz-demo`, `viz-repl` | off | Disable ANSI color codes |
| `--trace-json <path>` | `viz-repl`, `scenario` | — | Write events as newline-delimited JSON |
| `--preset base\|tiered` | `viz-repl`, `scenario` | `base` | Storage engine variant |
| `--segment-size <bytes>` | `viz-repl`, `scenario` (tiered), `compare` | 4096 | WAL segment size before rotation |
| `--cold-latency-ms <ms>` | `viz-repl`, `scenario` (tiered), `compare` | 50 | Artificial latency for cold segment reads |
