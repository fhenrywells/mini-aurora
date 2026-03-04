// Storage engine -- port of crates/storage/src/engine.rs + src/viz/engine.rs
// Combines both into one class that emits VizEvents at each step.

import type { VizEvent } from "./events"
import { PageCache } from "./page-cache"
import {
  type DurabilityState,
  type Lsn,
  type PageId,
  PAGE_SIZE,
  type RedoRecord,
  StorageError,
  emptyPage,
} from "./types"
import { InMemoryWal } from "./wal"

export type EventCallback = (event: VizEvent) => void

function dataPreview(data: Uint8Array, maxLen: number): string {
  const slice = data.slice(0, maxLen)
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(slice)
    return data.length > maxLen ? `${text}...` : text
  } catch {
    const hex = Array.from(slice)
      .map((b) => b.toString(16).padStart(2, "0").toUpperCase())
      .join("")
    return data.length > maxLen ? `0x${hex}...` : `0x${hex}`
  }
}

/**
 * Storage engine combining WAL + page store.
 * Emits VizEvents at every internal step for visualization.
 */
export class StorageEngine {
  private wal: InMemoryWal
  private pageIndex: Map<PageId, Lsn> = new Map()
  private lsnOffsets: Map<Lsn, number> = new Map()
  private nextLsn: Lsn = 1
  private durability: DurabilityState = { vcl: 0, vdl: 0 }
  private pageCache: PageCache
  private onEvent: EventCallback

  constructor(onEvent: EventCallback) {
    this.wal = new InMemoryWal()
    this.pageCache = new PageCache(1024)
    this.onEvent = onEvent
  }

  private emit(event: VizEvent): void {
    this.onEvent(event)
  }

  /** Append a batch of redo records. Returns the new durable LSN. */
  appendRedo(records: RedoRecord[]): Lsn {
    const firstLsn = this.nextLsn

    // Step: Assign LSNs
    for (const record of records) {
      record.lsn = this.nextLsn
      this.nextLsn++
    }
    const lastLsn = this.nextLsn - 1

    this.emit({ type: "AssignLsns", firstLsn, lastLsn })

    // Step: Link prev_lsn chains
    for (const record of records) {
      record.prevLsn = this.pageIndex.get(record.pageId) ?? 0
      this.emit({
        type: "LinkPrevLsn",
        lsn: record.lsn,
        pageId: record.pageId,
        prevLsn: record.prevLsn,
      })
    }

    // Step: WAL append
    const walOffset = this.wal.totalBytes()
    const startOffset = this.wal.append(records)
    const totalBytes = this.wal.totalBytes() - walOffset

    this.emit({
      type: "WalAppend",
      firstLsn,
      lastLsn,
      offset: startOffset,
      bytes: totalBytes,
    })

    // Step: WAL sync
    this.emit({ type: "WalSync" })

    // Step: Record LSN offsets
    for (const record of records) {
      const offset = this.wal.getOffset(record.lsn) ?? 0
      this.lsnOffsets.set(record.lsn, offset)
      this.emit({ type: "UpdateLsnOffset", lsn: record.lsn, fileOffset: offset })
    }

    // Step: Update page index
    for (const record of records) {
      const current = this.pageIndex.get(record.pageId) ?? 0
      if (record.lsn > current) {
        this.pageIndex.set(record.pageId, record.lsn)
      }
      this.emit({
        type: "UpdatePageIndex",
        pageId: record.pageId,
        latestLsn: record.lsn,
      })
    }

    // Step: Advance VCL
    const oldVcl = this.durability.vcl
    const highestLsn = records.length > 0 ? records[records.length - 1].lsn : this.durability.vcl
    this.durability.vcl = highestLsn
    this.emit({ type: "AdvanceVcl", old: oldVcl, new: this.durability.vcl })

    // Step: Advance VDL
    const oldVdl = this.durability.vdl
    for (let i = records.length - 1; i >= 0; i--) {
      if (records[i].isMtrEnd && records[i].lsn > this.durability.vdl) {
        this.durability.vdl = records[i].lsn
        break
      }
    }
    this.emit({ type: "AdvanceVdl", old: oldVdl, new: this.durability.vdl })

    return this.durability.vdl
  }

  /** Read a page materialized up to the given read-point LSN. */
  getPage(pageId: PageId, readPoint: Lsn): Uint8Array {
    // Check read_point doesn't exceed VDL
    if (readPoint > this.durability.vdl) {
      throw new StorageError(
        "lsn_beyond_durable",
        `requested LSN ${readPoint} exceeds durable LSN ${this.durability.vdl}`
      )
    }

    // Step: Page cache lookup
    const cached = this.pageCache.get(pageId, readPoint)
    if (cached) {
      this.emit({ type: "PageCacheLookup", pageId, readPoint, hit: true })
      return cached
    }
    this.emit({ type: "PageCacheLookup", pageId, readPoint, hit: false })

    // Step: Page index lookup
    const latestLsn = this.pageIndex.get(pageId) ?? 0
    this.emit({
      type: "PageIndexLookup",
      pageId,
      latestLsn: latestLsn === 0 ? null : latestLsn,
    })

    if (latestLsn === 0) {
      throw new StorageError("page_not_found", `page ${pageId} not found at or below LSN ${readPoint}`)
    }

    // Step: Chain walk (backwards through WAL via prev_lsn)
    const chain: RedoRecord[] = []
    let currentLsn = latestLsn

    while (currentLsn !== 0) {
      const entry = this.wal.getByLsn(currentLsn)
      if (!entry) break

      if (currentLsn > readPoint) {
        // Skip this record (beyond read point)
        this.emit({
          type: "ChainWalkStep",
          pageId,
          lsn: currentLsn,
          prevLsn: entry.record.prevLsn,
          skipped: true,
        })
        currentLsn = entry.record.prevLsn
        continue
      }

      this.emit({
        type: "ChainWalkStep",
        pageId,
        lsn: currentLsn,
        prevLsn: entry.record.prevLsn,
        skipped: false,
      })
      chain.push(entry.record)
      currentLsn = entry.record.prevLsn
    }

    chain.reverse() // oldest first for replay

    if (chain.length === 0) {
      throw new StorageError("page_not_found", `page ${pageId} not found at or below LSN ${readPoint}`)
    }

    this.emit({
      type: "ChainCollected",
      pageId,
      chainLen: chain.length,
      lsns: chain.map((r) => r.lsn),
    })

    // Step: Materialize page
    const page = emptyPage()
    for (const record of chain) {
      const start = record.offset
      const end = start + record.data.length
      if (end > PAGE_SIZE) {
        throw new StorageError("page_overflow", `redo record data overflows page: offset=${record.offset} len=${record.data.length}`)
      }
      page.set(record.data, start)

      this.emit({
        type: "MaterializeApply",
        pageId,
        lsn: record.lsn,
        offset: record.offset,
        dataLen: record.data.length,
        dataPreview: dataPreview(record.data, 20),
      })
    }

    this.emit({ type: "MaterializeComplete", pageId, readPoint })

    // Step: Cache the result
    this.pageCache.insert(pageId, readPoint, page)
    this.emit({ type: "PageCacheInsert", pageId, readPoint })

    return page
  }

  /** Get current durability state. */
  getDurabilityState(): DurabilityState {
    return { ...this.durability }
  }

  /** Return all page IDs that currently have durable entries. */
  durablePageIds(): PageId[] {
    return Array.from(this.pageIndex.keys())
  }

  /** Build and emit a state snapshot. */
  emitStateSnapshot(
    nodeLabel: string,
    readPoint: Lsn,
    nextMtr: number,
    bufferPoolPages: PageId[]
  ): void {
    this.emit({
      type: "StateSnapshot",
      nodeLabel,
      readPoint,
      nextMtr,
      bufferPoolPages,
      nextLsn: this.nextLsn,
      vcl: this.durability.vcl,
      vdl: this.durability.vdl,
      pageIndex: new Map(this.pageIndex),
      lsnOffsetCount: this.lsnOffsets.size,
      pageCacheCount: this.pageCache.len(),
      walByteSize: this.wal.totalBytes(),
      walLsnRange: this.nextLsn > 1 ? [1, this.nextLsn - 1] : null,
    })
  }

  /** Get next LSN (useful for display). */
  getNextLsn(): Lsn {
    return this.nextLsn
  }

  /** Get the page index (useful for display). */
  getPageIndex(): Map<PageId, Lsn> {
    return new Map(this.pageIndex)
  }

  /** Get WAL byte count. */
  getWalBytes(): number {
    return this.wal.totalBytes()
  }

  /** Get page cache count. */
  getPageCacheCount(): number {
    return this.pageCache.len()
  }

  /** Get LSN offset count. */
  getLsnOffsetCount(): number {
    return this.lsnOffsets.size
  }
}
