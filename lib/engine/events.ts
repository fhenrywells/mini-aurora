// VizEvent -- direct port of src/viz/events.rs + src/viz/metrics.rs

import type { Lsn, PageId } from "./types"

/** Every discrete internal operation that the visualization can display. */
export type VizEvent =
  // PUT path
  | { type: "MtrCreated"; mtrId: number; numRecords: number }
  | { type: "AssignLsns"; firstLsn: Lsn; lastLsn: Lsn }
  | { type: "LinkPrevLsn"; lsn: Lsn; pageId: PageId; prevLsn: Lsn }
  | { type: "WalAppend"; firstLsn: Lsn; lastLsn: Lsn; offset: number; bytes: number }
  | { type: "WalSync" }
  | { type: "UpdatePageIndex"; pageId: PageId; latestLsn: Lsn }
  | { type: "UpdateLsnOffset"; lsn: Lsn; fileOffset: number }
  | { type: "AdvanceVcl"; old: Lsn; new: Lsn }
  | { type: "AdvanceVdl"; old: Lsn; new: Lsn }
  | { type: "UpdateReadPoint"; old: Lsn; new: Lsn }
  | { type: "BufferPoolInvalidate"; pageId: PageId }
  // GET path
  | { type: "BufferPoolLookup"; pageId: PageId; readPoint: Lsn; hit: boolean }
  | { type: "PageCacheLookup"; pageId: PageId; readPoint: Lsn; hit: boolean }
  | { type: "PageIndexLookup"; pageId: PageId; latestLsn: Lsn | null }
  | { type: "ChainWalkStep"; pageId: PageId; lsn: Lsn; prevLsn: Lsn; skipped: boolean }
  | { type: "ChainCollected"; pageId: PageId; chainLen: number; lsns: Lsn[] }
  | {
      type: "MaterializeApply"
      pageId: PageId
      lsn: Lsn
      offset: number
      dataLen: number
      dataPreview: string
    }
  | { type: "MaterializeComplete"; pageId: PageId; readPoint: Lsn }
  | { type: "PageCacheInsert"; pageId: PageId; readPoint: Lsn }
  | { type: "BufferPoolInsert"; pageId: PageId; readPoint: Lsn }
  // State snapshot
  | {
      type: "StateSnapshot"
      nodeLabel: string
      readPoint: Lsn
      nextMtr: number
      bufferPoolPages: PageId[]
      nextLsn: Lsn
      vcl: Lsn
      vdl: Lsn
      pageIndex: Map<PageId, Lsn>
      lsnOffsetCount: number
      pageCacheCount: number
      walByteSize: number
      walLsnRange: [Lsn, Lsn] | null
    }

/** Format an event to a compact one-liner (matches renderer.rs format_one_liner). */
export function formatOneLiner(event: VizEvent): string {
  switch (event.type) {
    case "MtrCreated":
      return `MTR #${event.mtrId} (${event.numRecords} record)`
    case "AssignLsns":
      return event.firstLsn === event.lastLsn
        ? `Assign LSN ${event.firstLsn}`
        : `Assign LSNs ${event.firstLsn}..${event.lastLsn}`
    case "LinkPrevLsn":
      return event.prevLsn === 0
        ? `Link L${event.lsn}(pg${event.pageId}) prev=none`
        : `Link L${event.lsn}(pg${event.pageId}) prev=L${event.prevLsn}`
    case "WalAppend":
      return event.firstLsn === event.lastLsn
        ? `WAL append L${event.firstLsn} @${event.offset} (${event.bytes}B)`
        : `WAL append L${event.firstLsn}..${event.lastLsn} @${event.offset} (${event.bytes}B)`
    case "WalSync":
      return "WAL sync (fsync)"
    case "UpdatePageIndex":
      return `Page index pg${event.pageId}->L${event.latestLsn}`
    case "UpdateLsnOffset":
      return `LSN offset L${event.lsn}->${event.fileOffset}`
    case "AdvanceVcl":
      return `VCL: ${event.old}->${event.new}`
    case "AdvanceVdl":
      return `VDL: ${event.old}->${event.new} (CPL)`
    case "UpdateReadPoint":
      return `read_point: ${event.old}->${event.new}`
    case "BufferPoolInvalidate":
      return `Evict pg${event.pageId} from buffer`
    case "BufferPoolLookup": {
      const tag = event.hit ? "HIT" : "MISS"
      return `BufPool pg${event.pageId} @L${event.readPoint}: ${tag}`
    }
    case "PageCacheLookup": {
      const tag = event.hit ? "HIT" : "MISS"
      return `PageCache pg${event.pageId} @L${event.readPoint}: ${tag}`
    }
    case "PageIndexLookup":
      return event.latestLsn !== null
        ? `PageIdx pg${event.pageId}->L${event.latestLsn}`
        : `PageIdx pg${event.pageId}: not found`
    case "ChainWalkStep":
      return event.skipped ? `Chain: skip L${event.lsn}` : `Chain: collect L${event.lsn}`
    case "ChainCollected": {
      const chain = event.lsns.map((l) => `L${l}`).join("->")
      return `Chain pg${event.pageId}: ${event.chainLen} rec [${chain}]`
    }
    case "MaterializeApply":
      return `Apply L${event.lsn} ${event.dataLen}B @${event.offset}`
    case "MaterializeComplete":
      return `Materialized pg${event.pageId} @L${event.readPoint}`
    case "PageCacheInsert":
      return `Cache pg${event.pageId} @L${event.readPoint}`
    case "BufferPoolInsert":
      return `BufPool insert pg${event.pageId} @L${event.readPoint}`
    case "StateSnapshot":
      return ""
  }
}

/** Aggregate metrics collector -- port of src/viz/metrics.rs. */
export interface MetricsSummary {
  writeCount: number
  readCount: number
  pageCacheHits: number
  pageCacheMisses: number
  bufferPoolHits: number
  bufferPoolMisses: number
  materializeCount: number
  walBytesWritten: number
  vcl: number
  vdl: number
  uptimeSecs: number
}

export class MetricsCollector {
  private writeCount = 0
  private readCount = 0
  private pageCacheHits = 0
  private pageCacheMisses = 0
  private bufferPoolHits = 0
  private bufferPoolMisses = 0
  private materializeCount = 0
  private walBytesWritten = 0
  private vcl = 0
  private vdl = 0
  private startTime = Date.now()

  recordEvent(event: VizEvent): void {
    switch (event.type) {
      case "WalAppend":
        this.writeCount++
        this.walBytesWritten += event.bytes
        break
      case "PageCacheLookup":
        if (event.hit) this.pageCacheHits++
        else this.pageCacheMisses++
        break
      case "BufferPoolLookup":
        if (event.hit) this.bufferPoolHits++
        else {
          this.bufferPoolMisses++
          this.readCount++
        }
        break
      case "MaterializeComplete":
        this.materializeCount++
        break
      case "AdvanceVcl":
        this.vcl = event.new
        break
      case "AdvanceVdl":
        this.vdl = event.new
        break
    }
  }

  summary(): MetricsSummary {
    return {
      writeCount: this.writeCount,
      readCount: this.readCount,
      pageCacheHits: this.pageCacheHits,
      pageCacheMisses: this.pageCacheMisses,
      bufferPoolHits: this.bufferPoolHits,
      bufferPoolMisses: this.bufferPoolMisses,
      materializeCount: this.materializeCount,
      walBytesWritten: this.walBytesWritten,
      vcl: this.vcl,
      vdl: this.vdl,
      uptimeSecs: (Date.now() - this.startTime) / 1000,
    }
  }

  reset(): void {
    this.writeCount = 0
    this.readCount = 0
    this.pageCacheHits = 0
    this.pageCacheMisses = 0
    this.bufferPoolHits = 0
    this.bufferPoolMisses = 0
    this.materializeCount = 0
    this.walBytesWritten = 0
    this.vcl = 0
    this.vdl = 0
    this.startTime = Date.now()
  }
}
