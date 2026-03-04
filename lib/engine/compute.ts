// Compute engine -- port of src/viz/compute.rs
// Generates redo records and sends them to storage. Maintains a local buffer pool.

import { BufferPool } from "./buffer-pool"
import type { VizEvent } from "./events"
import { type EventCallback, StorageEngine } from "./storage"
import { type Lsn, type PageId, PAGE_SIZE, type RedoRecord, StorageError } from "./types"

/**
 * Compute engine -- the "SQL layer" in Aurora's architecture.
 * Generates redo records and sends them to storage.
 */
export class ComputeEngine {
  private storage: StorageEngine
  private bufferPool: BufferPool
  private nextMtrId = 1
  private readPoint: Lsn = 0
  private onEvent: EventCallback
  readonly label: string

  constructor(storage: StorageEngine, bufferPoolCapacity: number, onEvent: EventCallback, label: string) {
    this.storage = storage
    this.bufferPool = new BufferPool(bufferPoolCapacity)
    this.onEvent = onEvent
    this.label = label
  }

  private emit(event: VizEvent): void {
    this.onEvent(event)
  }

  /** Write bytes to a page at a given offset. Single-record MTR. */
  put(pageId: PageId, offset: number, data: Uint8Array): Lsn {
    if (offset + data.length > PAGE_SIZE) {
      throw new StorageError("page_overflow", `redo record data overflows page: offset=${offset} len=${data.length}`)
    }

    const mtrId = this.nextMtrId++

    // Step: Create MTR
    const record: RedoRecord = {
      lsn: 0,
      pageId,
      offset,
      data,
      prevLsn: 0,
      mtrId,
      isMtrEnd: true,
    }

    this.emit({ type: "MtrCreated", mtrId, numRecords: 1 })

    // StorageEngine handles its own event emissions
    const vdl = this.storage.appendRedo([record])

    // Step: Invalidate buffer pool + update read point
    const oldReadPoint = this.readPoint
    this.readPoint = vdl
    this.bufferPool.invalidate(pageId)

    this.emit({ type: "BufferPoolInvalidate", pageId })
    this.emit({ type: "UpdateReadPoint", old: oldReadPoint, new: vdl })

    // Show final state
    this.emitStateSnapshot()

    return vdl
  }

  /** Execute a multi-record mini-transaction. */
  putMulti(writes: Array<{ pageId: PageId; offset: number; data: Uint8Array }>): Lsn {
    for (const w of writes) {
      if (w.offset + w.data.length > PAGE_SIZE) {
        throw new StorageError("page_overflow", `redo record data overflows page: offset=${w.offset} len=${w.data.length}`)
      }
    }

    if (writes.length === 0) return this.readPoint

    const mtrId = this.nextMtrId++
    const records: RedoRecord[] = writes.map((w, i) => ({
      lsn: 0,
      pageId: w.pageId,
      offset: w.offset,
      data: w.data,
      prevLsn: 0,
      mtrId,
      isMtrEnd: i === writes.length - 1,
    }))

    this.emit({ type: "MtrCreated", mtrId, numRecords: records.length })

    const vdl = this.storage.appendRedo(records)

    const oldReadPoint = this.readPoint
    this.readPoint = vdl
    for (const w of writes) {
      this.bufferPool.invalidate(w.pageId)
      this.emit({ type: "BufferPoolInvalidate", pageId: w.pageId })
    }
    this.emit({ type: "UpdateReadPoint", old: oldReadPoint, new: vdl })

    this.emitStateSnapshot()

    return vdl
  }

  /** Read a page at the current read point. */
  get(pageId: PageId): Uint8Array {
    const readPoint = this.readPoint

    // Step: Buffer pool lookup
    const buffered = this.bufferPool.get(pageId, readPoint)
    if (buffered) {
      this.emit({ type: "BufferPoolLookup", pageId, readPoint, hit: true })
      return buffered
    }
    this.emit({ type: "BufferPoolLookup", pageId, readPoint, hit: false })

    // Fetch from storage (StorageEngine emits its own events)
    const page = this.storage.getPage(pageId, readPoint)

    // Step: Buffer pool insert
    this.bufferPool.insert(pageId, readPoint, page)
    this.emit({ type: "BufferPoolInsert", pageId, readPoint })

    this.emitStateSnapshot()

    return page
  }

  /** Get the current read point. */
  getReadPoint(): Lsn {
    return this.readPoint
  }

  /** Get next MTR id. */
  getNextMtrId(): number {
    return this.nextMtrId
  }

  /** Get buffer pool page IDs. */
  getBufferPoolPageIds(): PageId[] {
    return this.bufferPool.pageIds()
  }

  /** Refresh read point from storage's durability state. */
  refreshReadPoint(): Lsn {
    const state = this.storage.getDurabilityState()
    const old = this.readPoint
    this.readPoint = state.vdl

    this.emit({ type: "UpdateReadPoint", old, new: state.vdl })
    this.emitStateSnapshot()

    return state.vdl
  }

  private emitStateSnapshot(): void {
    this.storage.emitStateSnapshot(
      this.label,
      this.readPoint,
      this.nextMtrId,
      this.bufferPool.pageIds()
    )
  }
}
