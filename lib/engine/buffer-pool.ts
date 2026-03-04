// Compute-side buffer pool -- port of crates/compute/src/buffer_pool.rs

import type { Lsn, PageId } from "./types"

interface CachedPage {
  page: Uint8Array
  readPoint: Lsn
  accessCount: number
}

/**
 * Local buffer pool on the compute side.
 * Caches pages read from storage at specific read-point LSNs.
 */
export class BufferPool {
  private pages: Map<PageId, CachedPage> = new Map()
  private capacity: number

  constructor(capacity: number) {
    this.capacity = capacity
  }

  /** Get a page if it exists and its readPoint >= minLsn. */
  get(pageId: PageId, minLsn: Lsn): Uint8Array | undefined {
    const entry = this.pages.get(pageId)
    if (entry && entry.readPoint >= minLsn) {
      entry.accessCount++
      return entry.page
    }
    return undefined
  }

  /** Insert or replace a page. Evicts least-accessed entry if at capacity. */
  insert(pageId: PageId, readPoint: Lsn, page: Uint8Array): void {
    if (this.pages.size >= this.capacity && !this.pages.has(pageId)) {
      // Evict least accessed
      let minKey: PageId | undefined
      let minAccess = Infinity
      for (const [k, v] of this.pages) {
        if (v.accessCount < minAccess) {
          minAccess = v.accessCount
          minKey = k
        }
      }
      if (minKey !== undefined) {
        this.pages.delete(minKey)
      }
    }
    this.pages.set(pageId, { page, readPoint, accessCount: 1 })
  }

  /** Invalidate a page (e.g., after writing to it). */
  invalidate(pageId: PageId): void {
    this.pages.delete(pageId)
  }

  len(): number {
    return this.pages.size
  }

  /** Return all page IDs currently in the buffer pool. */
  pageIds(): PageId[] {
    return Array.from(this.pages.keys())
  }
}
