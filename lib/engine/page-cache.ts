// LRU page cache -- port of crates/pagestore/src/page_cache.rs

import type { Lsn, PageId } from "./types"

/**
 * LRU page cache using a Map (JS Maps iterate in insertion order).
 * Key: "pageId:lsn"
 */
export class PageCache {
  private cache: Map<string, Uint8Array> = new Map()
  private maxEntries: number

  constructor(maxEntries: number) {
    this.maxEntries = maxEntries
  }

  private key(pageId: PageId, lsn: Lsn): string {
    return `${pageId}:${lsn}`
  }

  get(pageId: PageId, lsn: Lsn): Uint8Array | undefined {
    const k = this.key(pageId, lsn)
    const page = this.cache.get(k)
    if (page) {
      // Move to end (most recently used)
      this.cache.delete(k)
      this.cache.set(k, page)
    }
    return page
  }

  insert(pageId: PageId, lsn: Lsn, page: Uint8Array): void {
    const k = this.key(pageId, lsn)
    // Remove if exists (will re-insert at end)
    this.cache.delete(k)
    // Evict oldest if at capacity
    if (this.cache.size >= this.maxEntries) {
      const firstKey = this.cache.keys().next().value
      if (firstKey !== undefined) {
        this.cache.delete(firstKey)
      }
    }
    this.cache.set(k, page)
  }

  len(): number {
    return this.cache.size
  }
}
