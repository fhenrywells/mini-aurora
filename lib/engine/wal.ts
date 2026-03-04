// In-memory WAL -- replaces the file-based WalWriter/WalReader/SegmentManager

import type { Lsn, RedoRecord } from "./types"

const LOG_ENTRY_HEADER_SIZE = 41

export interface WalEntry {
  record: RedoRecord
  offset: number
}

/**
 * In-memory WAL replacing the file-based implementation.
 * All records are stored in an array; offsets are simulated.
 */
export class InMemoryWal {
  private entries: WalEntry[] = []
  private lsnIndex: Map<Lsn, number> = new Map() // lsn -> index in entries[]
  private totalBytesWritten = 0

  /** Append a batch of records to the WAL. Returns the file offset at which they were written. */
  append(records: RedoRecord[]): number {
    const startOffset = this.totalBytesWritten
    for (const record of records) {
      const entrySize = LOG_ENTRY_HEADER_SIZE + record.data.length
      this.entries.push({ record, offset: this.totalBytesWritten })
      this.lsnIndex.set(record.lsn, this.entries.length - 1)
      this.totalBytesWritten += entrySize
    }
    return startOffset
  }

  /** Look up a WAL entry by LSN. */
  getByLsn(lsn: Lsn): WalEntry | undefined {
    const idx = this.lsnIndex.get(lsn)
    if (idx === undefined) return undefined
    return this.entries[idx]
  }

  /** Get the simulated total bytes written. */
  totalBytes(): number {
    return this.totalBytesWritten
  }

  /** Get the number of entries. */
  entryCount(): number {
    return this.entries.length
  }

  /** Get the file offset for a given LSN. */
  getOffset(lsn: Lsn): number | undefined {
    const entry = this.getByLsn(lsn)
    return entry?.offset
  }
}
