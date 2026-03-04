// Core types -- direct port of crates/common/src/lib.rs

/** Monotonically increasing log sequence number (1-based; 0 means "no LSN"). */
export type Lsn = number

/** Logical page identifier. */
export type PageId = number

/** 8 KiB page size (matches typical database page size). */
export const PAGE_SIZE = 8192

/** Create a zeroed page (blank slate for materialization). */
export function emptyPage(): Uint8Array {
  return new Uint8Array(PAGE_SIZE)
}

/** Physical redo record -- the unit of change in Aurora's "log is the database" model. */
export interface RedoRecord {
  /** Log sequence number assigned at write time. */
  lsn: Lsn
  /** Which page this record modifies. */
  pageId: PageId
  /** Byte offset within the page where data should be written. */
  offset: number
  /** Payload -- the bytes to write at offset. */
  data: Uint8Array
  /** Previous LSN that touched this same page (forms a per-page chain). */
  prevLsn: Lsn
  /** Mini-transaction group identifier. */
  mtrId: number
  /** When true, this record is the Consistency Point LSN (CPL) of its MTR. */
  isMtrEnd: boolean
}

/** Aurora-style LSN watermarks that track durability progress. */
export interface DurabilityState {
  /** Volume Complete LSN -- highest LSN where all prior LSNs are present. */
  vcl: Lsn
  /** Volume Durable LSN -- highest CPL (MTR-end) whose LSN <= VCL. */
  vdl: Lsn
}

export type StorageErrorKind =
  | "page_not_found"
  | "lsn_beyond_durable"
  | "page_overflow"
  | "other"

export class StorageError extends Error {
  kind: StorageErrorKind

  constructor(kind: StorageErrorKind, message: string) {
    super(message)
    this.kind = kind
    this.name = "StorageError"
  }
}
