"use client"

import { useCallback, useRef, useState } from "react"
import { ComputeEngine } from "@/lib/engine/compute"
import {
  type VizEvent,
  MetricsCollector,
  formatOneLiner,
  type MetricsSummary,
} from "@/lib/engine/events"
import { StorageEngine } from "@/lib/engine/storage"
import type { Lsn, PageId } from "@/lib/engine/types"

export interface TimestampedEvent {
  id: number
  event: VizEvent
  text: string
  timestamp: number
}

export interface NodeState {
  label: string
  readPoint: Lsn
  nextMtr: number
  bufferPoolPages: PageId[]
}

export interface SharedState {
  nextLsn: Lsn
  vcl: Lsn
  vdl: Lsn
  pageIndex: Map<PageId, Lsn>
  lsnOffsetCount: number
  pageCacheCount: number
  walByteSize: number
  walLsnRange: [Lsn, Lsn] | null
}

export interface AuroraState {
  events: TimestampedEvent[]
  nodes: Map<string, NodeState>
  currentNode: string
  shared: SharedState
  metrics: MetricsSummary | null
  isRunning: boolean
  operationHeader: string
  output: string[]
}

function textEncoder() {
  return new TextEncoder()
}

export function useAurora() {
  const eventIdRef = useRef(0)
  const metricsRef = useRef(new MetricsCollector())
  // Pending events buffer -- filled synchronously by engine callbacks, flushed to state after commands
  const pendingEventsRef = useRef<TimestampedEvent[]>([])
  // Latest snapshot state (written synchronously by engine, read during flush)
  const latestNodesRef = useRef<Map<string, NodeState>>(
    new Map([
      ["A", { label: "A", readPoint: 0, nextMtr: 1, bufferPoolPages: [] }],
      ["B", { label: "B", readPoint: 0, nextMtr: 1, bufferPoolPages: [] }],
    ])
  )
  const latestSharedRef = useRef<SharedState>({
    nextLsn: 1,
    vcl: 0,
    vdl: 0,
    pageIndex: new Map(),
    lsnOffsetCount: 0,
    pageCacheCount: 0,
    walByteSize: 0,
    walLsnRange: null,
  })

  const [state, setState] = useState<AuroraState>({
    events: [],
    nodes: latestNodesRef.current,
    currentNode: "A",
    shared: latestSharedRef.current,
    metrics: null,
    isRunning: false,
    operationHeader: "",
    output: [],
  })

  const storageRef = useRef<StorageEngine | null>(null)
  const nodesRef = useRef<Map<string, ComputeEngine>>(new Map())
  const initializedRef = useRef(false)

  // The event callback -- called synchronously by the engine during operations.
  // Collects events into pendingEventsRef and updates snapshot refs.
  const onEventRef = useRef<(event: VizEvent) => void>((event: VizEvent) => {
    metricsRef.current.recordEvent(event)

    const text = formatOneLiner(event)
    if (text) {
      pendingEventsRef.current.push({
        id: eventIdRef.current++,
        event,
        text,
        timestamp: Date.now(),
      })
    }

    // Update snapshot refs synchronously for StateSnapshot events
    if (event.type === "StateSnapshot") {
      const nodes = new Map(latestNodesRef.current)
      nodes.set(event.nodeLabel, {
        label: event.nodeLabel,
        readPoint: event.readPoint,
        nextMtr: event.nextMtr,
        bufferPoolPages: [...event.bufferPoolPages],
      })
      latestNodesRef.current = nodes
      latestSharedRef.current = {
        nextLsn: event.nextLsn,
        vcl: event.vcl,
        vdl: event.vdl,
        pageIndex: new Map(event.pageIndex),
        lsnOffsetCount: event.lsnOffsetCount,
        pageCacheCount: event.pageCacheCount,
        walByteSize: event.walByteSize,
        walLsnRange: event.walLsnRange,
      }
    }
  })

  /** Flush pending events + snapshot state into React state. */
  const flushToState = useCallback(
    (extraUpdates?: Partial<AuroraState>) => {
      const newEvents = pendingEventsRef.current.splice(0) // drain
      setState((prev) => ({
        ...prev,
        events: [...prev.events, ...newEvents],
        nodes: latestNodesRef.current,
        shared: latestSharedRef.current,
        metrics: metricsRef.current.summary(),
        ...extraUpdates,
      }))
    },
    []
  )

  const ensureInitialized = useCallback(() => {
    if (initializedRef.current) return
    initializedRef.current = true

    const onEvent = onEventRef.current

    const storage = new StorageEngine(onEvent)
    storageRef.current = storage

    const nodeA = new ComputeEngine(storage, 256, onEvent, "A")
    const nodeB = new ComputeEngine(storage, 256, onEvent, "B")
    nodesRef.current.set("A", nodeA)
    nodesRef.current.set("B", nodeB)

    // Refresh read points (emits events synchronously)
    nodeA.refreshReadPoint()
    nodeB.refreshReadPoint()

    // Flush init events
    flushToState()
  }, [flushToState])

  // Read current node label from a ref to avoid stale closures
  const currentNodeRef = useRef("A")
  // Keep ref in sync with state
  if (state.currentNode !== currentNodeRef.current) {
    currentNodeRef.current = state.currentNode
  }

  const getNode = useCallback(
    (label?: string): ComputeEngine => {
      const nodeLabel = label ?? currentNodeRef.current
      const node = nodesRef.current.get(nodeLabel)
      if (!node) throw new Error(`Unknown node: ${nodeLabel}`)
      return node
    },
    []
  )

  const executeCommand = useCallback(
    (cmd: string): string => {
      ensureInitialized()

      const trimmed = cmd.trim()
      if (!trimmed) return ""

      const parts = trimmed.split(/\s+/)
      const command = parts[0].toLowerCase()
      let result = ""

      try {
        switch (command) {
          case "put": {
            const pageId = parseInt(parts[1] ?? "1", 10)
            const offset = parseInt(parts[2] ?? "0", 10)
            const data = parts.slice(3).join(" ") || "hello"
            const encoded = textEncoder().encode(data)

            const node = getNode()
            const vdl = node.put(pageId, offset, encoded)
            result = `OK (VDL=${vdl})`

            flushToState({
              operationHeader: `Node ${currentNodeRef.current}: PUT pg${pageId} offset=${offset} "${data}"`,
              output: [...state.output, `> ${trimmed}`, result].slice(-100),
            })
            return result
          }
          case "get": {
            const pageId = parseInt(parts[1] ?? "1", 10)
            const node = getNode()

            const page = node.get(pageId)
            const end = page.indexOf(0)
            const textEnd = end === -1 ? page.length : end
            const preview =
              textEnd === 0
                ? "(empty page)"
                : new TextDecoder().decode(page.slice(0, Math.min(textEnd, 80)))
            result = preview

            flushToState({
              operationHeader: `Node ${currentNodeRef.current}: GET pg${pageId} @L${node.getReadPoint()}`,
              output: [...state.output, `> ${trimmed}`, result].slice(-100),
            })
            return result
          }
          case "get-raw": {
            const pageId = parseInt(parts[1] ?? "1", 10)
            const node = getNode()
            const page = node.get(pageId)
            const hex = Array.from(page.slice(0, 64))
              .map((b) => b.toString(16).padStart(2, "0"))
              .join(" ")
            result = hex + (page.some((b, i) => i >= 64 && b !== 0) ? " ..." : "")
            flushToState({
              output: [...state.output, `> ${trimmed}`, result].slice(-100),
            })
            return result
          }
          case "del": {
            const pageId = parseInt(parts[1] ?? "1", 10)
            const offset = parseInt(parts[2] ?? "0", 10)
            const len = parseInt(parts[3] ?? "4", 10)
            const zeros = new Uint8Array(len)
            const node = getNode()
            const vdl = node.put(pageId, offset, zeros)
            result = `Zeroed ${len}B at pg${pageId}:${offset} (VDL=${vdl})`
            flushToState({
              operationHeader: `Node ${currentNodeRef.current}: DEL pg${pageId} offset=${offset} len=${len}`,
              output: [...state.output, `> ${trimmed}`, result].slice(-100),
            })
            return result
          }
          case "clear-page": {
            const pageId = parseInt(parts[1] ?? "1", 10)
            const zeros = new Uint8Array(256)
            const node = getNode()
            const vdl = node.put(pageId, 0, zeros)
            result = `Cleared pg${pageId} (VDL=${vdl})`
            flushToState({
              output: [...state.output, `> ${trimmed}`, result].slice(-100),
            })
            return result
          }
          case "refresh": {
            const node = getNode()
            const rp = node.refreshReadPoint()
            result = `read_point -> ${rp}`
            flushToState({
              operationHeader: `Node ${currentNodeRef.current}: REFRESH read_point`,
              output: [...state.output, `> ${trimmed}`, result].slice(-100),
            })
            return result
          }
          case "node": {
            const target = (parts[1] ?? "A").toUpperCase()
            if (!nodesRef.current.has(target)) {
              result = `Unknown node: ${target}`
            } else {
              currentNodeRef.current = target
              result = `Switched to Node ${target}`
              flushToState({
                currentNode: target,
                output: [...state.output, `> ${trimmed}`, result].slice(-100),
              })
              return result
            }
            break
          }
          case "state": {
            const storage = storageRef.current!
            const dur = storage.getDurabilityState()
            const pi = storage.getPageIndex()
            const piStr =
              Array.from(pi.entries())
                .sort((a, b) => a[0] - b[0])
                .map(([p, l]) => `pg${p}->L${l}`)
                .join(" ") || "none"
            result = `VCL=${dur.vcl} VDL=${dur.vdl} nextLSN=${storage.getNextLsn()} pages=[${piStr}]`
            break
          }
          case "metrics": {
            const m = metricsRef.current.summary()
            const cacheTotal = m.pageCacheHits + m.pageCacheMisses
            const cachePct =
              cacheTotal > 0 ? Math.round((m.pageCacheHits / cacheTotal) * 100) : 0
            const bpTotal = m.bufferPoolHits + m.bufferPoolMisses
            const bpPct =
              bpTotal > 0 ? Math.round((m.bufferPoolHits / bpTotal) * 100) : 0
            result = `Writes:${m.writeCount} Reads:${m.readCount} CacheHit:${cachePct}% BufHit:${bpPct}% Mats:${m.materializeCount}`
            break
          }
          case "clear": {
            eventIdRef.current = 0
            pendingEventsRef.current = []
            setState((prev) => ({
              ...prev,
              events: [],
              output: [],
              operationHeader: "",
            }))
            return "Cleared"
          }
          case "put-random": {
            const count = parseInt(parts[1] ?? "5", 10)
            const node = getNode()
            for (let i = 0; i < count; i++) {
              const pageId = Math.floor(Math.random() * 10) + 1
              const data = textEncoder().encode(`rnd-${i}-${Date.now().toString(36)}`)
              node.put(pageId, 0, data)
            }
            result = `Wrote ${count} random records`
            flushToState({
              output: [...state.output, `> ${trimmed}`, result].slice(-100),
            })
            return result
          }
          case "help": {
            result = [
              "Commands:",
              "  put <page> <offset> <text>  - Write to a page",
              "  get <page>                  - Read a page",
              "  get-raw <page>              - Read page as hex",
              "  del <page> <offset> <len>   - Zero a range",
              "  clear-page <page>           - Clear a page",
              "  put-random <count>          - Random writes",
              "  refresh                     - Refresh read point",
              "  node A|B                    - Switch node",
              "  state                       - Show system state",
              "  metrics                     - Show metrics",
              "  clear                       - Clear log",
              "  help                        - Show this help",
            ].join("\n")
            break
          }
          default:
            result = `Unknown command: ${command}. Type "help" for available commands.`
        }
      } catch (err) {
        result = `Error: ${err instanceof Error ? err.message : String(err)}`
      }

      flushToState({
        output: [...state.output, `> ${trimmed}`, result].slice(-100),
      })

      return result
    },
    [ensureInitialized, getNode, flushToState, state.output]
  )

  const reset = useCallback(() => {
    initializedRef.current = false
    storageRef.current = null
    nodesRef.current = new Map()
    eventIdRef.current = 0
    pendingEventsRef.current = []
    metricsRef.current = new MetricsCollector()
    latestNodesRef.current = new Map([
      ["A", { label: "A", readPoint: 0, nextMtr: 1, bufferPoolPages: [] }],
      ["B", { label: "B", readPoint: 0, nextMtr: 1, bufferPoolPages: [] }],
    ])
    latestSharedRef.current = {
      nextLsn: 1,
      vcl: 0,
      vdl: 0,
      pageIndex: new Map(),
      lsnOffsetCount: 0,
      pageCacheCount: 0,
      walByteSize: 0,
      walLsnRange: null,
    }
    currentNodeRef.current = "A"
    setState({
      events: [],
      nodes: latestNodesRef.current,
      currentNode: "A",
      shared: latestSharedRef.current,
      metrics: null,
      isRunning: false,
      operationHeader: "",
      output: [],
    })
  }, [])

  return {
    state,
    executeCommand,
    reset,
  }
}
