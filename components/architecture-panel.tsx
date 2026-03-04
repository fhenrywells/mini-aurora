"use client"

import type { NodeState, SharedState } from "@/lib/hooks/use-aurora"
import { cn } from "@/lib/utils"

function NodeCard({
  node,
  isActive,
  color,
}: {
  node: NodeState
  isActive: boolean
  color: string
}) {
  const bpStr =
    node.bufferPoolPages.length === 0
      ? "(empty)"
      : node.bufferPoolPages.map((p) => `pg${p}`).join(", ")

  return (
    <div
      className={cn(
        "rounded-lg border p-3 transition-colors",
        isActive ? "border-accent bg-accent/5" : "border-border bg-card"
      )}
    >
      <div className="mb-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={cn("h-2 w-2 rounded-full", color)} />
          <span className="text-xs font-semibold uppercase tracking-wider text-foreground">
            Node {node.label}
          </span>
        </div>
        <span className="font-mono text-xs text-muted-foreground">@L{node.readPoint}</span>
      </div>
      <div className="space-y-1 font-mono text-xs">
        <div className="flex justify-between">
          <span className="text-muted-foreground">read_point</span>
          <span className="text-foreground">{node.readPoint}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">next_mtr</span>
          <span className="text-foreground">{node.nextMtr}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">buffer_pool</span>
          <span className="text-foreground">{bpStr}</span>
        </div>
      </div>
    </div>
  )
}

function StorageCard({ shared }: { shared: SharedState }) {
  const piEntries = Array.from(shared.pageIndex.entries()).sort((a, b) => a[0] - b[0])
  const piStr =
    piEntries.length === 0 ? "(empty)" : piEntries.map(([p, l]) => `${p}\u2192L${l}`).join("  ")

  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 flex items-center gap-2">
        <div className="h-2 w-2 rounded-full bg-storage" />
        <span className="text-xs font-semibold uppercase tracking-wider text-foreground">Storage</span>
      </div>
      <div className="space-y-1 font-mono text-xs">
        <div className="flex justify-between">
          <span className="text-muted-foreground">VCL / VDL</span>
          <span className="text-warning">{shared.vcl} / {shared.vdl}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">next_lsn</span>
          <span className="text-foreground">{shared.nextLsn}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">page_index</span>
          <span className="text-foreground">{piStr}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">cache / offsets</span>
          <span className="text-foreground">{shared.pageCacheCount} / {shared.lsnOffsetCount}</span>
        </div>
      </div>
    </div>
  )
}

function WalCard({ shared }: { shared: SharedState }) {
  const walStr = shared.walLsnRange
    ? `L${shared.walLsnRange[0]}..L${shared.walLsnRange[1]}`
    : "(empty)"

  const walSize =
    shared.walByteSize < 1024
      ? `${shared.walByteSize} B`
      : `${(shared.walByteSize / 1024).toFixed(1)} KB`

  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 flex items-center gap-2">
        <div className="h-2 w-2 rounded-full bg-wal" />
        <span className="text-xs font-semibold uppercase tracking-wider text-foreground">WAL</span>
      </div>
      <div className="space-y-1 font-mono text-xs">
        <div className="flex justify-between">
          <span className="text-muted-foreground">LSN range</span>
          <span className="text-wal">{walStr}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">size</span>
          <span className="text-foreground">{walSize}</span>
        </div>
      </div>
    </div>
  )
}

export function ArchitecturePanel({
  nodes,
  currentNode,
  shared,
}: {
  nodes: Map<string, NodeState>
  currentNode: string
  shared: SharedState
}) {
  const nodeA = nodes.get("A") ?? { label: "A", readPoint: 0, nextMtr: 1, bufferPoolPages: [] }
  const nodeB = nodes.get("B") ?? { label: "B", readPoint: 0, nextMtr: 1, bufferPoolPages: [] }

  return (
    <div className="flex h-full flex-col">
      <div className="border-b px-4 py-2">
        <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Architecture</h2>
      </div>
      <div className="flex flex-1 flex-col gap-2 overflow-y-auto p-3">
        <NodeCard node={nodeA} isActive={currentNode === "A"} color="bg-node-a" />
        {/* Arrow between nodes and storage */}
        <div className="flex justify-center">
          <div className="h-4 w-px bg-border" />
        </div>
        <NodeCard node={nodeB} isActive={currentNode === "B"} color="bg-node-b" />
        <div className="flex justify-center">
          <div className="h-4 w-px bg-border" />
        </div>
        <StorageCard shared={shared} />
        <div className="flex justify-center">
          <div className="h-4 w-px bg-border" />
        </div>
        <WalCard shared={shared} />
      </div>
    </div>
  )
}
