"use client"

import type { MetricsSummary } from "@/lib/engine/events"

function MetricItem({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex flex-col items-center">
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
      <span className="text-xs text-muted-foreground">{label}</span>
    </div>
  )
}

export function MetricsDisplay({ metrics }: { metrics: MetricsSummary | null }) {
  if (!metrics) return null

  const cacheTotal = metrics.pageCacheHits + metrics.pageCacheMisses
  const cachePct = cacheTotal > 0 ? Math.round((metrics.pageCacheHits / cacheTotal) * 100) : 0
  const bpTotal = metrics.bufferPoolHits + metrics.bufferPoolMisses
  const bpPct = bpTotal > 0 ? Math.round((metrics.bufferPoolHits / bpTotal) * 100) : 0

  return (
    <div className="flex items-center gap-6 border-t px-4 py-2">
      <MetricItem label="Writes" value={metrics.writeCount} />
      <MetricItem label="Reads" value={metrics.readCount} />
      <MetricItem label="Cache Hit" value={`${cachePct}%`} />
      <MetricItem label="BufPool Hit" value={`${bpPct}%`} />
      <MetricItem label="Materializations" value={metrics.materializeCount} />
      <MetricItem label="VCL" value={metrics.vcl} />
      <MetricItem label="VDL" value={metrics.vdl} />
    </div>
  )
}
