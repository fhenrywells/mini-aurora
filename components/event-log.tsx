"use client"

import { useEffect, useRef } from "react"
import type { TimestampedEvent } from "@/lib/hooks/use-aurora"
import { cn } from "@/lib/utils"

function getEventColor(text: string): string {
  if (text.startsWith("MTR")) return "text-node-a"
  if (text.startsWith("Assign")) return "text-accent"
  if (text.startsWith("Link")) return "text-muted-foreground"
  if (text.startsWith("WAL")) return "text-wal"
  if (text.startsWith("Page index") || text.startsWith("LSN offset")) return "text-storage"
  if (text.startsWith("VCL") || text.startsWith("VDL")) return "text-warning"
  if (text.startsWith("read_point")) return "text-node-a"
  if (text.includes("HIT")) return "text-success"
  if (text.includes("MISS")) return "text-destructive"
  if (text.startsWith("PageIdx") || text.startsWith("PageCache")) return "text-storage"
  if (text.startsWith("Chain")) return "text-muted-foreground"
  if (text.startsWith("Apply")) return "text-wal"
  if (text.startsWith("Materialized")) return "text-success"
  if (text.startsWith("Cache") || text.startsWith("BufPool insert")) return "text-accent"
  if (text.startsWith("Evict")) return "text-destructive"
  return "text-foreground"
}

export function EventLog({ events }: { events: TimestampedEvent[] }) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const shouldAutoScroll = useRef(true)

  useEffect(() => {
    if (shouldAutoScroll.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [events])

  const handleScroll = () => {
    if (!scrollRef.current) return
    const { scrollTop, scrollHeight, clientHeight } = scrollRef.current
    shouldAutoScroll.current = scrollHeight - scrollTop - clientHeight < 40
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b px-4 py-2">
        <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Event Log</h2>
        <span className="font-mono text-xs text-muted-foreground">{events.length} events</span>
      </div>
      <div ref={scrollRef} onScroll={handleScroll} className="flex-1 overflow-y-auto p-2">
        {events.length === 0 ? (
          <div className="flex h-full items-center justify-center">
            <p className="text-sm text-muted-foreground">
              {"Type a command below to see events flow through the engine"}
            </p>
          </div>
        ) : (
          <div className="space-y-px">
            {events.map((e, idx) => (
              <div
                key={e.id}
                className={cn(
                  "flex items-baseline gap-2 rounded px-2 py-0.5 font-mono text-xs transition-colors",
                  idx === events.length - 1 ? "bg-muted" : "hover:bg-muted/50"
                )}
              >
                <span className="w-6 shrink-0 text-right text-muted-foreground">{idx + 1}</span>
                <span className={getEventColor(e.text)}>{e.text}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
