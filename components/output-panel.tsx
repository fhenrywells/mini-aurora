"use client"

import { useEffect, useRef } from "react"

function colorLine(line: string): string {
  if (line.startsWith("> ")) return "text-accent"
  if (line.startsWith("OK") || line.startsWith("read_point") || line.startsWith("Cleared") || line.startsWith("Switched")) return "text-success"
  if (line.startsWith("Error")) return "text-destructive"
  if (line.startsWith("Commands:") || line.startsWith("  ")) return "text-muted-foreground"
  return "text-foreground"
}

export function OutputPanel({ output }: { output: string[] }) {
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [output])

  if (output.length === 0) return null

  return (
    <div className="border-t">
      <div className="flex items-center border-b px-4 py-1.5">
        <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Output</h2>
      </div>
      <div ref={scrollRef} className="max-h-32 overflow-y-auto px-4 py-2">
        {output.map((line, i) => (
          <div key={i} className={`font-mono text-xs whitespace-pre-wrap ${colorLine(line)}`}>
            {line}
          </div>
        ))}
      </div>
    </div>
  )
}
