"use client"

import { useRef, useState } from "react"
import { cn } from "@/lib/utils"

const SUGGESTION_CHIPS = [
  { label: "put 1 0 hello", description: "Write 'hello' to page 1" },
  { label: "get 1", description: "Read page 1" },
  { label: "refresh", description: "Refresh read point" },
  { label: "node B", description: "Switch to Node B" },
  { label: "put 2 0 world", description: "Write 'world' to page 2" },
  { label: "state", description: "Show system state" },
  { label: "metrics", description: "Show metrics" },
  { label: "put-random 5", description: "5 random writes" },
]

export function CommandInput({
  onCommand,
  currentNode,
}: {
  onCommand: (cmd: string) => void
  currentNode: string
}) {
  const [value, setValue] = useState("")
  const [history, setHistory] = useState<string[]>([])
  const [historyIdx, setHistoryIdx] = useState(-1)
  const inputRef = useRef<HTMLInputElement>(null)

  const submit = (cmd: string) => {
    const trimmed = cmd.trim()
    if (!trimmed) return
    onCommand(trimmed)
    setHistory((prev) => [...prev, trimmed])
    setHistoryIdx(-1)
    setValue("")
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      submit(value)
    } else if (e.key === "ArrowUp") {
      e.preventDefault()
      if (history.length === 0) return
      const newIdx = historyIdx === -1 ? history.length - 1 : Math.max(0, historyIdx - 1)
      setHistoryIdx(newIdx)
      setValue(history[newIdx])
    } else if (e.key === "ArrowDown") {
      e.preventDefault()
      if (historyIdx === -1) return
      const newIdx = historyIdx + 1
      if (newIdx >= history.length) {
        setHistoryIdx(-1)
        setValue("")
      } else {
        setHistoryIdx(newIdx)
        setValue(history[newIdx])
      }
    }
  }

  return (
    <div className="border-t bg-card">
      {/* Suggestion chips */}
      <div className="flex flex-wrap gap-1.5 border-b px-4 py-2">
        {SUGGESTION_CHIPS.map((chip) => (
          <button
            key={chip.label}
            onClick={() => submit(chip.label)}
            title={chip.description}
            className={cn(
              "rounded-md border border-border bg-muted px-2.5 py-1 font-mono text-xs text-muted-foreground",
              "transition-colors hover:border-accent hover:text-foreground"
            )}
          >
            {chip.label}
          </button>
        ))}
      </div>

      {/* Input */}
      <div className="flex items-center gap-2 px-4 py-3">
        <span className="font-mono text-sm font-semibold text-accent">
          {currentNode}{">"}
        </span>
        <input
          ref={inputRef}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder='Type a command... (try "put 1 0 hello" or "help")'
          className="flex-1 bg-transparent font-mono text-sm text-foreground placeholder:text-muted-foreground focus:outline-none"
          autoFocus
          spellCheck={false}
          autoComplete="off"
        />
      </div>
    </div>
  )
}
