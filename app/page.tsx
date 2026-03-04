"use client"

import { useCallback } from "react"
import { ArchitecturePanel } from "@/components/architecture-panel"
import { CommandInput } from "@/components/command-input"
import { EventLog } from "@/components/event-log"
import { MetricsDisplay } from "@/components/metrics-display"
import { OutputPanel } from "@/components/output-panel"
import { useAurora } from "@/lib/hooks/use-aurora"

function Header({ onReset }: { onReset: () => void }) {
  return (
    <header className="flex items-center justify-between border-b bg-card px-6 py-3">
      <div className="flex items-center gap-3">
        <div className="flex h-7 w-7 items-center justify-center rounded-md bg-accent">
          <svg viewBox="0 0 16 16" fill="none" className="h-4 w-4 text-background" aria-hidden="true">
            <path d="M8 2L14 6V10L8 14L2 10V6L8 2Z" fill="currentColor" />
          </svg>
        </div>
        <div>
          <h1 className="text-sm font-semibold text-foreground">Mini-Aurora</h1>
          <p className="text-xs text-muted-foreground">Interactive Database Internals Visualizer</p>
        </div>
      </div>
      <div className="flex items-center gap-3">
        <a
          href="https://github.com/fhenrywells/mini-aurora"
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-muted-foreground transition-colors hover:text-foreground"
        >
          GitHub
        </a>
        <button
          onClick={onReset}
          className="rounded-md border border-border bg-muted px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:border-accent hover:text-foreground"
        >
          Reset
        </button>
      </div>
    </header>
  )
}

function OperationBanner({ header }: { header: string }) {
  if (!header) return null
  return (
    <div className="border-b bg-accent/5 px-6 py-1.5">
      <span className="font-mono text-xs font-medium text-accent">{header}</span>
    </div>
  )
}

export default function AuroraPage() {
  const { state, executeCommand, reset } = useAurora()

  const handleCommand = useCallback(
    (cmd: string) => {
      executeCommand(cmd)
    },
    [executeCommand]
  )

  return (
    <div className="flex h-screen flex-col bg-background">
      <Header onReset={reset} />
      <OperationBanner header={state.operationHeader} />

      {/* Main content area */}
      <div className="flex min-h-0 flex-1">
        {/* Left: Event log */}
        <div className="flex min-h-0 flex-1 flex-col border-r">
          <EventLog events={state.events} />
        </div>

        {/* Right: Architecture panel */}
        <div className="hidden w-72 flex-col lg:flex xl:w-80">
          <ArchitecturePanel
            nodes={state.nodes}
            currentNode={state.currentNode}
            shared={state.shared}
          />
        </div>
      </div>

      {/* Bottom section */}
      <div className="flex flex-col">
        <OutputPanel output={state.output} />
        <MetricsDisplay metrics={state.metrics} />
        <CommandInput onCommand={handleCommand} currentNode={state.currentNode} />
      </div>
    </div>
  )
}
