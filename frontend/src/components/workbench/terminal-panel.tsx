"use client";

import { useWorkspace } from "@/lib/workspace-context";

export function TerminalPanel() {
  const { activeConnection } = useWorkspace();

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", background: "var(--terminal-bg)", overflow: "hidden" }}>
      {/* Header */}
      <div
        className="flex items-center gap-2 px-3 py-1 shrink-0"
        style={{ background: "var(--toolbar-bg)", borderBottom: "1px solid var(--border)" }}
      >
        <span className="text-xs font-medium" style={{ color: "var(--muted-foreground)" }}>
          psql Terminal
        </span>
        <div className="flex-1" />
        {activeConnection && (
          <span className="text-[10px] px-2 py-0.5 rounded" style={{ background: "var(--muted)", color: "var(--muted-foreground)" }}>
            {activeConnection.db_username}@{activeConnection.host}/{activeConnection.database_name}
          </span>
        )}
      </div>

      {/* Terminal body - placeholder for xterm.js in Phase 4 */}
      <div style={{ flex: 1, padding: "12px", overflowY: "auto", minHeight: 0, fontFamily: "'Cascadia Code', 'Fira Code', monospace" }}>
        {!activeConnection ? (
          <div className="text-xs" style={{ color: "var(--muted-foreground)" }}>
            Select a connection to start a psql session.
          </div>
        ) : (
          <div className="text-xs space-y-1">
            <div style={{ color: "var(--muted-foreground)" }}>
              {`-- psql terminal will connect to ${activeConnection.db_username}@${activeConnection.host}:${activeConnection.port}/${activeConnection.database_name}`}
            </div>
            <div style={{ color: "var(--muted-foreground)" }}>
              -- WebSocket terminal integration coming in Phase 4
            </div>
            <div className="flex items-center gap-1" style={{ color: "#22c55e" }}>
              <span>{activeConnection.database_name}=#</span>
              <span className="animate-pulse">▌</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
