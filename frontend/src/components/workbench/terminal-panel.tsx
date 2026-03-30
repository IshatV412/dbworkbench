"use client";

import { useState, useEffect } from "react";
import dynamic from "next/dynamic";
import { useWorkspace } from "@/lib/workspace-context";
import { getTerminalTicket } from "@/lib/api";

const XtermTerminal = dynamic(
  () => import("@/components/workbench/xterm-terminal").then((m) => m.XtermTerminal),
  {
    ssr: false,
    loading: () => (
      <div style={{ padding: 12, color: "var(--muted-foreground)", fontSize: 12 }}>
        Loading terminal...
      </div>
    ),
  }
);

const WS_BASE = (process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001").replace(
  /^http/,
  "ws"
);

export function TerminalPanel() {
  const { activeConnection } = useWorkspace();
  const [wsUrl, setWsUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setWsUrl(null);
    setError(null);
    if (!activeConnection) return;

    let cancelled = false;
    getTerminalTicket(activeConnection.id)
      .then((ticket) => {
        if (!cancelled) {
          setWsUrl(`${WS_BASE}/terminal/ws?ticket=${encodeURIComponent(ticket)}`);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to obtain terminal ticket");
        }
      });

    return () => { cancelled = true; };
  }, [activeConnection]);

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background: "var(--terminal-bg)",
        overflow: "hidden",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "0 12px",
          height: 30,
          flexShrink: 0,
          background: "var(--toolbar-bg)",
          borderBottom: "1px solid var(--border)",
        }}
      >
        <span style={{ fontSize: 11, fontWeight: 600, color: "var(--muted-foreground)", letterSpacing: "0.5px" }}>
          psql Terminal
        </span>
        {activeConnection && (
          <>
            <div style={{ width: 1, height: 14, background: "var(--border)" }} />
            <span style={{ fontSize: 10, color: "var(--muted-foreground)" }}>
              {activeConnection.db_username}@{activeConnection.host}:{activeConnection.port}/
              {activeConnection.database_name}
            </span>
          </>
        )}
      </div>

      {/* Terminal body */}
      <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
        {!activeConnection ? (
          <div style={{ padding: "24px 16px", color: "var(--muted-foreground)", fontSize: 12 }}>
            Select a connection to start a psql session.
          </div>
        ) : error ? (
          <div style={{ padding: "24px 16px", color: "var(--destructive, red)", fontSize: 12 }}>
            {error}
          </div>
        ) : !wsUrl ? (
          <div style={{ padding: "24px 16px", color: "var(--muted-foreground)", fontSize: 12 }}>
            Connecting...
          </div>
        ) : (
          <XtermTerminal
            key={wsUrl}
            wsUrl={wsUrl}
          />
        )}
      </div>
    </div>
  );
}
