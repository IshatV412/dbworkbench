"use client";

import { useAuth } from "@/lib/auth-context";
import { useWorkspace } from "@/lib/workspace-context";
import { ConnectionModal } from "@/components/workbench/connection-modal";
import { useState } from "react";

export function Toolbar() {
  const { username, logout } = useAuth();
  const { activeConnection, connections, setActiveConnection } = useWorkspace();
  const [showConnModal, setShowConnModal] = useState(false);

  return (
    <>
      <div
        className="flex items-center justify-between px-3 py-1.5 text-sm select-none shrink-0"
        style={{ background: "var(--toolbar-bg)", borderBottom: "1px solid var(--border)" }}
      >
        {/* Left: Logo + Connection */}
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <svg width="20" height="20" viewBox="0 0 32 32" fill="none" className="text-blue-500">
              <rect x="2" y="4" width="28" height="8" rx="2" fill="currentColor" opacity="0.9" />
              <rect x="2" y="14" width="28" height="8" rx="2" fill="currentColor" opacity="0.6" />
              <rect x="2" y="24" width="28" height="4" rx="2" fill="currentColor" opacity="0.3" />
            </svg>
            <span className="font-semibold tracking-tight" style={{ color: "var(--foreground)" }}>
              WEAVE-DB
            </span>
          </div>

          <div style={{ width: 1, height: 20, background: "var(--border)" }} />

          {/* Connection selector */}
          <select
            value={activeConnection?.id ?? ""}
            onChange={(e) => {
              const id = Number(e.target.value);
              const conn = connections.find((c) => c.id === id) ?? null;
              setActiveConnection(conn);
            }}
            className="rounded px-2 py-1 text-xs outline-none"
            style={{
              background: "var(--muted)",
              border: "1px solid var(--border)",
              color: "var(--foreground)",
              minWidth: 160,
            }}
          >
            <option value="">Select connection...</option>
            {connections.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name} ({c.db_username}@{c.host}/{c.database_name})
              </option>
            ))}
          </select>

          <button
            onClick={() => setShowConnModal(true)}
            className="rounded px-2 py-1 text-xs font-medium transition-colors hover:opacity-80"
            style={{ background: "var(--primary)", color: "var(--primary-foreground)" }}
            title="New Connection"
          >
            + Connection
          </button>
        </div>

        {/* Right: User */}
        <div className="flex items-center gap-3">
          <span className="text-xs" style={{ color: "var(--muted-foreground)" }}>
            {username}
          </span>
          <button
            onClick={logout}
            className="rounded px-2 py-1 text-xs transition-colors hover:opacity-80"
            style={{ background: "var(--secondary)", color: "var(--secondary-foreground)" }}
          >
            Sign Out
          </button>
        </div>
      </div>

      {showConnModal && <ConnectionModal onClose={() => setShowConnModal(false)} />}
    </>
  );
}
