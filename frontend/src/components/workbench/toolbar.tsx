"use client";

import { useAuth } from "@/lib/auth-context";
import { useWorkspace } from "@/lib/workspace-context";
import { ConnectionModal } from "@/components/workbench/connection-modal";
import { useState } from "react";
import type { ConnectionProfile } from "@/lib/api";

export function Toolbar() {
  const { username, logout } = useAuth();
  const { activeConnection, connections, setActiveConnection } = useWorkspace();
  const [showConnModal, setShowConnModal] = useState(false);
  const [editConn, setEditConn] = useState<ConnectionProfile | null>(null);

  const openNew = () => { setEditConn(null); setShowConnModal(true); };
  const openEdit = () => { if (activeConnection) { setEditConn(activeConnection); setShowConnModal(true); } };

  return (
    <>
      <div
        style={{
          background: "var(--toolbar-bg)",
          borderBottom: "2px solid var(--toolbar-border)",
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 12px",
          height: 44,
          userSelect: "none",
        }}
      >
        {/* Left: Logo + Connection */}
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {/* Logo */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginRight: 4 }}>
            <svg width="22" height="22" viewBox="0 0 32 32" fill="none">
              <rect x="2" y="4" width="28" height="8" rx="2" fill="#4e9cf5" opacity="0.95" />
              <rect x="2" y="14" width="28" height="8" rx="2" fill="#4e9cf5" opacity="0.65" />
              <rect x="2" y="24" width="28" height="4" rx="2" fill="#4e9cf5" opacity="0.35" />
            </svg>
            <span style={{ color: "#e2e8f0", fontWeight: 700, fontSize: 14, letterSpacing: "0.5px" }}>
              WEAVE-DB
            </span>
          </div>

          <div style={{ width: 1, height: 24, background: "var(--border)" }} />

          {/* Connection selector */}
          <select
            value={activeConnection?.id ?? ""}
            onChange={(e) => {
              const id = Number(e.target.value);
              const conn = connections.find((c) => c.id === id) ?? null;
              setActiveConnection(conn);
            }}
            style={{
              background: "var(--muted)",
              border: "1px solid var(--border)",
              color: activeConnection ? "#e2e8f0" : "var(--muted-foreground)",
              fontSize: 12,
              borderRadius: 4,
              padding: "4px 8px",
              minWidth: 200,
              outline: "none",
              cursor: "pointer",
            }}
          >
            <option value="">— Select connection —</option>
            {connections.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name} ({c.db_username}@{c.host}/{c.database_name})
              </option>
            ))}
          </select>

          {/* Connection status indicator */}
          {activeConnection ? (
            <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
              <div style={{ width: 8, height: 8, borderRadius: "50%", background: "#22c55e", boxShadow: "0 0 6px #22c55e88" }} />
              <span style={{ fontSize: 11, color: "#22c55e", fontWeight: 500 }}>Connected</span>
            </div>
          ) : (
            <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
              <div style={{ width: 8, height: 8, borderRadius: "50%", background: "#6b7280" }} />
              <span style={{ fontSize: 11, color: "#6b7280" }}>Disconnected</span>
            </div>
          )}

          <div style={{ width: 1, height: 24, background: "var(--border)" }} />

          {/* Action buttons */}
          <button
            onClick={openNew}
            style={{
              background: "var(--primary)",
              color: "#fff",
              border: "none",
              borderRadius: 4,
              padding: "4px 12px",
              fontSize: 12,
              fontWeight: 600,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: 4,
            }}
            title="Add new connection"
          >
            <span style={{ fontSize: 14, lineHeight: 1 }}>＋</span> New Connection
          </button>

          {activeConnection && (
            <button
              onClick={openEdit}
              style={{
                background: "var(--secondary)",
                color: "var(--secondary-foreground)",
                border: "1px solid var(--border)",
                borderRadius: 4,
                padding: "4px 10px",
                fontSize: 12,
                cursor: "pointer",
              }}
              title="Edit active connection"
            >
              ✎ Edit
            </button>
          )}
        </div>

        {/* Right: User info */}
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <div style={{
              width: 26, height: 26, borderRadius: "50%",
              background: "var(--primary)", color: "#fff",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 11, fontWeight: 700,
            }}>
              {username?.[0]?.toUpperCase() ?? "U"}
            </div>
            <span style={{ fontSize: 12, color: "var(--muted-foreground)" }}>{username}</span>
          </div>
          <button
            onClick={logout}
            style={{
              background: "transparent",
              color: "var(--muted-foreground)",
              border: "1px solid var(--border)",
              borderRadius: 4,
              padding: "3px 10px",
              fontSize: 11,
              cursor: "pointer",
            }}
          >
            Sign Out
          </button>
        </div>
      </div>

      {showConnModal && (
        <ConnectionModal
          onClose={() => setShowConnModal(false)}
          editConnection={editConn}
        />
      )}
    </>
  );
}
