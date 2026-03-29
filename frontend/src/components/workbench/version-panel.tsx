"use client";

import { useState, useEffect, useCallback } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { listCommits, listSnapshots, rollbackToVersion } from "@/lib/api";
import type { Commit, Snapshot } from "@/lib/api";

type Tab = "commits" | "snapshots";

export function VersionPanel() {
  const { activeConnection } = useWorkspace();
  const [tab, setTab] = useState<Tab>("commits");
  const [commits, setCommits] = useState<Commit[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [loading, setLoading] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<string | null>(null);
  const [rollbackStatus, setRollbackStatus] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    if (!activeConnection) return;
    setLoading(true);
    try {
      const [c, s] = await Promise.all([
        listCommits(activeConnection.id),
        listSnapshots(activeConnection.id),
      ]);
      setCommits(c);
      setSnapshots(s);
    } catch {
      /* ignore */
    } finally {
      setLoading(false);
    }
  }, [activeConnection]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const handleRollback = async (versionId: string) => {
    if (!activeConnection) return;
    setRollbackTarget(versionId);
    setRollbackStatus(null);
    try {
      const result = await rollbackToVersion(activeConnection.id, versionId);
      setRollbackStatus(`Rolled back. Anti-commands applied: ${result.anti_commands_applied}`);
      refresh();
    } catch (err: unknown) {
      setRollbackStatus(err instanceof Error ? err.message : "Rollback failed");
    } finally {
      setRollbackTarget(null);
    }
  };

  const formatTime = (ts: string) => {
    const d = new Date(ts);
    return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  };

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden", background: "var(--panel-bg)" }}>
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 10px",
          height: 32,
          flexShrink: 0,
          color: "var(--muted-foreground)",
          background: "var(--sidebar-header)",
          borderBottom: "1px solid var(--border)",
        }}
      >
        <span style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.8px", textTransform: "uppercase" }}>Version Control</span>
        <button
          onClick={refresh}
          style={{ fontSize: 14, background: "transparent", border: "none", color: "var(--muted-foreground)", cursor: "pointer", padding: "2px 4px" }}
          title="Refresh"
        >
          ↻
        </button>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", flexShrink: 0, borderBottom: "1px solid var(--border)" }}>
        {(["commits", "snapshots"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className="flex-1 py-1.5 text-xs font-medium capitalize"
            style={{
              color: tab === t ? "var(--foreground)" : "var(--muted-foreground)",
              borderBottom: tab === t ? "2px solid var(--primary)" : "2px solid transparent",
            }}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Content */}
      <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
        {!activeConnection && (
          <div className="px-3 py-8 text-center text-xs" style={{ color: "var(--muted-foreground)" }}>
            Select a connection to view history
          </div>
        )}

        {activeConnection && loading && (
          <div className="px-3 py-4 text-xs" style={{ color: "var(--muted-foreground)" }}>
            Loading...
          </div>
        )}

        {/* Rollback status */}
        {rollbackStatus && (
          <div
            className="mx-2 mt-2 rounded px-2 py-1 text-[11px]"
            style={{
              background: rollbackStatus.includes("failed") ? "rgba(239,68,68,0.1)" : "rgba(34,197,94,0.1)",
              color: rollbackStatus.includes("failed") ? "var(--destructive)" : "#22c55e",
            }}
          >
            {rollbackStatus}
          </div>
        )}

        {/* Commits tab */}
        {tab === "commits" && activeConnection && !loading && (
          <div className="py-1">
            {commits.length === 0 && (
              <div className="px-3 py-4 text-xs text-center" style={{ color: "var(--muted-foreground)" }}>
                No commits yet
              </div>
            )}
            {[...commits].reverse().map((c) => (
              <div
                key={c.version_id}
                className="px-3 py-2 text-xs hover:opacity-80"
                style={{ borderBottom: "1px solid var(--border)" }}
              >
                <div className="flex items-center gap-2 mb-1">
                  <span
                    className="w-2 h-2 rounded-full shrink-0"
                    style={{ background: c.status === "success" ? "#22c55e" : "var(--destructive)" }}
                  />
                  <span className="font-mono font-medium truncate" style={{ color: "var(--foreground)" }}>
                    v{c.seq}
                  </span>
                  <span className="ml-auto text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                    {formatTime(c.timestamp)}
                  </span>
                </div>
                <div
                  className="font-mono text-[11px] truncate mb-1"
                  style={{ color: "var(--muted-foreground)" }}
                  title={c.sql_command}
                >
                  {c.sql_command}
                </div>
                <button
                  onClick={() => handleRollback(c.version_id)}
                  disabled={!!rollbackTarget}
                  className="text-[10px] px-2 py-0.5 rounded hover:opacity-80 disabled:opacity-40"
                  style={{ background: "var(--secondary)", color: "var(--secondary-foreground)" }}
                >
                  {rollbackTarget === c.version_id ? "Rolling back..." : "Rollback here"}
                </button>
              </div>
            ))}
          </div>
        )}

        {/* Snapshots tab */}
        {tab === "snapshots" && activeConnection && !loading && (
          <div className="py-1">
            {snapshots.length === 0 && (
              <div className="px-3 py-4 text-xs text-center" style={{ color: "var(--muted-foreground)" }}>
                No snapshots yet
              </div>
            )}
            {snapshots.map((s) => (
              <div
                key={s.snapshot_id}
                className="px-3 py-2 text-xs"
                style={{ borderBottom: "1px solid var(--border)" }}
              >
                <div className="flex items-center gap-2 mb-1">
                  <span>📸</span>
                  <span className="font-mono" style={{ color: "var(--foreground)" }}>
                    {s.snapshot_id.slice(0, 8)}
                  </span>
                  <span className="ml-auto text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                    {formatTime(s.created_at)}
                  </span>
                </div>
                <div className="text-[11px] truncate" style={{ color: "var(--muted-foreground)" }}>
                  version: {s.version_id.slice(0, 8)}...
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
