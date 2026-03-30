"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import {
  listCommits,
  listSnapshots,
  rollbackToVersion,
  createSnapshot,
  getSnapshotFrequency,
  setSnapshotFrequency,
  getAntiCommand,
} from "@/lib/api";
import type { Commit, Snapshot } from "@/lib/api";

type Tab = "commits" | "snapshots";

interface ExpandedState {
  inverseSql: string | null;
  loading: boolean;
}

const btnBase: React.CSSProperties = {
  fontSize: 10,
  padding: "2px 8px",
  borderRadius: 3,
  cursor: "pointer",
  border: "none",
  fontWeight: 500,
};

function relTime(ts: string) {
  const diff = Date.now() - new Date(ts).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function fmtDate(ts: string) {
  return new Date(ts).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function VersionPanel() {
  const { activeConnection } = useWorkspace();
  const [tab, setTab] = useState<Tab>("commits");
  const [commits, setCommits] = useState<Commit[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [loading, setLoading] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<string | null>(null);
  const [rollbackMsg, setRollbackMsg] = useState<{ text: string; ok: boolean } | null>(null);
  const [expanded, setExpanded] = useState<Record<string, ExpandedState>>({});
  const [freq, setFreq] = useState<number | null>(null);
  const [editFreq, setEditFreq] = useState(false);
  const [freqInput, setFreqInput] = useState("");
  const [snapshotting, setSnapshotting] = useState(false);
  const freqRef = useRef<HTMLInputElement>(null);

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

  const loadFreq = useCallback(async () => {
    if (!activeConnection) return;
    try {
      const f = await getSnapshotFrequency(activeConnection.id);
      setFreq(f);
      setFreqInput(String(f));
    } catch {
      /* ignore */
    }
  }, [activeConnection]);

  useEffect(() => {
    setExpanded({});
    setRollbackMsg(null);
    setEditFreq(false);
    refresh();
    loadFreq();
  }, [refresh, loadFreq]);

  useEffect(() => {
    if (editFreq) freqRef.current?.focus();
  }, [editFreq]);

  const handleRollback = async (versionId: string) => {
    if (!activeConnection) return;
    if (!confirm("Roll back to this version? This will apply inverse operations.")) return;
    setRollbackTarget(versionId);
    setRollbackMsg(null);
    try {
      const r = await rollbackToVersion(activeConnection.id, versionId);
      setRollbackMsg({
        text: `Rolled back to v${versionId.slice(0, 6)}. ${r.anti_commands_applied} anti-command(s) applied.`,
        ok: true,
      });
      refresh();
    } catch (err: unknown) {
      setRollbackMsg({
        text: err instanceof Error ? err.message : "Rollback failed",
        ok: false,
      });
    } finally {
      setRollbackTarget(null);
    }
  };

  const toggleExpand = async (versionId: string) => {
    if (expanded[versionId]) {
      setExpanded((p) => {
        const n = { ...p };
        delete n[versionId];
        return n;
      });
      return;
    }
    setExpanded((p) => ({ ...p, [versionId]: { inverseSql: null, loading: true } }));
    try {
      const anti = await getAntiCommand(versionId);
      setExpanded((p) => ({
        ...p,
        [versionId]: { inverseSql: anti.inverse_sql, loading: false },
      }));
    } catch {
      setExpanded((p) => ({ ...p, [versionId]: { inverseSql: null, loading: false } }));
    }
  };

  const handleTakeSnapshot = async () => {
    if (!activeConnection || snapshotting) return;
    setSnapshotting(true);
    try {
      await createSnapshot(activeConnection.id);
      await refresh();
      setTab("snapshots");
    } catch (e) {
      alert(e instanceof Error ? e.message : "Snapshot failed");
    } finally {
      setSnapshotting(false);
    }
  };

  const handleSaveFreq = async () => {
    if (!activeConnection) return;
    const v = parseInt(freqInput, 10);
    if (isNaN(v) || v < 1) return;
    try {
      const updated = await setSnapshotFrequency(activeConnection.id, v);
      setFreq(updated);
      setEditFreq(false);
    } catch {
      /* ignore */
    }
  };

  const copySQL = (sql: string) => navigator.clipboard?.writeText(sql).catch(() => {});

  // ---- Render helpers ----

  const headerBtn = (label: string, onClick: () => void, disabled = false): React.ReactNode => (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        ...btnBase,
        background: "var(--secondary)",
        color: disabled ? "var(--muted-foreground)" : "var(--secondary-foreground)",
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
      }}
    >
      {label}
    </button>
  );

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        background: "var(--panel-bg)",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 10px",
          height: 32,
          flexShrink: 0,
          background: "var(--sidebar-header)",
          borderBottom: "1px solid var(--border)",
        }}
      >
        <span
          style={{
            fontSize: 11,
            fontWeight: 700,
            letterSpacing: "0.8px",
            textTransform: "uppercase",
            color: "var(--muted-foreground)",
          }}
        >
          Version Control
        </span>
        <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
          {activeConnection && tab === "snapshots" && (
            <button
              onClick={handleTakeSnapshot}
              disabled={snapshotting}
              style={{
                ...btnBase,
                background: snapshotting ? "var(--muted)" : "var(--primary)",
                color: "#fff",
                padding: "2px 8px",
                opacity: snapshotting ? 0.6 : 1,
              }}
              title="Take a manual pg_dump snapshot"
            >
              {snapshotting ? "Saving..." : "+ Snapshot"}
            </button>
          )}
          <button
            onClick={refresh}
            style={{
              fontSize: 14,
              background: "transparent",
              border: "none",
              color: "var(--muted-foreground)",
              cursor: "pointer",
              padding: "2px 4px",
              lineHeight: 1,
            }}
            title="Refresh"
          >
            ↻
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", flexShrink: 0, borderBottom: "1px solid var(--border)" }}>
        {(["commits", "snapshots"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              flex: 1,
              padding: "6px 0",
              fontSize: 11,
              fontWeight: 600,
              textTransform: "capitalize",
              background: "transparent",
              border: "none",
              cursor: "pointer",
              color: tab === t ? "var(--foreground)" : "var(--muted-foreground)",
              borderBottom: tab === t ? "2px solid var(--primary)" : "2px solid transparent",
            }}
          >
            {t}
            {t === "commits" && commits.length > 0 && (
              <span
                style={{
                  marginLeft: 4,
                  fontSize: 9,
                  background: "var(--muted)",
                  padding: "1px 4px",
                  borderRadius: 8,
                }}
              >
                {commits.length}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Rollback status banner */}
      {rollbackMsg && (
        <div
          style={{
            margin: "6px 8px 0",
            borderRadius: 4,
            padding: "5px 8px",
            fontSize: 11,
            background: rollbackMsg.ok ? "rgba(34,197,94,0.12)" : "rgba(239,68,68,0.12)",
            color: rollbackMsg.ok ? "#22c55e" : "var(--destructive)",
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 6,
          }}
        >
          <span>{rollbackMsg.text}</span>
          <button
            onClick={() => setRollbackMsg(null)}
            style={{ background: "transparent", border: "none", cursor: "pointer", color: "inherit", fontSize: 14, lineHeight: 1 }}
          >
            Ã—
          </button>
        </div>
      )}

      {/* Content */}
      <div style={{ flex: 1, overflowY: "auto", minHeight: 0 }}>
        {!activeConnection && (
          <div
            style={{ padding: "32px 12px", textAlign: "center", fontSize: 12, color: "var(--muted-foreground)" }}
          >
            Select a connection to view history
          </div>
        )}

        {activeConnection && loading && (
          <div style={{ padding: "12px", fontSize: 12, color: "var(--muted-foreground)" }}>
            Loadingâ€¦
          </div>
        )}

        {/* ---- COMMITS TAB ---- */}
        {tab === "commits" && activeConnection && !loading && (
          <>
            {commits.length === 0 ? (
              <div
                style={{ padding: "32px 12px", textAlign: "center", fontSize: 12, color: "var(--muted-foreground)" }}
              >
                No commits yet
              </div>
            ) : (
              [...commits].reverse().map((c) => {
                const exp = expanded[c.version_id];
                const isExpanded = !!exp;
                return (
                  <div
                    key={c.version_id}
                    style={{ borderBottom: "1px solid var(--border)" }}
                  >
                    {/* Commit row */}
                    <div
                      onClick={() => toggleExpand(c.version_id)}
                      style={{
                        padding: "7px 10px",
                        cursor: "pointer",
                        background: isExpanded ? "var(--accent)" : "transparent",
                        userSelect: "none",
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
                        <span
                          style={{
                            width: 7,
                            height: 7,
                            borderRadius: "50%",
                            flexShrink: 0,
                            background: c.status === "success" ? "#22c55e" : "var(--destructive)",
                          }}
                        />
                        <span
                          style={{
                            fontSize: 11,
                            fontWeight: 700,
                            color: "var(--foreground)",
                            fontFamily: "monospace",
                          }}
                        >
                          v{c.seq}
                        </span>
                        <span
                          style={{ fontSize: 9, marginLeft: "auto", color: "var(--muted-foreground)" }}
                          title={fmtDate(c.timestamp)}
                        >
                          {relTime(c.timestamp)}
                        </span>
                        <span
                          style={{
                            fontSize: 10,
                            color: "var(--muted-foreground)",
                            transform: isExpanded ? "rotate(90deg)" : "none",
                            display: "inline-block",
                            transition: "transform 0.15s",
                          }}
                        >
                          â–¶
                        </span>
                      </div>
                      <div
                        style={{
                          fontSize: 11,
                          fontFamily: "monospace",
                          color: "var(--muted-foreground)",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                          maxWidth: "100%",
                        }}
                        title={c.sql_command}
                      >
                        {c.sql_command}
                      </div>
                    </div>

                    {/* Expanded details */}
                    {isExpanded && (
                      <div
                        style={{
                          background: "var(--editor-bg)",
                          borderTop: "1px solid var(--border)",
                          padding: "8px 10px",
                        }}
                      >
                        {/* SQL command */}
                        <div style={{ marginBottom: 6 }}>
                          <div
                            style={{
                              display: "flex",
                              justifyContent: "space-between",
                              alignItems: "center",
                              marginBottom: 3,
                            }}
                          >
                            <span style={{ fontSize: 9, fontWeight: 700, color: "var(--primary)", letterSpacing: "0.6px", textTransform: "uppercase" }}>
                              SQL
                            </span>
                            <button
                              onClick={() => copySQL(c.sql_command)}
                              style={{ ...btnBase, background: "var(--muted)", color: "var(--muted-foreground)" }}
                            >
                              Copy
                            </button>
                          </div>
                          <pre
                            style={{
                              fontSize: 11,
                              fontFamily: "monospace",
                              color: "#93c5fd",
                              background: "var(--background)",
                              padding: "5px 7px",
                              borderRadius: 3,
                              whiteSpace: "pre-wrap",
                              wordBreak: "break-all",
                              margin: 0,
                              maxHeight: 80,
                              overflowY: "auto",
                            }}
                          >
                            {c.sql_command}
                          </pre>
                        </div>

                        {/* Inverse SQL */}
                        <div style={{ marginBottom: 8 }}>
                          <div
                            style={{
                              display: "flex",
                              justifyContent: "space-between",
                              alignItems: "center",
                              marginBottom: 3,
                            }}
                          >
                            <span style={{ fontSize: 9, fontWeight: 700, color: "#f59e0b", letterSpacing: "0.6px", textTransform: "uppercase" }}>
                              Inverse SQL
                            </span>
                            {exp.inverseSql && (
                              <button
                                onClick={() => copySQL(exp.inverseSql!)}
                                style={{ ...btnBase, background: "var(--muted)", color: "var(--muted-foreground)" }}
                              >
                                Copy
                              </button>
                            )}
                          </div>
                          {exp.loading ? (
                            <div style={{ fontSize: 11, color: "var(--muted-foreground)" }}>Loadingâ€¦</div>
                          ) : exp.inverseSql ? (
                            <pre
                              style={{
                                fontSize: 11,
                                fontFamily: "monospace",
                                color: "#fcd34d",
                                background: "var(--background)",
                                padding: "5px 7px",
                                borderRadius: 3,
                                whiteSpace: "pre-wrap",
                                wordBreak: "break-all",
                                margin: 0,
                                maxHeight: 80,
                                overflowY: "auto",
                              }}
                            >
                              {exp.inverseSql}
                            </pre>
                          ) : (
                            <div style={{ fontSize: 11, color: "var(--muted-foreground)" }}>Not available</div>
                          )}
                        </div>

                        {/* Rollback button */}
                        <button
                          onClick={() => handleRollback(c.version_id)}
                          disabled={!!rollbackTarget}
                          style={{
                            ...btnBase,
                            background: rollbackTarget === c.version_id ? "var(--muted)" : "rgba(239,68,68,0.18)",
                            color: rollbackTarget === c.version_id ? "var(--muted-foreground)" : "#f87171",
                            padding: "3px 10px",
                            fontSize: 11,
                            border: "1px solid rgba(239,68,68,0.3)",
                          }}
                        >
                          {rollbackTarget === c.version_id ? "Rolling backâ€¦" : "âª Rollback to here"}
                        </button>
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </>
        )}

        {/* ---- SNAPSHOTS TAB ---- */}
        {tab === "snapshots" && activeConnection && !loading && (
          <>
            {/* Snapshot frequency control */}
            <div
              style={{
                padding: "8px 10px",
                borderBottom: "1px solid var(--border)",
                background: "var(--background)",
              }}
            >
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 6 }}>
                <span style={{ fontSize: 11, color: "var(--muted-foreground)" }}>
                  Auto-snapshot every{" "}
                  {!editFreq && (
                    <strong style={{ color: "var(--foreground)" }}>
                      {freq ?? "â€¦"} commit{freq !== 1 ? "s" : ""}
                    </strong>
                  )}
                </span>
                {!editFreq ? (
                  headerBtn("Edit", () => setEditFreq(true))
                ) : (
                  <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                    <input
                      ref={freqRef}
                      type="number"
                      min={1}
                      value={freqInput}
                      onChange={(e) => setFreqInput(e.target.value)}
                      onKeyDown={(e) => { if (e.key === "Enter") handleSaveFreq(); if (e.key === "Escape") setEditFreq(false); }}
                      style={{
                        width: 46,
                        fontSize: 11,
                        background: "var(--muted)",
                        border: "1px solid var(--border)",
                        borderRadius: 3,
                        color: "var(--foreground)",
                        padding: "2px 5px",
                        outline: "none",
                      }}
                    />
                    <button
                      onClick={handleSaveFreq}
                      style={{ ...btnBase, background: "var(--primary)", color: "#fff", padding: "2px 7px" }}
                    >
                      Save
                    </button>
                    <button
                      onClick={() => setEditFreq(false)}
                      style={{ ...btnBase, background: "var(--secondary)", color: "var(--muted-foreground)" }}
                    >
                      âœ•
                    </button>
                  </div>
                )}
              </div>
            </div>

            {/* Snapshot list */}
            {snapshots.length === 0 ? (
              <div
                style={{ padding: "32px 12px", textAlign: "center", fontSize: 12, color: "var(--muted-foreground)" }}
              >
                No snapshots yet â€” click "+ Snapshot" to create one
              </div>
            ) : (
              snapshots.map((s) => (
                <div
                  key={s.snapshot_id}
                  style={{
                    padding: "8px 10px",
                    borderBottom: "1px solid var(--border)",
                    fontSize: 11,
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
                    <span style={{ fontSize: 13 }}>ðŸ“¸</span>
                    <span style={{ fontFamily: "monospace", color: "var(--foreground)", fontWeight: 600 }}>
                      {s.snapshot_id.slice(0, 8)}
                    </span>
                    <span style={{ marginLeft: "auto", fontSize: 9, color: "var(--muted-foreground)" }}>
                      {relTime(s.created_at)}
                    </span>
                  </div>
                  <div style={{ fontSize: 10, color: "var(--muted-foreground)", marginBottom: 1 }}>
                    {fmtDate(s.created_at)}
                  </div>
                  <div
                    style={{
                      fontSize: 10,
                      fontFamily: "monospace",
                      color: "var(--muted-foreground)",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                    title={s.s3_key}
                  >
                    {s.s3_key}
                  </div>
                </div>
              ))
            )}
          </>
        )}
      </div>
    </div>
  );
}
