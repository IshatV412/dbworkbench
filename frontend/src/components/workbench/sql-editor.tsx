"use client";

import { useState, useRef, useCallback } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { executeSQL, type QueryResult } from "@/lib/api";

// We'll store results in a simple event emitter pattern so ResultsGrid can pick them up
// For Phase 1, we use a simple callback approach via context or window event

export function SqlEditor() {
  const { activeConnection } = useWorkspace();
  const [sql, setSql] = useState("SELECT 1;");
  const [running, setRunning] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const handleRun = useCallback(async () => {
    if (!activeConnection || !sql.trim()) return;
    setRunning(true);
    try {
      const result = await executeSQL(activeConnection.id, sql);
      // Dispatch custom event so ResultsGrid picks it up
      window.dispatchEvent(new CustomEvent<QueryResult>("query-result", { detail: result }));
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : "Query failed";
      window.dispatchEvent(
        new CustomEvent<QueryResult>("query-result", {
          detail: { columns: ["Error"], rows: [[msg]], rowcount: 0, status: "error" },
        })
      );
    } finally {
      setRunning(false);
    }
  }, [activeConnection, sql]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    // Ctrl+Enter or F5 to run
    if ((e.ctrlKey && e.key === "Enter") || e.key === "F5") {
      e.preventDefault();
      handleRun();
    }
    // Tab inserts spaces
    if (e.key === "Tab") {
      e.preventDefault();
      const ta = textareaRef.current;
      if (ta) {
        const start = ta.selectionStart;
        const end = ta.selectionEnd;
        const newValue = sql.substring(0, start) + "  " + sql.substring(end);
        setSql(newValue);
        setTimeout(() => {
          ta.selectionStart = ta.selectionEnd = start + 2;
        }, 0);
      }
    }
  };

  return (
    <div className="h-full flex flex-col" style={{ background: "var(--editor-bg)" }}>
      {/* Editor toolbar */}
      <div
        className="flex items-center gap-2 px-3 py-1 shrink-0"
        style={{ background: "var(--toolbar-bg)", borderBottom: "1px solid var(--border)" }}
      >
        <span className="text-xs font-medium" style={{ color: "var(--muted-foreground)" }}>
          Query
        </span>
        <div className="flex-1" />
        <button
          onClick={handleRun}
          disabled={running || !activeConnection}
          className="flex items-center gap-1 rounded px-3 py-1 text-xs font-medium transition-colors disabled:opacity-40"
          style={{ background: "#22c55e", color: "#000" }}
          title="Run Query (Ctrl+Enter)"
        >
          <span>▶</span>
          <span>{running ? "Running..." : "Run"}</span>
        </button>
      </div>

      {/* SQL textarea - will be replaced by CodeMirror in Phase 3 */}
      <div className="flex-1 relative">
        {/* Line numbers gutter */}
        <div className="absolute left-0 top-0 bottom-0 w-10 overflow-hidden select-none" style={{ background: "#1a1a1a", borderRight: "1px solid var(--border)" }}>
          <div className="pt-2 text-right pr-2">
            {sql.split("\n").map((_, i) => (
              <div key={i} className="text-[11px] leading-[20px]" style={{ color: "var(--muted-foreground)" }}>
                {i + 1}
              </div>
            ))}
          </div>
        </div>
        <textarea
          ref={textareaRef}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          className="w-full h-full resize-none outline-none pl-12 pt-2 pr-3 text-sm leading-[20px]"
          style={{
            background: "transparent",
            color: "#d4d4d4",
            fontFamily: "'Cascadia Code', 'Fira Code', 'Consolas', monospace",
            caretColor: "#fff",
          }}
          spellCheck={false}
          placeholder="Enter SQL query..."
        />
      </div>
    </div>
  );
}
