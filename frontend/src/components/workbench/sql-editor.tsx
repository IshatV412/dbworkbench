"use client";

import { useState, useRef, useCallback, useEffect, useMemo } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { executeSQL, type QueryResult } from "@/lib/api";
import dynamic from "next/dynamic";
import type { SchemaCompletionData } from "./codemirror-editor";

// Dynamically import CodeMirror to avoid SSR issues
const CodeMirrorEditor = dynamic(
  () => import("./codemirror-editor").then((m) => ({ default: m.CodeMirrorEditor })),
  { ssr: false, loading: () => <div className="flex-1" style={{ background: "var(--editor-bg)" }} /> }
);

interface QueryTab {
  id: string;
  title: string;
  sql: string;
}

let tabCounter = 1;
function newTab(sql = ""): QueryTab {
  return { id: `tab-${tabCounter++}`, title: `Query ${tabCounter - 1}`, sql };
}

export interface QueryResultWithTiming extends QueryResult {
  elapsed?: number;
}

export function SqlEditor() {
  const { activeConnection, on, schemaMeta } = useWorkspace();
  const [tabs, setTabs] = useState<QueryTab[]>(() => [newTab("SELECT 1;")]);
  const [activeTabId, setActiveTabId] = useState<string>(tabs[0].id);
  const [running, setRunning] = useState(false);
  const runRef = useRef<(overrideSql?: string) => void>(null);

  const activeTab = tabs.find((t) => t.id === activeTabId) ?? tabs[0];

  const updateTabSql = useCallback((sql: string) => {
    setTabs((prev) => prev.map((t) => (t.id === activeTabId ? { ...t, sql } : t)));
  }, [activeTabId]);

  const addTab = useCallback((sql = "") => {
    const t = newTab(sql);
    setTabs((prev) => [...prev, t]);
    setActiveTabId(t.id);
  }, []);

  const closeTab = useCallback((id: string) => {
    setTabs((prev) => {
      if (prev.length <= 1) return prev; // keep at least one
      const filtered = prev.filter((t) => t.id !== id);
      if (activeTabId === id) {
        setActiveTabId(filtered[filtered.length - 1].id);
      }
      return filtered;
    });
  }, [activeTabId]);

  const handleRun = useCallback(async (overrideSql?: string) => {
    const query = overrideSql ?? activeTab.sql;
    if (!activeConnection || !query.trim()) return;
    setRunning(true);
    const start = performance.now();
    try {
      const result = await executeSQL(activeConnection.id, query);
      const elapsed = Math.round(performance.now() - start);
      window.dispatchEvent(new CustomEvent<QueryResultWithTiming>("query-result", {
        detail: { ...result, elapsed },
      }));
    } catch (err: unknown) {
      const elapsed = Math.round(performance.now() - start);
      const msg = err instanceof Error ? err.message : "Query failed";
      window.dispatchEvent(
        new CustomEvent<QueryResultWithTiming>("query-result", {
          detail: { columns: ["Error"], rows: [[msg]], rowcount: 0, status: "error", elapsed },
        })
      );
    } finally {
      setRunning(false);
    }
  }, [activeConnection, activeTab.sql]);

  runRef.current = handleRun;

  // Listen for "run-query" events from Object Explorer
  useEffect(() => {
    const unsub = on("run-query", (data: unknown) => {
      const query = data as string;
      // Set SQL in current tab and run
      setTabs((prev) => prev.map((t) => (t.id === activeTabId ? { ...t, sql: query } : t)));
      runRef.current?.(query);
    });
    return unsub;
  }, [on, activeTabId]);

  // Build schema completion data from workspace metadata
  const schemaData: SchemaCompletionData = useMemo(() => ({
    schemas: schemaMeta.schemas,
    tables: schemaMeta.tables,
    columns: schemaMeta.columns,
  }), [schemaMeta]);

  return (
    <div className="h-full flex flex-col" style={{ background: "var(--editor-bg)" }}>
      {/* Tab bar + toolbar */}
      <div
        className="flex items-center shrink-0"
        style={{ background: "var(--toolbar-bg)", borderBottom: "1px solid var(--border)" }}
      >
        {/* Tabs */}
        <div className="flex items-center flex-1 overflow-x-auto min-w-0">
          {tabs.map((tab) => (
            <div
              key={tab.id}
              className="flex items-center gap-1 px-3 py-1.5 cursor-pointer text-xs shrink-0 border-r transition-colors"
              style={{
                background: tab.id === activeTabId ? "var(--editor-bg)" : "transparent",
                borderColor: "var(--border)",
                color: tab.id === activeTabId ? "var(--foreground)" : "var(--muted-foreground)",
                borderBottom: tab.id === activeTabId ? "2px solid var(--primary)" : "2px solid transparent",
              }}
              onClick={() => setActiveTabId(tab.id)}
            >
              <span className="truncate max-w-[120px]">{tab.title}</span>
              {tabs.length > 1 && (
                <button
                  className="ml-1 hover:opacity-100 opacity-50 text-[10px]"
                  onClick={(e) => { e.stopPropagation(); closeTab(tab.id); }}
                  title="Close tab"
                >
                  ✕
                </button>
              )}
            </div>
          ))}
          <button
            onClick={() => addTab()}
            className="px-2 py-1.5 text-xs hover:opacity-80 shrink-0"
            style={{ color: "var(--muted-foreground)" }}
            title="New query tab"
          >
            +
          </button>
        </div>

        {/* Run button */}
        <div className="flex items-center gap-2 px-3 shrink-0">
          <button
            onClick={() => handleRun()}
            disabled={running || !activeConnection}
            className="flex items-center gap-1 rounded px-3 py-1 text-xs font-medium transition-colors disabled:opacity-40"
            style={{ background: "#22c55e", color: "#000" }}
            title="Run Query (Ctrl+Enter / F5)"
          >
            <span>▶</span>
            <span>{running ? "Running..." : "Run"}</span>
          </button>
        </div>
      </div>

      {/* CodeMirror editor */}
      <div className="flex-1 min-h-0">
        <CodeMirrorEditor
          value={activeTab.sql}
          onChange={updateTabSql}
          onRun={() => handleRun()}
          placeholder="Enter SQL query..."
          schemaData={schemaData}
        />
      </div>
    </div>
  );
}
