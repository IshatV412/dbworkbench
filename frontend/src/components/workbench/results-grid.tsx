"use client";

import { useState, useEffect } from "react";
import type { QueryResult } from "@/lib/api";

interface QueryResultWithTiming extends QueryResult {
  elapsed?: number;
}

type ResultTab = "data" | "messages";

export function ResultsGrid() {
  const [result, setResult] = useState<QueryResultWithTiming | null>(null);
  const [activeTab, setActiveTab] = useState<ResultTab>("data");

  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent<QueryResultWithTiming>).detail;
      setResult(detail);
      setActiveTab(detail.status === "error" ? "messages" : "data");
    };
    window.addEventListener("query-result", handler);
    return () => window.removeEventListener("query-result", handler);
  }, []);

  return (
    <div className="h-full flex flex-col" style={{ background: "var(--panel-bg)" }}>
      {/* Header tabs */}
      <div
        className="flex items-center gap-1 px-2 py-1 shrink-0"
        style={{ background: "var(--toolbar-bg)", borderBottom: "1px solid var(--border)" }}
      >
        {(["data", "messages"] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className="px-3 py-0.5 text-xs font-medium rounded-sm transition-colors"
            style={{
              color: activeTab === tab ? "var(--foreground)" : "var(--muted-foreground)",
              background: activeTab === tab ? "var(--sidebar-active)" : "transparent",
              borderBottom: activeTab === tab ? "2px solid var(--primary)" : "2px solid transparent",
            }}
          >
            {tab === "data" ? "Data Output" : "Messages"}
            {tab === "messages" && result?.status === "error" && (
              <span className="ml-1 text-[9px]" style={{ color: "var(--destructive)" }}>●</span>
            )}
          </button>
        ))}
        {result?.elapsed != null && (
          <span className="ml-auto text-[10px] pr-2" style={{ color: "var(--muted-foreground)" }}>
            {result.elapsed < 1000
              ? `${result.elapsed} ms`
              : `${(result.elapsed / 1000).toFixed(2)} s`}
          </span>
        )}
      </div>

      {/* Tab content */}
      <div className="flex-1 overflow-auto">
        {!result && (
          <div className="flex items-center justify-center h-full text-sm" style={{ color: "var(--muted-foreground)" }}>
            No data output. Execute a query to see results.
          </div>
        )}

        {/* Messages tab */}
        {result && activeTab === "messages" && (
          <div className="p-3 text-xs font-mono" style={{ color: result.status === "error" ? "var(--destructive)" : "var(--foreground)" }}>
            {result.status === "error" ? (
              <div>
                <div className="font-semibold mb-1">ERROR</div>
                <div style={{ whiteSpace: "pre-wrap" }}>{result.rows[0]?.[0] as string}</div>
              </div>
            ) : (
              <div>
                <div style={{ color: "#22c55e" }}>
                  Query returned successfully: {result.rowcount} row(s)
                  {result.elapsed != null && ` in ${result.elapsed} ms`}
                </div>
                {result.columns.length === 0 && (
                  <div className="mt-1">{result.rowcount} row(s) affected.</div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Data tab */}
        {result && activeTab === "data" && result.status === "error" && (
          <div className="p-3 text-sm" style={{ color: "var(--destructive)" }}>
            {result.rows[0]?.[0] as string}
          </div>
        )}

        {result && activeTab === "data" && result.status === "success" && result.columns.length > 0 && (
          <table className="w-full text-xs border-collapse" style={{ fontFamily: "monospace" }}>
            <thead>
              <tr>
                <th
                  className="sticky top-0 px-3 py-1.5 text-left font-semibold border-r"
                  style={{
                    background: "var(--grid-header)",
                    borderColor: "var(--grid-border)",
                    color: "var(--foreground)",
                    width: 40,
                  }}
                >
                  #
                </th>
                {result.columns.map((col, i) => (
                  <th
                    key={i}
                    className="sticky top-0 px-3 py-1.5 text-left font-semibold border-r"
                    style={{
                      background: "var(--grid-header)",
                      borderColor: "var(--grid-border)",
                      color: "var(--foreground)",
                      minWidth: 100,
                    }}
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, ri) => (
                <tr
                  key={ri}
                  className="hover:opacity-80"
                  style={{ background: ri % 2 === 0 ? "transparent" : "var(--grid-row-alt)" }}
                >
                  <td
                    className="px-3 py-1 border-r border-b"
                    style={{ borderColor: "var(--grid-border)", color: "var(--muted-foreground)" }}
                  >
                    {ri + 1}
                  </td>
                  {row.map((cell, ci) => (
                    <td
                      key={ci}
                      className="px-3 py-1 border-r border-b"
                      style={{ borderColor: "var(--grid-border)", color: "var(--foreground)" }}
                    >
                      {cell === null ? (
                        <span style={{ color: "var(--muted-foreground)", fontStyle: "italic" }}>[null]</span>
                      ) : (
                        String(cell)
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}

        {result && activeTab === "data" && result.status === "success" && result.columns.length === 0 && (
          <div className="p-3 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Query OK. {result.rowcount} row(s) affected.
          </div>
        )}
      </div>

      {/* Footer */}
      {result && result.status === "success" && (
        <div
          className="flex items-center justify-between px-3 py-1 text-[11px] shrink-0"
          style={{ color: "var(--muted-foreground)", borderTop: "1px solid var(--border)" }}
        >
          <span>Rows: {result.rowcount}</span>
          {result.elapsed != null && (
            <span>Execution: {result.elapsed} ms</span>
          )}
        </div>
      )}
    </div>
  );
}
