"use client";

import { useState, useEffect } from "react";
import type { QueryResult } from "@/lib/api";

export function ResultsGrid() {
  const [result, setResult] = useState<QueryResult | null>(null);

  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent<QueryResult>).detail;
      setResult(detail);
    };
    window.addEventListener("query-result", handler);
    return () => window.removeEventListener("query-result", handler);
  }, []);

  return (
    <div className="h-full flex flex-col" style={{ background: "var(--panel-bg)" }}>
      {/* Header tabs */}
      <div
        className="flex items-center gap-4 px-3 py-1.5 shrink-0"
        style={{ background: "var(--toolbar-bg)", borderBottom: "1px solid var(--border)" }}
      >
        <span
          className="text-xs font-medium pb-0.5"
          style={{ color: "var(--foreground)", borderBottom: "2px solid var(--primary)" }}
        >
          Data Output
        </span>
        <span className="text-xs" style={{ color: "var(--muted-foreground)" }}>
          Messages
        </span>
      </div>

      {/* Grid content */}
      <div className="flex-1 overflow-auto">
        {!result && (
          <div className="flex items-center justify-center h-full text-sm" style={{ color: "var(--muted-foreground)" }}>
            No data output. Execute a query to get output.
          </div>
        )}

        {result && result.status === "error" && (
          <div className="p-3 text-sm" style={{ color: "var(--destructive)" }}>
            {result.rows[0]?.[0] as string}
          </div>
        )}

        {result && result.status === "success" && result.columns.length > 0 && (
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

        {result && result.status === "success" && result.columns.length === 0 && (
          <div className="p-3 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Query OK. {result.rowcount} row(s) affected.
          </div>
        )}
      </div>

      {/* Footer */}
      {result && result.status === "success" && (
        <div
          className="px-3 py-1 text-[11px] shrink-0"
          style={{ color: "var(--muted-foreground)", borderTop: "1px solid var(--border)" }}
        >
          Total rows: {result.rowcount}
        </div>
      )}
    </div>
  );
}
