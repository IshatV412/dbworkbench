"use client";

import { useState, useEffect, useCallback } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { executeSQL } from "@/lib/api";

interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  isPk: boolean;
}

interface TreeItem {
  name: string;
  type: "table" | "view" | "function" | "sequence";
  columns?: ColumnInfo[];
  columnsLoaded?: boolean;
}

interface CategoryNode {
  name: string;
  items: TreeItem[];
  loaded: boolean;
}

interface SchemaNode {
  name: string;
  categories: CategoryNode[];
}

const QUERIES = {
  schemas: `SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast') 
            ORDER BY schema_name`,
  tables: (schema: string) =>
    `SELECT table_name FROM information_schema.tables 
     WHERE table_schema='${schema}' AND table_type='BASE TABLE' ORDER BY table_name`,
  views: (schema: string) =>
    `SELECT table_name FROM information_schema.views 
     WHERE table_schema='${schema}' ORDER BY table_name`,
  functions: (schema: string) =>
    `SELECT routine_name FROM information_schema.routines 
     WHERE routine_schema='${schema}' AND routine_type='FUNCTION' ORDER BY routine_name`,
  sequences: (schema: string) =>
    `SELECT sequence_name FROM information_schema.sequences 
     WHERE sequence_schema='${schema}' ORDER BY sequence_name`,
  columns: (schema: string, table: string) =>
    `SELECT c.column_name, c.data_type, c.is_nullable,
       CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_pk
     FROM information_schema.columns c
     LEFT JOIN information_schema.key_column_usage kcu 
       ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name
     LEFT JOIN information_schema.table_constraints tc 
       ON kcu.constraint_name = tc.constraint_name AND tc.constraint_type = 'PRIMARY KEY'
     WHERE c.table_schema='${schema}' AND c.table_name='${table}'
     ORDER BY c.ordinal_position`,
  tableRowCount: (schema: string, table: string) =>
    `SELECT count(*) FROM "${schema}"."${table}"`,
};

const CATEGORY_NAMES = ["Tables", "Views", "Functions", "Sequences"] as const;
type CategoryName = typeof CATEGORY_NAMES[number];

const CATEGORY_QUERY: Record<CategoryName, (s: string) => string> = {
  Tables: QUERIES.tables,
  Views: QUERIES.views,
  Functions: QUERIES.functions,
  Sequences: QUERIES.sequences,
};

const CATEGORY_TYPE: Record<CategoryName, TreeItem["type"]> = {
  Tables: "table",
  Views: "view",
  Functions: "function",
  Sequences: "sequence",
};

export function Sidebar() {
  const { activeConnection, emit } = useWorkspace();
  const [schemas, setSchemas] = useState<SchemaNode[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [selectedItem, setSelectedItem] = useState<string | null>(null);

  const loadSchemas = useCallback(async () => {
    if (!activeConnection) { setSchemas([]); return; }
    setLoading(true);
    try {
      const result = await executeSQL(activeConnection.id, QUERIES.schemas);
      setSchemas(result.rows.map((row) => ({
        name: String(row[0]),
        categories: CATEGORY_NAMES.map((name) => ({ name, items: [], loaded: false })),
      })));
      setExpanded(new Set());
    } catch {
      setSchemas([]);
    } finally {
      setLoading(false);
    }
  }, [activeConnection]);

  useEffect(() => { loadSchemas(); }, [loadSchemas]);

  const loadCategory = async (schemaName: string, catName: CategoryName) => {
    if (!activeConnection) return;
    try {
      const result = await executeSQL(activeConnection.id, CATEGORY_QUERY[catName](schemaName));
      setSchemas((prev) => prev.map((s) => {
        if (s.name !== schemaName) return s;
        return { ...s, categories: s.categories.map((c) => {
          if (c.name !== catName) return c;
          return { ...c, loaded: true, items: result.rows.map((row) => ({
            name: String(row[0]),
            type: CATEGORY_TYPE[catName],
          })) };
        }) };
      }));
    } catch { /* ignore */ }
  };

  const loadColumns = async (schemaName: string, tableName: string) => {
    if (!activeConnection) return;
    try {
      const result = await executeSQL(activeConnection.id, QUERIES.columns(schemaName, tableName));
      const columns: ColumnInfo[] = result.rows.map((row) => ({
        name: String(row[0]),
        type: String(row[1]),
        nullable: row[2] === "YES",
        isPk: row[3] === true || row[3] === "true",
      }));
      setSchemas((prev) => prev.map((s) => {
        if (s.name !== schemaName) return s;
        return { ...s, categories: s.categories.map((c) => ({
          ...c, items: c.items.map((item) => {
            if (item.name !== tableName) return item;
            return { ...item, columns, columnsLoaded: true };
          }),
        })) };
      }));
    } catch { /* ignore */ }
  };

  const toggle = (key: string, onExpand?: () => void) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) { next.delete(key); }
      else { next.add(key); onExpand?.(); }
      return next;
    });
  };

  const handleTableClick = (schemaName: string, tableName: string) => {
    const sql = `SELECT * FROM "${schemaName}"."${tableName}" LIMIT 100;`;
    setSelectedItem(`${schemaName}.${tableName}`);
    emit("run-query", sql);
  };

  const handleTableDblClick = (schemaName: string, tableName: string, type: string) => {
    const key = `${schemaName}.${type === "table" ? "Tables" : "Views"}.${tableName}`;
    if (!expanded.has(key)) {
      toggle(key, () => loadColumns(schemaName, tableName));
    }
  };

  const iconFor = (type: string) => {
    switch (type) {
      case "table": return "🗃️";
      case "view": return "👁️";
      case "function": return "⚡";
      case "sequence": return "🔢";
      default: return "📄";
    }
  };

  return (
    <div className="h-full flex flex-col overflow-hidden" style={{ background: "var(--sidebar-bg)" }}>
      {/* Header */}
      <div
        className="flex items-center justify-between px-3 py-2 shrink-0"
        style={{ background: "var(--sidebar-header)", borderBottom: "1px solid var(--border)" }}
      >
        <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: "var(--muted-foreground)" }}>
          Object Explorer
        </span>
        {activeConnection && (
          <button
            onClick={loadSchemas}
            className="text-[10px] px-1.5 py-0.5 rounded hover:opacity-80"
            style={{ background: "var(--muted)", color: "var(--muted-foreground)" }}
            title="Refresh"
          >
            ↻
          </button>
        )}
      </div>

      {/* Connection info */}
      {activeConnection && (
        <div className="px-3 py-1.5 shrink-0" style={{ borderBottom: "1px solid var(--border)" }}>
          <div className="flex items-center gap-1.5 text-[11px]">
            <span className="w-1.5 h-1.5 rounded-full shrink-0" style={{ background: "#22c55e" }} />
            <span className="truncate font-medium" style={{ color: "var(--foreground)" }}>
              {activeConnection.database_name}
            </span>
          </div>
          <div className="text-[10px] truncate mt-0.5" style={{ color: "var(--muted-foreground)" }}>
            {activeConnection.db_username}@{activeConnection.host}:{activeConnection.port}
          </div>
        </div>
      )}

      {/* Tree */}
      <div className="flex-1 overflow-y-auto py-1 text-xs">
        {!activeConnection && (
          <div className="px-3 py-8 text-center" style={{ color: "var(--muted-foreground)" }}>
            Select a connection to browse
          </div>
        )}

        {loading && (
          <div className="px-3 py-4" style={{ color: "var(--muted-foreground)" }}>Loading schemas...</div>
        )}

        {schemas.map((schema) => {
          const sKey = schema.name;
          const sOpen = expanded.has(sKey);

          return (
            <div key={sKey}>
              {/* Schema */}
              <button
                onClick={() => toggle(sKey)}
                className="w-full flex items-center gap-1.5 px-2 py-[3px] text-left hover:brightness-125 transition-all"
                style={{ color: "var(--foreground)" }}
              >
                <span className="text-[9px] w-3 text-center">{sOpen ? "▼" : "▶"}</span>
                <span>📁</span>
                <span className="font-medium">{schema.name}</span>
              </button>

              {sOpen && schema.categories.map((cat) => {
                const cKey = `${sKey}.${cat.name}`;
                const cOpen = expanded.has(cKey);

                return (
                  <div key={cKey}>
                    {/* Category */}
                    <button
                      onClick={() => toggle(cKey, () => { if (!cat.loaded) loadCategory(sKey, cat.name as CategoryName); })}
                      className="w-full flex items-center gap-1.5 pl-5 pr-2 py-[3px] text-left hover:brightness-125"
                      style={{ color: "var(--foreground)" }}
                    >
                      <span className="text-[9px] w-3 text-center">{cOpen ? "▼" : "▶"}</span>
                      <span>📂</span>
                      <span>{cat.name}</span>
                      {cat.loaded && (
                        <span className="ml-auto text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                          ({cat.items.length})
                        </span>
                      )}
                    </button>

                    {cOpen && cat.items.map((item) => {
                      const iKey = `${cKey}.${item.name}`;
                      const iOpen = expanded.has(iKey);
                      const isSelected = selectedItem === `${sKey}.${item.name}`;
                      const isExpandable = item.type === "table" || item.type === "view";

                      return (
                        <div key={iKey}>
                          {/* Item row */}
                          <div
                            className="flex items-center gap-1.5 pl-9 pr-2 py-[3px] cursor-pointer transition-all"
                            style={{
                              color: "var(--foreground)",
                              background: isSelected ? "var(--sidebar-active)" : "transparent",
                            }}
                            onClick={() => {
                              if (item.type === "table" || item.type === "view") {
                                handleTableClick(sKey, item.name);
                              }
                            }}
                            onDoubleClick={() => {
                              if (isExpandable) handleTableDblClick(sKey, item.name, item.type);
                            }}
                            title={`${sKey}.${item.name} — Click to query, Double-click to expand columns`}
                          >
                            {isExpandable && (
                              <span
                                className="text-[9px] w-3 text-center cursor-pointer"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  toggle(iKey, () => { if (!item.columnsLoaded) loadColumns(sKey, item.name); });
                                }}
                              >
                                {iOpen ? "▼" : "▶"}
                              </span>
                            )}
                            {!isExpandable && <span className="w-3" />}
                            <span>{iconFor(item.type)}</span>
                            <span className={isSelected ? "font-medium" : ""}>{item.name}</span>
                          </div>

                          {/* Columns */}
                          {iOpen && isExpandable && item.columns && item.columns.map((col) => (
                            <div
                              key={`${iKey}.${col.name}`}
                              className="flex items-center gap-1.5 pl-14 pr-2 py-[2px]"
                              style={{ color: "var(--muted-foreground)" }}
                              title={`${col.name}: ${col.type}${col.nullable ? "" : " NOT NULL"}${col.isPk ? " (PK)" : ""}`}
                            >
                              {col.isPk ? (
                                <span className="text-[9px]" style={{ color: "#f59e0b" }}>🔑</span>
                              ) : (
                                <span className="text-[9px]">│</span>
                              )}
                              <span className="truncate">{col.name}</span>
                              <span className="ml-auto text-[10px] shrink-0" style={{ color: "#6b7280" }}>
                                {col.type}
                              </span>
                            </div>
                          ))}

                          {iOpen && isExpandable && !item.columnsLoaded && (
                            <div className="pl-14 pr-2 py-[2px] text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                              Loading...
                            </div>
                          )}

                          {iOpen && isExpandable && item.columnsLoaded && item.columns?.length === 0 && (
                            <div className="pl-14 pr-2 py-[2px] text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                              (no columns)
                            </div>
                          )}
                        </div>
                      );
                    })}

                    {cOpen && cat.loaded && cat.items.length === 0 && (
                      <div className="pl-9 pr-2 py-[3px] text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                        (empty)
                      </div>
                    )}

                    {cOpen && !cat.loaded && (
                      <div className="pl-9 pr-2 py-[3px] text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                        Loading...
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}
