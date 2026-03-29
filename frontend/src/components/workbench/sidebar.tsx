"use client";

import { useState, useEffect, useCallback } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { executeSQL } from "@/lib/api";

interface TreeNode {
  name: string;
  type: "schema" | "category" | "table" | "view" | "function" | "sequence";
  children?: TreeNode[];
}

// Metadata queries for PostgreSQL
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
};

export function Sidebar() {
  const { activeConnection } = useWorkspace();
  const [tree, setTree] = useState<TreeNode[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);

  const loadSchemas = useCallback(async () => {
    if (!activeConnection) {
      setTree([]);
      return;
    }
    setLoading(true);
    try {
      const result = await executeSQL(activeConnection.id, QUERIES.schemas);
      const schemas: TreeNode[] = result.rows.map((row) => ({
        name: String(row[0]),
        type: "schema" as const,
        children: [
          { name: "Tables", type: "category" as const, children: [] },
          { name: "Views", type: "category" as const, children: [] },
          { name: "Functions", type: "category" as const, children: [] },
          { name: "Sequences", type: "category" as const, children: [] },
        ],
      }));
      setTree(schemas);
    } catch {
      setTree([]);
    } finally {
      setLoading(false);
    }
  }, [activeConnection]);

  useEffect(() => {
    loadSchemas();
  }, [loadSchemas]);

  const loadCategory = async (schemaName: string, category: string) => {
    if (!activeConnection) return;
    const queryMap: Record<string, (s: string) => string> = {
      Tables: QUERIES.tables,
      Views: QUERIES.views,
      Functions: QUERIES.functions,
      Sequences: QUERIES.sequences,
    };
    const queryFn = queryMap[category];
    if (!queryFn) return;
    try {
      const result = await executeSQL(activeConnection.id, queryFn(schemaName));
      const typeMap: Record<string, TreeNode["type"]> = {
        Tables: "table",
        Views: "view",
        Functions: "function",
        Sequences: "sequence",
      };
      setTree((prev) =>
        prev.map((schema) => {
          if (schema.name !== schemaName) return schema;
          return {
            ...schema,
            children: schema.children?.map((cat) => {
              if (cat.name !== category) return cat;
              return {
                ...cat,
                children: result.rows.map((row) => ({
                  name: String(row[0]),
                  type: typeMap[category],
                })),
              };
            }),
          };
        })
      );
    } catch {
      /* ignore */
    }
  };

  const toggleExpand = (key: string, schemaName?: string, category?: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
        // Lazy-load category items
        if (schemaName && category) {
          loadCategory(schemaName, category);
        }
      }
      return next;
    });
  };

  const iconFor = (type: TreeNode["type"]) => {
    switch (type) {
      case "schema":
        return "📁";
      case "category":
        return "📂";
      case "table":
        return "🗃️";
      case "view":
        return "👁️";
      case "function":
        return "⚡";
      case "sequence":
        return "🔢";
    }
  };

  return (
    <div className="h-full flex flex-col overflow-hidden" style={{ background: "var(--sidebar-bg)" }}>
      {/* Header */}
      <div
        className="px-3 py-2 text-xs font-semibold uppercase tracking-wider shrink-0"
        style={{ color: "var(--muted-foreground)", background: "var(--sidebar-header)", borderBottom: "1px solid var(--border)" }}
      >
        Object Explorer
      </div>

      {/* Tree */}
      <div className="flex-1 overflow-y-auto py-1 text-sm">
        {!activeConnection && (
          <div className="px-3 py-8 text-center text-xs" style={{ color: "var(--muted-foreground)" }}>
            Select a connection to browse objects
          </div>
        )}

        {loading && (
          <div className="px-3 py-4 text-xs" style={{ color: "var(--muted-foreground)" }}>
            Loading...
          </div>
        )}

        {tree.map((schema) => {
          const schemaKey = schema.name;
          const isExpanded = expanded.has(schemaKey);

          return (
            <div key={schemaKey}>
              <button
                onClick={() => toggleExpand(schemaKey)}
                className="w-full flex items-center gap-1.5 px-2 py-1 text-left text-xs hover:opacity-80 transition-colors"
                style={{ color: "var(--foreground)" }}
              >
                <span className="text-[10px]">{isExpanded ? "▼" : "▶"}</span>
                <span>{iconFor("schema")}</span>
                <span className="font-medium">{schema.name}</span>
              </button>

              {isExpanded &&
                schema.children?.map((cat) => {
                  const catKey = `${schemaKey}.${cat.name}`;
                  const catExpanded = expanded.has(catKey);

                  return (
                    <div key={catKey}>
                      <button
                        onClick={() => toggleExpand(catKey, schemaKey, cat.name)}
                        className="w-full flex items-center gap-1.5 pl-6 pr-2 py-1 text-left text-xs hover:opacity-80"
                        style={{ color: "var(--foreground)" }}
                      >
                        <span className="text-[10px]">{catExpanded ? "▼" : "▶"}</span>
                        <span>{iconFor("category")}</span>
                        <span>{cat.name}</span>
                        {cat.children && cat.children.length > 0 && (
                          <span className="ml-auto text-[10px]" style={{ color: "var(--muted-foreground)" }}>
                            ({cat.children.length})
                          </span>
                        )}
                      </button>

                      {catExpanded &&
                        cat.children?.map((item) => (
                          <div
                            key={`${catKey}.${item.name}`}
                            className="flex items-center gap-1.5 pl-10 pr-2 py-1 text-xs cursor-pointer hover:opacity-80"
                            style={{ color: "var(--foreground)" }}
                            title={`${schemaKey}.${item.name}`}
                          >
                            <span>{iconFor(item.type)}</span>
                            <span>{item.name}</span>
                          </div>
                        ))}

                      {catExpanded && (!cat.children || cat.children.length === 0) && (
                        <div className="pl-10 pr-2 py-1 text-[11px]" style={{ color: "var(--muted-foreground)" }}>
                          (empty)
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
