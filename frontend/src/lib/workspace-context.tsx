"use client";

import { createContext, useContext, useState, useCallback, useRef } from "react";
import type { ConnectionProfile } from "@/lib/api";
import { listConnections } from "@/lib/api";

type EventCallback = (data: unknown) => void;

export interface SchemaMetadata {
  schemas: string[];
  tables: Record<string, string[]>;
  columns: Record<string, string[]>;
}

interface WorkspaceState {
  connections: ConnectionProfile[];
  activeConnection: ConnectionProfile | null;
  setActiveConnection: (conn: ConnectionProfile | null) => void;
  refreshConnections: () => Promise<void>;
  isLoadingConnections: boolean;
  /** Schema metadata for autocomplete */
  schemaMeta: SchemaMetadata;
  updateSchemaMeta: (meta: Partial<SchemaMetadata>) => void;
  /** Fire an event that other components can listen to */
  emit: (event: string, data?: unknown) => void;
  /** Subscribe to an event, returns unsubscribe fn */
  on: (event: string, cb: EventCallback) => () => void;
}

const WorkspaceContext = createContext<WorkspaceState | null>(null);

export function WorkspaceProvider({ children }: { children: React.ReactNode }) {
  const [connections, setConnections] = useState<ConnectionProfile[]>([]);
  const [activeConnection, setActiveConnection] = useState<ConnectionProfile | null>(null);
  const [isLoadingConnections, setIsLoadingConnections] = useState(false);
  const [schemaMeta, setSchemaMeta] = useState<SchemaMetadata>({ schemas: [], tables: {}, columns: {} });
  const listenersRef = useRef<Map<string, Set<EventCallback>>>(new Map());

  const activeConnectionRef = useRef(activeConnection);
  activeConnectionRef.current = activeConnection;

  const refreshConnections = useCallback(async () => {
    setIsLoadingConnections(true);
    try {
      const data = await listConnections();
      setConnections(data);
      // If active connection was deleted, clear it
      if (activeConnectionRef.current && !data.find((c) => c.id === activeConnectionRef.current!.id)) {
        setActiveConnection(null);
      }
    } finally {
      setIsLoadingConnections(false);
    }
  }, []);

  const emit = useCallback((event: string, data?: unknown) => {
    const cbs = listenersRef.current.get(event);
    if (cbs) cbs.forEach((cb) => cb(data));
  }, []);

  const updateSchemaMeta = useCallback((partial: Partial<SchemaMetadata>) => {
    setSchemaMeta((prev) => ({
      schemas: partial.schemas ?? prev.schemas,
      tables: partial.tables ? { ...prev.tables, ...partial.tables } : prev.tables,
      columns: partial.columns ? { ...prev.columns, ...partial.columns } : prev.columns,
    }));
  }, []);

  const on = useCallback((event: string, cb: EventCallback) => {
    if (!listenersRef.current.has(event)) {
      listenersRef.current.set(event, new Set());
    }
    listenersRef.current.get(event)!.add(cb);
    return () => {
      listenersRef.current.get(event)?.delete(cb);
    };
  }, []);

  return (
    <WorkspaceContext.Provider
      value={{
        connections, activeConnection, setActiveConnection,
        refreshConnections, isLoadingConnections,
        schemaMeta, updateSchemaMeta,
        emit, on,
      }}
    >
      {children}
    </WorkspaceContext.Provider>
  );
}

export function useWorkspace() {
  const ctx = useContext(WorkspaceContext);
  if (!ctx) throw new Error("useWorkspace must be used within WorkspaceProvider");
  return ctx;
}
