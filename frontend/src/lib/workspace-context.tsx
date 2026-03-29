"use client";

import { createContext, useContext, useState, useCallback } from "react";
import type { ConnectionProfile } from "@/lib/api";
import { listConnections } from "@/lib/api";

interface WorkspaceState {
  connections: ConnectionProfile[];
  activeConnection: ConnectionProfile | null;
  setActiveConnection: (conn: ConnectionProfile | null) => void;
  refreshConnections: () => Promise<void>;
  isLoadingConnections: boolean;
}

const WorkspaceContext = createContext<WorkspaceState | null>(null);

export function WorkspaceProvider({ children }: { children: React.ReactNode }) {
  const [connections, setConnections] = useState<ConnectionProfile[]>([]);
  const [activeConnection, setActiveConnection] = useState<ConnectionProfile | null>(null);
  const [isLoadingConnections, setIsLoadingConnections] = useState(false);

  const refreshConnections = useCallback(async () => {
    setIsLoadingConnections(true);
    try {
      const data = await listConnections();
      setConnections(data);
      // If active connection was deleted, clear it
      if (activeConnection && !data.find((c) => c.id === activeConnection.id)) {
        setActiveConnection(null);
      }
    } finally {
      setIsLoadingConnections(false);
    }
  }, [activeConnection]);

  return (
    <WorkspaceContext.Provider
      value={{ connections, activeConnection, setActiveConnection, refreshConnections, isLoadingConnections }}
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
