"use client";

import { useEffect } from "react";
import { useAuth } from "@/lib/auth-context";
import { useWorkspace } from "@/lib/workspace-context";
import { Toolbar } from "@/components/workbench/toolbar";
import { Sidebar } from "@/components/workbench/sidebar";
import { CenterPanel } from "@/components/workbench/center-panel";
import { VersionPanel } from "@/components/workbench/version-panel";
import { StatusBar } from "@/components/workbench/status-bar";
import { ResizableLayout } from "@/components/workbench/resizable-layout";

export function WorkbenchShell() {
  const { isAuthenticated } = useAuth();
  const { refreshConnections } = useWorkspace();

  useEffect(() => {
    if (isAuthenticated) {
      refreshConnections();
    }
  }, [isAuthenticated, refreshConnections]);

  return (
    <div className="h-screen flex flex-col overflow-hidden" style={{ background: "var(--background)" }}>
      <Toolbar />
      <ResizableLayout
        sidebar={<Sidebar />}
        center={<CenterPanel />}
        rightPanel={<VersionPanel />}
      />
      <StatusBar />
    </div>
  );
}
