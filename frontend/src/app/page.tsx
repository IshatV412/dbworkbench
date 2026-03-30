"use client";

import { AuthProvider, useAuth } from "@/lib/auth-context";
import { WorkspaceProvider } from "@/lib/workspace-context";
import LoginPage from "@/components/auth/login-page";
import { WorkbenchShell } from "@/components/workbench/workbench-shell";

function AppContent() {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return (
      <div
        className="min-h-screen flex items-center justify-center"
        style={{ background: "var(--background)" }}
      >
        <div className="text-sm" style={{ color: "var(--muted-foreground)" }}>
          Loading...
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <LoginPage />;
  }

  return (
    <WorkspaceProvider>
      <WorkbenchShell />
    </WorkspaceProvider>
  );
}

export default function Home() {
  return (
    <AuthProvider>
      <AppContent />
    </AuthProvider>
  );
}
