"use client";

import { useAuth } from "@/lib/auth-context";
import { useWorkspace } from "@/lib/workspace-context";

export function StatusBar() {
  const { username } = useAuth();
  const { activeConnection } = useWorkspace();

  return (
    <div
      className="flex items-center justify-between px-3 py-0.5 text-[11px] select-none shrink-0"
      style={{ background: "var(--status-bar)", color: "#fff" }}
    >
      <div className="flex items-center gap-3">
        {activeConnection ? (
          <>
            <span>⚡ Connected</span>
            <span>
              {activeConnection.db_username}@{activeConnection.host}:{activeConnection.port}/{activeConnection.database_name}
            </span>
          </>
        ) : (
          <span>No connection selected</span>
        )}
      </div>
      <div className="flex items-center gap-3">
        <span>{username}</span>
        <span>WEAVE-DB v0.3.0</span>
      </div>
    </div>
  );
}
