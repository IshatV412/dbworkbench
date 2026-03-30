"use client";

import { useState } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { createConnection, updateConnection, deleteConnection, testConnection } from "@/lib/api";
import type { ConnectionProfile } from "@/lib/api";

interface ConnectionModalProps {
  onClose: () => void;
  editConnection?: ConnectionProfile | null;
}

export function ConnectionModal({ onClose, editConnection }: ConnectionModalProps) {
  const { refreshConnections, setActiveConnection } = useWorkspace();
  const isEdit = !!editConnection;

  const [form, setForm] = useState({
    name: editConnection?.name ?? "",
    host: editConnection?.host ?? "localhost",
    port: String(editConnection?.port ?? 5432),
    database_name: editConnection?.database_name ?? "",
    db_username: editConnection?.db_username ?? "postgres",
    db_password: "",
  });
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [testResult, setTestResult] = useState<"success" | "fail" | null>(null);
  const [testLoading, setTestLoading] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      if (isEdit && editConnection) {
        const payload: Record<string, unknown> = {};
        if (form.name !== editConnection.name) payload.name = form.name;
        if (form.host !== editConnection.host) payload.host = form.host;
        if (parseInt(form.port) !== editConnection.port) payload.port = parseInt(form.port);
        if (form.database_name !== editConnection.database_name) payload.database_name = form.database_name;
        if (form.db_username !== editConnection.db_username) payload.db_username = form.db_username;
        if (form.db_password) payload.db_password = form.db_password;

        const updated = await updateConnection(editConnection.id, payload);
        await refreshConnections();
        setActiveConnection(updated);
      } else {
        const conn = await createConnection({
          ...form,
          port: parseInt(form.port, 10),
        });
        await refreshConnections();
        setActiveConnection(conn);
      }
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to save connection");
    } finally {
      setLoading(false);
    }
  };

  const handleTest = async () => {
    if (!editConnection) return;
    setTestLoading(true);
    setTestResult(null);
    try {
      await testConnection(editConnection.id);
      setTestResult("success");
    } catch {
      setTestResult("fail");
    } finally {
      setTestLoading(false);
    }
  };

  const handleDelete = async () => {
    if (!editConnection) return;
    setLoading(true);
    try {
      await deleteConnection(editConnection.id);
      await refreshConnections();
      setActiveConnection(null);
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to delete");
    } finally {
      setLoading(false);
    }
  };

  const update = (field: string, value: string) => setForm((f) => ({ ...f, [field]: value }));

  const fields = [
    ["name", "Connection Name", "My Database", "text"],
    ["host", "Host", "localhost", "text"],
    ["port", "Port", "5432", "number"],
    ["database_name", "Database Name", "postgres", "text"],
    ["db_username", "Username", "postgres", "text"],
    ["db_password", "Password", isEdit ? "(unchanged)" : "••••••", "password"],
  ] as const;

  return (
    <div
      className="fixed inset-0 flex items-center justify-center z-50"
      style={{ background: "rgba(0,0,0,0.6)" }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div
        className="w-full max-w-lg rounded-lg p-6 shadow-2xl"
        style={{ background: "var(--card)", border: "1px solid var(--border)" }}
      >
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold" style={{ color: "var(--foreground)" }}>
            {isEdit ? "Edit Connection" : "New Connection"}
          </h2>
          {isEdit && (
            <button
              onClick={handleTest}
              disabled={testLoading}
              className="rounded px-3 py-1 text-xs font-medium transition-colors disabled:opacity-50"
              style={{
                background: testResult === "success" ? "rgba(34,197,94,0.15)" :
                             testResult === "fail" ? "rgba(239,68,68,0.15)" : "var(--muted)",
                color: testResult === "success" ? "#22c55e" :
                       testResult === "fail" ? "var(--destructive)" : "var(--foreground)",
                border: `1px solid ${testResult === "success" ? "#22c55e" :
                         testResult === "fail" ? "var(--destructive)" : "var(--border)"}`,
              }}
            >
              {testLoading ? "Testing..." : testResult === "success" ? "✓ Connected" : testResult === "fail" ? "✗ Failed" : "Test Connection"}
            </button>
          )}
        </div>

        {error && (
          <div
            className="mb-4 rounded-md px-3 py-2 text-sm"
            style={{ background: "rgba(239,68,68,0.1)", color: "var(--destructive)" }}
          >
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-3">
          {fields.map(([field, label, placeholder, type]) => (
            <div key={field}>
              <label className="block text-xs font-medium mb-1" style={{ color: "var(--muted-foreground)" }}>
                {label}
              </label>
              <input
                type={type}
                value={form[field]}
                onChange={(e) => update(field, e.target.value)}
                required={field !== "db_password"}
                className="w-full rounded-md px-3 py-1.5 text-sm outline-none focus:ring-1"
                style={{
                  background: "var(--input)",
                  border: "1px solid var(--border)",
                  color: "var(--foreground)",
                }}
                placeholder={placeholder}
              />
            </div>
          ))}

          <div className="flex items-center justify-between pt-3">
            <div>
              {isEdit && !confirmDelete && (
                <button
                  type="button"
                  onClick={() => setConfirmDelete(true)}
                  className="rounded px-3 py-1.5 text-xs transition-colors hover:opacity-80"
                  style={{ color: "var(--destructive)" }}
                >
                  Delete Connection
                </button>
              )}
              {isEdit && confirmDelete && (
                <div className="flex items-center gap-2">
                  <span className="text-xs" style={{ color: "var(--destructive)" }}>Sure?</span>
                  <button
                    type="button"
                    onClick={handleDelete}
                    disabled={loading}
                    className="rounded px-2 py-1 text-xs font-medium"
                    style={{ background: "var(--destructive)", color: "var(--destructive-foreground)" }}
                  >
                    Yes
                  </button>
                  <button
                    type="button"
                    onClick={() => setConfirmDelete(false)}
                    className="rounded px-2 py-1 text-xs"
                    style={{ color: "var(--muted-foreground)" }}
                  >
                    No
                  </button>
                </div>
              )}
            </div>

            <div className="flex gap-2">
              <button
                type="button"
                onClick={onClose}
                className="rounded px-4 py-1.5 text-sm"
                style={{ background: "var(--secondary)", color: "var(--secondary-foreground)" }}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={loading}
                className="rounded px-4 py-1.5 text-sm font-medium disabled:opacity-50"
                style={{ background: "var(--primary)", color: "var(--primary-foreground)" }}
              >
                {loading ? "Saving..." : isEdit ? "Save" : "Create"}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
