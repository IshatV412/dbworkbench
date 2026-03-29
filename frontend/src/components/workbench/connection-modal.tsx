"use client";

import { useState } from "react";
import { useWorkspace } from "@/lib/workspace-context";
import { createConnection } from "@/lib/api";

interface ConnectionModalProps {
  onClose: () => void;
}

export function ConnectionModal({ onClose }: ConnectionModalProps) {
  const { refreshConnections, setActiveConnection } = useWorkspace();
  const [form, setForm] = useState({
    name: "",
    host: "localhost",
    port: "5432",
    database_name: "",
    db_username: "postgres",
    db_password: "",
  });
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const conn = await createConnection({
        ...form,
        port: parseInt(form.port, 10),
      });
      await refreshConnections();
      setActiveConnection(conn);
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to create connection");
    } finally {
      setLoading(false);
    }
  };

  const update = (field: string, value: string) => setForm((f) => ({ ...f, [field]: value }));

  return (
    <div className="fixed inset-0 flex items-center justify-center z-50" style={{ background: "rgba(0,0,0,0.6)" }}>
      <div
        className="w-full max-w-lg rounded-lg p-6 shadow-2xl"
        style={{ background: "var(--card)", border: "1px solid var(--border)" }}
      >
        <h2 className="text-lg font-semibold mb-4" style={{ color: "var(--foreground)" }}>
          New Connection
        </h2>

        {error && (
          <div
            className="mb-4 rounded-md px-3 py-2 text-sm"
            style={{ background: "rgba(239,68,68,0.1)", color: "var(--destructive)" }}
          >
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-3">
          {([
            ["name", "Connection Name", "My Database", "text"],
            ["host", "Host", "localhost", "text"],
            ["port", "Port", "5432", "number"],
            ["database_name", "Database Name", "postgres", "text"],
            ["db_username", "Username", "postgres", "text"],
            ["db_password", "Password", "••••••", "password"],
          ] as const).map(([field, label, placeholder, type]) => (
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

          <div className="flex justify-end gap-2 pt-2">
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
              {loading ? "Connecting..." : "Create"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
