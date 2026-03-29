"use client";

import { useState } from "react";
import { useAuth } from "@/lib/auth-context";

export default function LoginPage() {
  const { login, register } = useAuth();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [email, setEmail] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      if (mode === "login") {
        await login(username, password);
      } else {
        await register(username, password, email);
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : "Something went wrong";
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: "var(--background)" }}>
      <div
        className="w-full max-w-md rounded-lg p-8 shadow-2xl"
        style={{ background: "var(--card)", border: "1px solid var(--border)" }}
      >
        {/* Logo */}
        <div className="text-center mb-8">
          <div className="flex items-center justify-center gap-2 mb-2">
            <svg width="32" height="32" viewBox="0 0 32 32" fill="none" className="text-blue-500">
              <rect x="2" y="4" width="28" height="8" rx="2" fill="currentColor" opacity="0.9" />
              <rect x="2" y="14" width="28" height="8" rx="2" fill="currentColor" opacity="0.6" />
              <rect x="2" y="24" width="28" height="4" rx="2" fill="currentColor" opacity="0.3" />
            </svg>
            <span className="text-xl font-bold tracking-tight" style={{ color: "var(--foreground)" }}>
              WEAVE-DB
            </span>
          </div>
          <p className="text-sm" style={{ color: "var(--muted-foreground)" }}>
            Database Version-Control Workbench
          </p>
        </div>

        {/* Tabs */}
        <div className="flex mb-6 rounded-md overflow-hidden" style={{ border: "1px solid var(--border)" }}>
          <button
            type="button"
            onClick={() => setMode("login")}
            className="flex-1 py-2 text-sm font-medium transition-colors"
            style={{
              background: mode === "login" ? "var(--primary)" : "transparent",
              color: mode === "login" ? "var(--primary-foreground)" : "var(--muted-foreground)",
            }}
          >
            Sign In
          </button>
          <button
            type="button"
            onClick={() => setMode("register")}
            className="flex-1 py-2 text-sm font-medium transition-colors"
            style={{
              background: mode === "register" ? "var(--primary)" : "transparent",
              color: mode === "register" ? "var(--primary-foreground)" : "var(--muted-foreground)",
            }}
          >
            Register
          </button>
        </div>

        {error && (
          <div
            className="mb-4 rounded-md px-3 py-2 text-sm"
            style={{ background: "rgba(239,68,68,0.1)", color: "var(--destructive)", border: "1px solid rgba(239,68,68,0.3)" }}
          >
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium mb-1" style={{ color: "var(--foreground)" }}>
              Username
            </label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              className="w-full rounded-md px-3 py-2 text-sm outline-none focus:ring-2"
              style={{
                background: "var(--input)",
                border: "1px solid var(--border)",
                color: "var(--foreground)",
                // @ts-expect-error CSS custom property
                "--tw-ring-color": "var(--ring)",
              }}
              placeholder="Enter username"
            />
          </div>

          {mode === "register" && (
            <div>
              <label className="block text-sm font-medium mb-1" style={{ color: "var(--foreground)" }}>
                Email
              </label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm outline-none focus:ring-2"
                style={{
                  background: "var(--input)",
                  border: "1px solid var(--border)",
                  color: "var(--foreground)",
                }}
                placeholder="Enter email (optional)"
              />
            </div>
          )}

          <div>
            <label className="block text-sm font-medium mb-1" style={{ color: "var(--foreground)" }}>
              Password
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              className="w-full rounded-md px-3 py-2 text-sm outline-none focus:ring-2"
              style={{
                background: "var(--input)",
                border: "1px solid var(--border)",
                color: "var(--foreground)",
              }}
              placeholder="Enter password"
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-md py-2 text-sm font-medium transition-opacity hover:opacity-90 disabled:opacity-50"
            style={{ background: "var(--primary)", color: "var(--primary-foreground)" }}
          >
            {loading ? "Please wait..." : mode === "login" ? "Sign In" : "Create Account"}
          </button>
        </form>
      </div>
    </div>
  );
}
