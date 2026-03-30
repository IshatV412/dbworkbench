"use client";

import { useEffect, useRef } from "react";

interface Props {
  wsUrl: string;
}

/**
 * XtermTerminal — dynamically loads xterm.js and connects it to a WebSocket.
 * Import this via next/dynamic with ssr:false.
 */
export function XtermTerminal({ wsUrl }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const container = containerRef.current;

    let disposed = false;
    let ws: WebSocket | null = null;
    let cleanupTerm: (() => void) | null = null;
    let disconnectRO: (() => void) | null = null;

    (async () => {
      const { Terminal } = await import("@xterm/xterm");
      const { FitAddon } = await import("@xterm/addon-fit");
      if (disposed) return;

      const term = new Terminal({
        cursorBlink: true,
        fontSize: 13,
        fontFamily: "'Cascadia Code', 'Fira Code', 'Courier New', monospace",
        theme: {
          background: "#0a0d14",
          foreground: "#e2e8f0",
          cursor: "#4e9cf5",
          cursorAccent: "#0a0d14",
          selectionBackground: "#4e9cf540",
        },
        scrollback: 5000,
        convertEol: false,
      });

      const fitAddon = new FitAddon();
      term.loadAddon(fitAddon);
      term.open(container);
      requestAnimationFrame(() => {
        try { fitAddon.fit(); } catch { /* ignore */ }
      });

      cleanupTerm = () => term.dispose();

      ws = new WebSocket(wsUrl);
      ws.binaryType = "arraybuffer";

      ws.onmessage = (e) => {
        if (disposed) return;
        if (e.data instanceof ArrayBuffer) {
          term.write(new Uint8Array(e.data));
        } else {
          term.write(e.data as string);
        }
      };

      ws.onclose = () => {
        if (!disposed) term.write("\r\n\x1b[31m[disconnected]\x1b[0m\r\n");
      };

      ws.onerror = () => {
        term.write("\r\n\x1b[31m[connection error — is the FastAPI backend running?]\x1b[0m\r\n");
      };

      term.onData((data) => {
        if (ws?.readyState === WebSocket.OPEN) {
          ws.send(data);
        }
      });

      const ro = new ResizeObserver(() => {
        try { fitAddon.fit(); } catch { /* ignore */ }
      });
      ro.observe(container);
      disconnectRO = () => ro.disconnect();
    })();

    return () => {
      disposed = true;
      disconnectRO?.();
      ws?.close();
      cleanupTerm?.();
    };
  }, [wsUrl]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: "100%", padding: "2px 4px", boxSizing: "border-box" }}
    />
  );
}
