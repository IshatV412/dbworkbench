"use client";

import { Group, Panel, Separator } from "react-resizable-panels";
import { SqlEditor } from "@/components/workbench/sql-editor";
import { ResultsGrid } from "@/components/workbench/results-grid";
import { TerminalPanel } from "@/components/workbench/terminal-panel";

const hSepStyle: React.CSSProperties = {
  height: 4,
  minHeight: 4,
  background: "var(--resize-handle)",
  cursor: "row-resize",
  flexShrink: 0,
  transition: "background 0.15s",
};

export function CenterPanel() {
  return (
    <Group orientation="vertical" style={{ height: "100%", display: "flex", flexDirection: "column" }}>
      {/* SQL Editor + Results */}
      <Panel defaultSize="65%" minSize="30%">
        <Group orientation="vertical" style={{ height: "100%", display: "flex", flexDirection: "column" }}>
          {/* SQL Editor */}
          <Panel defaultSize="45%" minSize="20%">
            <div style={{ height: "100%", overflow: "hidden" }}>
              <SqlEditor />
            </div>
          </Panel>

          <Separator style={hSepStyle} />

          {/* Results Grid */}
          <Panel defaultSize="55%" minSize="15%">
            <div style={{ height: "100%", overflow: "hidden" }}>
              <ResultsGrid />
            </div>
          </Panel>
        </Group>
      </Panel>

      <Separator style={hSepStyle} />

      {/* Terminal */}
      <Panel defaultSize="35%" minSize="8%">
        <div style={{ height: "100%", overflow: "hidden" }}>
          <TerminalPanel />
        </div>
      </Panel>
    </Group>
  );
}
