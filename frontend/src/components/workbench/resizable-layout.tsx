"use client";

import { Group, Panel, Separator } from "react-resizable-panels";

interface ResizableLayoutProps {
  sidebar: React.ReactNode;
  center: React.ReactNode;
  rightPanel: React.ReactNode;
}

const vSepStyle: React.CSSProperties = {
  width: 4,
  minWidth: 4,
  background: "var(--resize-handle)",
  cursor: "col-resize",
  flexShrink: 0,
  transition: "background 0.15s",
};

export function ResizableLayout({ sidebar, center, rightPanel }: ResizableLayoutProps) {
  return (
    <Group
      orientation="horizontal"
      style={{ flex: 1, minHeight: 0, height: "100%", display: "flex", overflow: "hidden" }}
    >
      {/* Left Sidebar */}
      <Panel defaultSize="18%" minSize="12%" maxSize="35%">
        <div style={{ height: "100%", borderRight: "1px solid var(--border)", overflow: "hidden" }}>
          {sidebar}
        </div>
      </Panel>

      <Separator style={vSepStyle} />

      {/* Center - Editor + Results + Terminal */}
      <Panel defaultSize="58%" minSize="30%">
        <div style={{ height: "100%", overflow: "hidden" }}>
          {center}
        </div>
      </Panel>

      <Separator style={vSepStyle} />

      {/* Right - Version Control Panel */}
      <Panel defaultSize="24%" minSize="15%" maxSize="38%">
        <div style={{ height: "100%", borderLeft: "1px solid var(--border)", overflow: "hidden" }}>
          {rightPanel}
        </div>
      </Panel>
    </Group>
  );
}
