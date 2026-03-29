"use client";

import { Group, Panel, Separator } from "react-resizable-panels";

interface ResizableLayoutProps {
  sidebar: React.ReactNode;
  center: React.ReactNode;
  rightPanel: React.ReactNode;
}

export function ResizableLayout({ sidebar, center, rightPanel }: ResizableLayoutProps) {
  return (
    <Group orientation="horizontal" className="flex-1">
      {/* Left Sidebar - Object Explorer */}
      <Panel defaultSize={18} minSize={12} maxSize={30}>
        {sidebar}
      </Panel>

      <Separator />

      {/* Center - Editor + Results + Terminal */}
      <Panel defaultSize={58} minSize={30}>
        {center}
      </Panel>

      <Separator />

      {/* Right - Version Control Panel */}
      <Panel defaultSize={24} minSize={15} maxSize={35}>
        {rightPanel}
      </Panel>
    </Group>
  );
}
