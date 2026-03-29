"use client";

import { Group, Panel, Separator } from "react-resizable-panels";
import { SqlEditor } from "@/components/workbench/sql-editor";
import { ResultsGrid } from "@/components/workbench/results-grid";
import { TerminalPanel } from "@/components/workbench/terminal-panel";

export function CenterPanel() {
  return (
    <Group orientation="vertical" className="h-full">
      {/* SQL Editor + Results */}
      <Panel defaultSize={65} minSize={30}>
        <Group orientation="vertical" className="h-full">
          {/* SQL Editor */}
          <Panel defaultSize={45} minSize={20}>
            <SqlEditor />
          </Panel>

          <Separator />

          {/* Results Grid */}
          <Panel defaultSize={55} minSize={15}>
            <ResultsGrid />
          </Panel>
        </Group>
      </Panel>

      <Separator />

      {/* Terminal */}
      <Panel defaultSize={35} minSize={10}>
        <TerminalPanel />
      </Panel>
    </Group>
  );
}
