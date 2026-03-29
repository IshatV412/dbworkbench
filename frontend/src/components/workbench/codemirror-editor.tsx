"use client";

import { useRef, useEffect, useCallback } from "react";
import { EditorView, keymap, placeholder as cmPlaceholder, lineNumbers, highlightActiveLine, highlightActiveLineGutter } from "@codemirror/view";
import { EditorState, Compartment, type Extension } from "@codemirror/state";
import { PostgreSQL, sql } from "@codemirror/lang-sql";
import { autocompletion, type CompletionContext, type Completion } from "@codemirror/autocomplete";
import { defaultKeymap, indentWithTab, history, historyKeymap } from "@codemirror/commands";
import { syntaxHighlighting, defaultHighlightStyle, bracketMatching } from "@codemirror/language";
import { searchKeymap, highlightSelectionMatches } from "@codemirror/search";
import { oneDark } from "@codemirror/theme-one-dark";

export interface SchemaCompletionData {
  schemas: string[];
  tables: Record<string, string[]>;   // schema -> table names
  columns: Record<string, string[]>;  // "schema.table" -> column names
}

interface CodeMirrorEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
  placeholder?: string;
  schemaData?: SchemaCompletionData;
}

// Custom dark theme tweaks to match our UI
const darkTheme = EditorView.theme({
  "&": {
    fontSize: "13px",
    height: "100%",
    background: "var(--editor-bg)",
  },
  ".cm-content": {
    fontFamily: "'Cascadia Code', 'Fira Code', 'Consolas', monospace",
    caretColor: "#fff",
    padding: "8px 0",
  },
  ".cm-gutters": {
    background: "#1a1a1a",
    borderRight: "1px solid var(--border)",
    color: "var(--muted-foreground)",
  },
  ".cm-activeLineGutter": {
    background: "#252930",
  },
  ".cm-activeLine": {
    background: "#ffffff08",
  },
  ".cm-cursor": {
    borderLeftColor: "#fff",
  },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground": {
    background: "#264f78 !important",
  },
  ".cm-tooltip.cm-tooltip-autocomplete": {
    background: "#1e1e2e",
    border: "1px solid var(--border)",
    borderRadius: "4px",
    fontSize: "12px",
  },
  ".cm-tooltip-autocomplete > ul > li": {
    padding: "2px 8px",
  },
  ".cm-tooltip-autocomplete > ul > li[aria-selected]": {
    background: "#264f78",
  },
  ".cm-scroller": {
    overflow: "auto",
  },
}, { dark: true });

function buildSchemaCompletion(schemaData: SchemaCompletionData) {
  return function schemaCompletion(context: CompletionContext) {
    const word = context.matchBefore(/[\w."]+/);
    if (!word && !context.explicit) return null;
    const from = word ? word.from : context.pos;
    const text = word ? word.text : "";

    const options: Completion[] = [];

    // If user typed "schema." suggest tables
    const dotParts = text.split(".");
    if (dotParts.length >= 2) {
      const schemaName = dotParts[0].replace(/"/g, "");
      const tables = schemaData.tables[schemaName] || [];
      for (const t of tables) {
        options.push({ label: t, type: "class", detail: "table", boost: 2 });
      }
      // If "schema.table." suggest columns
      if (dotParts.length >= 3) {
        const tableName = dotParts[1].replace(/"/g, "");
        const cols = schemaData.columns[`${schemaName}.${tableName}`] || [];
        for (const c of cols) {
          options.push({ label: c, type: "property", detail: "column", boost: 3 });
        }
      }
    } else {
      // Suggest schemas
      for (const s of schemaData.schemas) {
        options.push({ label: s, type: "namespace", detail: "schema" });
      }
      // Suggest all tables across all schemas
      for (const [schema, tables] of Object.entries(schemaData.tables)) {
        for (const t of tables) {
          options.push({ label: t, type: "class", detail: `${schema}`, boost: 1 });
        }
      }
    }

    if (options.length === 0) return null;
    return { from, options, validFor: /[\w".]*/  };
  };
}

export function CodeMirrorEditor({ value, onChange, onRun, placeholder, schemaData }: CodeMirrorEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const onRunRef = useRef(onRun);
  const autocompleteCompartment = useRef(new Compartment());
  onChangeRef.current = onChange;
  onRunRef.current = onRun;

  const buildAutocompleteExt = useCallback((): Extension => {
    const completionSources = [];
    if (schemaData && schemaData.schemas.length > 0) {
      completionSources.push(buildSchemaCompletion(schemaData));
    }
    return autocompletion({
      override: completionSources.length > 0 ? completionSources : undefined,
      activateOnTyping: true,
      maxRenderedOptions: 30,
    });
  }, [schemaData]);

  // Create editor
  useEffect(() => {
    if (!containerRef.current) return;

    const exts: Extension[] = [
      lineNumbers(),
      highlightActiveLine(),
      highlightActiveLineGutter(),
      history(),
      bracketMatching(),
      highlightSelectionMatches(),
      syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
      oneDark,
      darkTheme,
      sql({ dialect: PostgreSQL }),
      keymap.of([
        ...defaultKeymap,
        ...historyKeymap,
        ...searchKeymap,
        indentWithTab,
        {
          key: "Ctrl-Enter",
          run: () => { onRunRef.current?.(); return true; },
        },
        {
          key: "F5",
          run: () => { onRunRef.current?.(); return true; },
        },
      ]),
      EditorView.updateListener.of((update) => {
        if (update.docChanged) {
          onChangeRef.current(update.state.doc.toString());
        }
      }),
      autocompleteCompartment.current.of(buildAutocompleteExt()),
    ];

    if (placeholder) {
      exts.push(cmPlaceholder(placeholder));
    }

    const state = EditorState.create({
      doc: value,
      extensions: exts,
    });

    const view = new EditorView({
      state,
      parent: containerRef.current,
    });

    viewRef.current = view;

    return () => {
      view.destroy();
      viewRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update autocomplete when schemaData changes
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    view.dispatch({
      effects: autocompleteCompartment.current.reconfigure(buildAutocompleteExt()),
    });
  }, [buildAutocompleteExt]);

  // Sync external value changes (e.g. from sidebar click)
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const currentDoc = view.state.doc.toString();
    if (currentDoc !== value) {
      view.dispatch({
        changes: { from: 0, to: currentDoc.length, insert: value },
      });
    }
  }, [value]);

  return (
    <div
      ref={containerRef}
      className="h-full w-full overflow-hidden"
      style={{ background: "var(--editor-bg)" }}
    />
  );
}
