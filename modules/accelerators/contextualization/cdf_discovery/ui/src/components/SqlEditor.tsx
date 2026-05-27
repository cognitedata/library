import { sql } from "@codemirror/lang-sql";
import type { Extension } from "@codemirror/state";
import { oneDark } from "@codemirror/theme-one-dark";
import { keymap, placeholder } from "@codemirror/view";
import CodeMirror, { type ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { forwardRef, useId, useImperativeHandle, useMemo, useRef } from "react";

export type SqlEditorSelection = {
  start: number;
  end: number;
};

export type SqlEditorHandle = {
  getSelection: () => SqlEditorSelection;
};

type Props = {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
  onRunSelection?: () => void;
  height: string;
  readOnly?: boolean;
  placeholder?: string;
  theme: "light" | "dark";
  ariaLabel: string;
  shortcutsHint?: string;
};

export const SqlEditor = forwardRef<SqlEditorHandle, Props>(function SqlEditor(
  {
    value,
    onChange,
    onRun,
    onRunSelection,
    height,
    readOnly,
    placeholder: placeholderText,
    theme,
    ariaLabel,
    shortcutsHint,
  },
  ref
) {
  const cmRef = useRef<ReactCodeMirrorRef>(null);
  const labelId = useId();
  const shortcutsId = useId();

  useImperativeHandle(ref, () => ({
    getSelection: () => {
      const view = cmRef.current?.view;
      if (!view) return { start: 0, end: 0 };
      const { from, to } = view.state.selection.main;
      return { start: from, end: to };
    },
  }));

  const extensions = useMemo((): Extension[] => {
    const exts: Extension[] = [sql()];
    if (placeholderText) {
      exts.push(placeholder(placeholderText));
    }
    exts.push(
      keymap.of([
        {
          key: "Mod-Enter",
          run: () => {
            onRun?.();
            return true;
          },
        },
        {
          key: "Shift-Mod-Enter",
          run: () => {
            onRunSelection?.();
            return true;
          },
        },
      ])
    );
    return exts;
  }, [placeholderText, onRun, onRunSelection]);

  const describedBy = shortcutsHint ? shortcutsId : undefined;

  return (
    <div className="disc-sql-editor-wrap">
      <span id={labelId} className="disc-visually-hidden">
        {ariaLabel}
      </span>
      {shortcutsHint ? (
        <span id={shortcutsId} className="disc-visually-hidden">
          {shortcutsHint}
        </span>
      ) : null}
      <CodeMirror
        ref={cmRef}
        className="disc-sql-editor disc-sql-editor--cm"
        value={value}
        height={height}
        theme={theme === "dark" ? oneDark : "light"}
        extensions={extensions}
        editable={!readOnly}
        aria-labelledby={labelId}
        aria-describedby={describedBy}
        basicSetup={{
          lineNumbers: false,
          foldGutter: false,
          highlightActiveLine: false,
        }}
        onChange={(next) => onChange(next)}
      />
    </div>
  );
});
