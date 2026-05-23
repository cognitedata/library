import { forwardRef, useImperativeHandle, useRef } from "react";

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
};

export const SqlEditor = forwardRef<SqlEditorHandle, Props>(function SqlEditor(
  { value, onChange, onRun, onRunSelection, height, readOnly, placeholder },
  ref
) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useImperativeHandle(ref, () => ({
    getSelection: () => {
      const el = textareaRef.current;
      if (!el) return { start: 0, end: 0 };
      return { start: el.selectionStart, end: el.selectionEnd };
    },
  }));

  return (
    <textarea
      ref={textareaRef}
      className="exp-sql-editor"
      spellCheck={false}
      readOnly={readOnly}
      placeholder={placeholder}
      value={value}
      style={{ height, minHeight: height, maxHeight: height }}
      onChange={(e) => onChange(e.target.value)}
      onKeyDown={(e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
          e.preventDefault();
          if (e.shiftKey) onRunSelection?.();
          else onRun?.();
        }
      }}
    />
  );
});
