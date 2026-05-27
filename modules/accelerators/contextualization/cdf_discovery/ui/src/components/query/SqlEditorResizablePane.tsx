import type { ReactNode } from "react";
import { AccessibleResizeHandle } from "../AccessibleResizeHandle";
import { useVerticalPaneResize } from "../../hooks/useVerticalPaneResize";

type Props = {
  label: ReactNode;
  children: ReactNode;
  storageKey?: string;
};

/** SQL query area in a vertically resizable pane (drag handle below, not the textarea). */
export function SqlEditorResizablePane({
  label,
  children,
  storageKey = "transform.sqlEditorPaneHeight.v1",
}: Props) {
  const { height, onResizeStart, setHeight } = useVerticalPaneResize({ storageKey });

  return (
    <div className="transform-query-sql-stack">
      <div className="transform-query-sql-pane" style={{ height }}>
        <label className="transform-query-label transform-query-label--block transform-query-fields__query-label">{label}</label>
        <div className="transform-query-sql-pane__body">{children}</div>
      </div>
      <AccessibleResizeHandle
        className="transform-query-resize-handle-v"
        orientation="horizontal"
        value={height}
        min={80}
        max={Math.round(window.innerHeight * 0.5)}
        labelKey="transform.query.resizeSqlPane"
        onMouseDown={onResizeStart}
        onValueChange={setHeight}
      />
    </div>
  );
}
