import type { ReactNode } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
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
  const { t } = useAppSettings();
  const { height, onResizeStart } = useVerticalPaneResize({ storageKey });

  return (
    <div className="transform-query-sql-stack">
      <div className="transform-query-sql-pane" style={{ height }}>
        <label className="transform-query-label transform-query-label--block transform-query-fields__query-label">{label}</label>
        <div className="transform-query-sql-pane__body">{children}</div>
      </div>
      <div
        className="transform-query-resize-handle-v"
        role="separator"
        aria-orientation="horizontal"
        aria-valuenow={height}
        aria-label={t("transform.query.resizeSqlPane")}
        onMouseDown={onResizeStart}
      />
    </div>
  );
}
