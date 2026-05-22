import type { ReactNode } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { useVerticalPaneResize } from "../hooks/useVerticalPaneResize";

type Props = {
  label: ReactNode;
  children: ReactNode;
  storageKey?: string;
};

/** SQL query area in a vertically resizable pane (drag handle below, not the textarea). */
export function SqlEditorResizablePane({
  label,
  children,
  storageKey = "discovery.sqlEditorPaneHeight.v1",
}: Props) {
  const { t } = useAppSettings();
  const { height, onResizeStart } = useVerticalPaneResize({ storageKey });

  return (
    <div className="discovery-sql-editor-stack">
      <div className="discovery-sql-editor-pane" style={{ height }}>
        <label className="discovery-label discovery-label--block discovery-query-fields__query-label">{label}</label>
        <div className="discovery-sql-editor-pane__body">{children}</div>
      </div>
      <div
        className="discovery-resize-handle-v"
        role="separator"
        aria-orientation="horizontal"
        aria-valuenow={height}
        aria-label={t("flow.resizePanels")}
        onMouseDown={onResizeStart}
      />
    </div>
  );
}
