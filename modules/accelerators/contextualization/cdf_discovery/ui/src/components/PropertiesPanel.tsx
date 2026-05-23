import type { CSSProperties } from "react";
import { PanelDragHandle } from "./PanelDragHandle";
import { useAppSettings } from "../context/AppSettingsContext";
import type { TreeNode } from "../types/discoveryNodes";
import { isQueryableFileRow } from "../utils/queryableFileFromRow";

export type PropertiesPanelLayout = "bottom" | "side" | "stacked";

type Props = {
  collapsed: boolean;
  onToggleCollapse: () => void;
  selectedNode: TreeNode | null;
  rowDetail: unknown | null;
  paneSize: number;
  layout: PropertiesPanelLayout;
  isDragging?: boolean;
  onPanelDragStart: () => void;
  onPanelDragEnd: () => void;
  onQueryFile?: (row: Record<string, unknown>) => void;
};

export function PropertiesPanel({
  collapsed,
  onToggleCollapse,
  selectedNode,
  rowDetail,
  paneSize,
  layout,
  isDragging = false,
  onPanelDragStart,
  onPanelDragEnd,
  onQueryFile,
}: Props) {
  const { t } = useAppSettings();

  const layoutClass =
    layout === "side"
      ? "disc-properties-pane--side"
      : layout === "stacked"
        ? "disc-properties-pane--stacked"
        : "disc-properties-pane--bottom";

  const sizeStyle: CSSProperties =
    layout === "side"
      ? { width: paneSize, minWidth: paneSize, maxWidth: paneSize, height: "100%" }
      : collapsed
        ? { height: "auto" }
        : { height: paneSize };

  const payload =
    rowDetail ??
    (selectedNode
      ? { kind: selectedNode.kind, ...selectedNode.meta, id: selectedNode.id, label: selectedNode.label }
      : null);
  const queryableRow =
    rowDetail && typeof rowDetail === "object" && isQueryableFileRow(rowDetail as Record<string, unknown>)
      ? (rowDetail as Record<string, unknown>)
      : null;

  return (
    <div
      className={`disc-properties-pane ${layoutClass}${collapsed ? " disc-properties-pane--collapsed" : ""}${isDragging ? " disc-panel--dragging" : ""}`}
      style={sizeStyle}
    >
      <div className="disc-properties-header">
        <PanelDragHandle
          panel="properties"
          labelKey="layout.dragHandle.properties"
          onDragStart={() => onPanelDragStart()}
          onDragEnd={onPanelDragEnd}
        />
        <span className="disc-properties-header__title">
          {t("properties.title")}
          {!collapsed ? ` — ${rowDetail ? t("properties.row") : t("properties.node")}` : ""}
        </span>
        <div className="disc-properties-header__actions">
          {!collapsed && queryableRow && onQueryFile && (
            <button type="button" className="disc-btn" onClick={() => onQueryFile(queryableRow)}>
              {t("sql.queryFile")}
            </button>
          )}
          <button type="button" className="disc-btn" onClick={onToggleCollapse}>
            {collapsed ? t("properties.show") : t("properties.collapse")}
          </button>
        </div>
      </div>
      {!collapsed && (
        <div className="disc-properties-body">
          {payload ? (
            <pre className="disc-properties">{JSON.stringify(payload, null, 2)}</pre>
          ) : (
            <p className="disc-empty-hint">{t("properties.emptyHint")}</p>
          )}
        </div>
      )}
    </div>
  );
}
