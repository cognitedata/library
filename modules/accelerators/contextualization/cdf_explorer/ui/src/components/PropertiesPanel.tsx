import { useAppSettings } from "../context/AppSettingsContext";
import type { TreeNode } from "../types/explorerNodes";
import { isQueryableFileRow } from "../utils/queryableFileFromRow";

type Props = {
  collapsed: boolean;
  onToggleCollapse: () => void;
  selectedNode: TreeNode | null;
  rowDetail: unknown | null;
  propertiesHeight: number;
  onQueryFile?: (row: Record<string, unknown>) => void;
};

export function PropertiesPanel({
  collapsed,
  onToggleCollapse,
  selectedNode,
  rowDetail,
  propertiesHeight,
  onQueryFile,
}: Props) {
  const { t } = useAppSettings();
  if (collapsed) {
    return (
      <div className="exp-properties-pane" style={{ height: "auto" }}>
        <div className="exp-properties-header">
          <span>{t("properties.title")}</span>
          <button type="button" className="exp-btn" onClick={onToggleCollapse}>
            {t("properties.show")}
          </button>
        </div>
      </div>
    );
  }

  const payload = rowDetail ?? (selectedNode ? { kind: selectedNode.kind, ...selectedNode.meta, id: selectedNode.id, label: selectedNode.label } : null);
  const queryableRow =
    rowDetail && typeof rowDetail === "object" && isQueryableFileRow(rowDetail as Record<string, unknown>)
      ? (rowDetail as Record<string, unknown>)
      : null;

  return (
    <div className="exp-properties-pane" style={{ height: propertiesHeight }}>
      <div className="exp-properties-header">
        <span>
          {t("properties.title")} — {rowDetail ? t("properties.row") : t("properties.node")}
        </span>
        <div className="exp-properties-header__actions">
          {queryableRow && onQueryFile && (
            <button type="button" className="exp-btn" onClick={() => onQueryFile(queryableRow)}>
              {t("sql.queryFile")}
            </button>
          )}
          <button type="button" className="exp-btn" onClick={onToggleCollapse}>
            {t("properties.collapse")}
          </button>
        </div>
      </div>
      <div className="exp-properties-body">
        {payload ? (
          <pre className="exp-properties">{JSON.stringify(payload, null, 2)}</pre>
        ) : (
          <p className="exp-empty-hint">{t("properties.emptyHint")}</p>
        )}
      </div>
    </div>
  );
}
