import { useCallback, useEffect, useState } from "react";
import { fetchTransformationDetail } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { SqlDocumentTab, TransformationDocumentTab } from "../types/explorerNodes";
import { SqlQueryPane } from "./SqlQueryPane";
import { createSqlTab } from "../utils/sqlTabs";

type Props = {
  tab: TransformationDocumentTab;
  onTabUpdate: (tab: TransformationDocumentTab) => void;
  onSelectRow: (row: Record<string, unknown> | null) => void;
};

export function TransformationPane({ tab, onTabUpdate, onSelectRow }: Props) {
  const { t } = useAppSettings();
  const [sqlTab, setSqlTab] = useState<SqlDocumentTab>(() =>
    createSqlTab({ id: tab.id, label: tab.label, query: "" })
  );

  const load = useCallback(async () => {
    onTabUpdate({ ...tab, loading: true, error: null });
    try {
      const detail = await fetchTransformationDetail(tab.transformationId);
      onTabUpdate({
        ...tab,
        detail,
        loading: false,
        error: null,
        label: detail.name?.trim() || detail.external_id?.trim() || tab.label,
      });
      setSqlTab((prev) => ({
        ...prev,
        id: tab.id,
        label: detail.name?.trim() || detail.external_id?.trim() || tab.label,
        query: detail.query,
        result: null,
        error: null,
        pageIndex: 0,
        selectedRowIndex: null,
      }));
      onSelectRow(null);
    } catch (e) {
      onTabUpdate({ ...tab, detail: null, loading: false, error: String(e) });
    }
  }, [tab, onTabUpdate, onSelectRow]);

  useEffect(() => {
    if (tab.detail != null || tab.error) return;
    void load();
  }, [tab.id, tab.transformationId, tab.detail, tab.error, load]);

  const refresh = useCallback(() => {
    onTabUpdate({ ...tab, detail: null, loading: true, error: null });
    void load();
  }, [tab, onTabUpdate, load]);

  const metaLine = tab.detail
    ? [
        tab.detail.external_id ? `externalId: ${tab.detail.external_id}` : null,
        `id: ${tab.detail.id}`,
        tab.detail.data_set_id != null ? `dataSetId: ${tab.detail.data_set_id}` : null,
      ]
        .filter(Boolean)
        .join(" · ")
    : "";

  return (
    <div className="exp-doc-pane exp-transformation-pane">
      <div className="exp-doc-toolbar">
        <span className="exp-transformation-pane__title">{tab.label}</span>
        {metaLine ? <span className="exp-transformation-pane__meta">{metaLine}</span> : null}
        <button type="button" className="exp-btn" disabled={tab.loading} onClick={refresh}>
          {t("txViewer.refresh")}
        </button>
      </div>
      {tab.error && <div className="exp-banner--error">{t("status.error", { detail: tab.error })}</div>}
      {tab.loading && !tab.detail ? (
        <p className="exp-empty-hint">{t("txViewer.loading")}</p>
      ) : tab.detail ? (
        <div className="exp-transformation-pane__body">
          <div className="exp-transformation-pane__sql">
            <SqlQueryPane tab={sqlTab} onTabUpdate={setSqlTab} onSelectRow={onSelectRow} />
          </div>
          <details className="exp-transformation-pane__definition">
            <summary>{t("txViewer.definition")}</summary>
            <pre className="exp-properties">
              {JSON.stringify(tab.detail.definition ?? tab.detail, null, 2)}
            </pre>
          </details>
        </div>
      ) : null}
    </div>
  );
}
