import { useCallback, useEffect, useState } from "react";
import { fetchTransformationDetail } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { SqlDocumentTab, TransformationDocumentTab } from "../types/discoveryNodes";
import { SqlQueryPane } from "./SqlQueryPane";
import { createSqlTab } from "../utils/sqlTabs";

type Props = {
  tab: TransformationDocumentTab;
  onTabUpdate: (tab: TransformationDocumentTab) => void;
  onSelectRow: (row: Record<string, unknown> | null) => void;
  onQueryFile?: (row: Record<string, unknown>) => void;
};

export function TransformationPane({ tab, onTabUpdate, onSelectRow, onQueryFile }: Props) {
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
        tab.detail.external_id
          ? t("txViewer.meta.externalId", { value: tab.detail.external_id })
          : null,
        t("txViewer.meta.id", { value: String(tab.detail.id) }),
        tab.detail.data_set_id != null
          ? t("txViewer.meta.dataSetId", { value: String(tab.detail.data_set_id) })
          : null,
      ]
        .filter(Boolean)
        .join(t("common.metaSeparator"))
    : "";

  return (
    <div className="disc-doc-pane disc-transformation-pane">
      <div className="disc-doc-toolbar">
        <span className="disc-transformation-pane__title">{tab.label}</span>
        {metaLine ? <span className="disc-transformation-pane__meta">{metaLine}</span> : null}
        <button type="button" className="disc-btn" disabled={tab.loading} onClick={refresh}>
          {t("txViewer.refresh")}
        </button>
      </div>
      {tab.error && <div className="disc-banner--error">{t("status.error", { detail: tab.error })}</div>}
      {tab.loading && !tab.detail ? (
        <p className="disc-empty-hint">{t("txViewer.loading")}</p>
      ) : tab.detail ? (
        <div className="disc-transformation-pane__body">
          <div className="disc-transformation-pane__sql">
            <SqlQueryPane
              tab={sqlTab}
              onTabUpdate={setSqlTab}
              onSelectRow={onSelectRow}
              onQueryFile={onQueryFile}
            />
          </div>
          <details className="disc-transformation-pane__definition">
            <summary>{t("txViewer.definition")}</summary>
            <pre className="disc-properties">
              {JSON.stringify(tab.detail.definition ?? tab.detail, null, 2)}
            </pre>
          </details>
        </div>
      ) : null}
    </div>
  );
}
