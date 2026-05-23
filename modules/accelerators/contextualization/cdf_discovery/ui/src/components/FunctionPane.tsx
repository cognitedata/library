import { useCallback, useEffect } from "react";
import { fetchFunctionDetail } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { FunctionDocumentTab } from "../types/discoveryNodes";

type Props = {
  tab: FunctionDocumentTab;
  onTabUpdate: (tab: FunctionDocumentTab) => void;
};

export function FunctionPane({ tab, onTabUpdate }: Props) {
  const { t } = useAppSettings();

  const load = useCallback(async () => {
    onTabUpdate({ ...tab, loading: true, error: null });
    try {
      const detail = await fetchFunctionDetail(tab.functionId);
      onTabUpdate({
        ...tab,
        detail,
        loading: false,
        error: null,
        label: detail.name?.trim() || detail.external_id?.trim() || tab.label,
      });
    } catch (e) {
      onTabUpdate({ ...tab, detail: null, loading: false, error: String(e) });
    }
  }, [tab, onTabUpdate]);

  useEffect(() => {
    if (tab.detail != null || tab.error) return;
    void load();
  }, [tab.id, tab.functionId, tab.detail, tab.error, load]);

  const refresh = useCallback(() => {
    onTabUpdate({ ...tab, detail: null, loading: true, error: null });
    void load();
  }, [tab, onTabUpdate, load]);

  const metaLine = tab.detail
    ? [
        tab.detail.external_id
          ? t("fnViewer.meta.externalId", { value: tab.detail.external_id })
          : null,
        t("fnViewer.meta.id", { value: String(tab.detail.id) }),
        tab.detail.status ? t("fnViewer.meta.status", { value: tab.detail.status }) : null,
        tab.detail.file_id != null
          ? t("fnViewer.meta.fileId", { value: String(tab.detail.file_id) })
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
          {t("fnViewer.refresh")}
        </button>
      </div>
      {tab.error && <div className="disc-banner--error">{t("status.error", { detail: tab.error })}</div>}
      {tab.loading && !tab.detail ? (
        <p className="disc-empty-hint">{t("fnViewer.loading")}</p>
      ) : tab.detail ? (
        <div className="disc-transformation-pane__body">
          {tab.detail.description ? (
            <p className="disc-function-pane__description">{tab.detail.description}</p>
          ) : null}
          <details className="disc-transformation-pane__definition" open>
            <summary>{t("fnViewer.definition")}</summary>
            <pre className="disc-properties">
              {JSON.stringify(tab.detail.definition ?? tab.detail, null, 2)}
            </pre>
          </details>
        </div>
      ) : null}
    </div>
  );
}
