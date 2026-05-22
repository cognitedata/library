import { useCallback, useEffect } from "react";
import { fetchFunctionDetail } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { FunctionDocumentTab } from "../types/explorerNodes";

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
        tab.detail.external_id ? `externalId: ${tab.detail.external_id}` : null,
        `id: ${tab.detail.id}`,
        tab.detail.status ? `status: ${tab.detail.status}` : null,
        tab.detail.file_id != null ? `fileId: ${tab.detail.file_id}` : null,
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
          {t("fnViewer.refresh")}
        </button>
      </div>
      {tab.error && <div className="exp-banner--error">{t("status.error", { detail: tab.error })}</div>}
      {tab.loading && !tab.detail ? (
        <p className="exp-empty-hint">{t("fnViewer.loading")}</p>
      ) : tab.detail ? (
        <div className="exp-transformation-pane__body">
          {tab.detail.description ? (
            <p className="exp-function-pane__description">{tab.detail.description}</p>
          ) : null}
          <details className="exp-transformation-pane__definition" open>
            <summary>{t("fnViewer.definition")}</summary>
            <pre className="exp-properties">
              {JSON.stringify(tab.detail.definition ?? tab.detail, null, 2)}
            </pre>
          </details>
        </div>
      ) : null}
    </div>
  );
}
