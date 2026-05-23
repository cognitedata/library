import { useCallback, useEffect } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { fetchJson } from "../../api/fetchJson";
import type { GovernanceCdfGroupDocumentTab } from "../../types/discoveryNodes";

type Props = {
  tab: GovernanceCdfGroupDocumentTab;
  onTabUpdate: (tab: GovernanceCdfGroupDocumentTab) => void;
};

export function GovernanceCdfGroupPane({ tab, onTabUpdate }: Props) {
  const { t } = useAppSettings();

  const load = useCallback(async () => {
    onTabUpdate({ ...tab, loading: true, error: null });
    try {
      const detail = await fetchJson<Record<string, unknown>>(
        `/api/cdf/governance/groups/detail?id=${tab.groupId}`
      );
      onTabUpdate({ ...tab, detail, loading: false, error: null });
    } catch (e) {
      onTabUpdate({ ...tab, detail: null, loading: false, error: String(e) });
    }
  }, [tab, onTabUpdate]);

  useEffect(() => {
    if (tab.detail != null || tab.error) return;
    void load();
  }, [tab.detail, tab.error, load]);

  return (
    <div className="disc-gov-pane">
      <div className="disc-gov-toolbar">
        <span>{tab.label}</span>
        <button type="button" className="disc-btn" onClick={() => void load()}>
          {t("grid.refresh")}
        </button>
      </div>
      <div className="disc-gov-pane-body">
        {tab.loading && <p className="disc-empty-hint">{t("governance.live.loading")}</p>}
        {tab.error && <p className="disc-banner--error">{tab.error}</p>}
        {tab.detail && (
          <>
            <h3 className="disc-gov-section-title">{t("governance.live.capabilities")}</h3>
            <pre className="disc-properties">{JSON.stringify(tab.detail, null, 2)}</pre>
          </>
        )}
      </div>
    </div>
  );
}
