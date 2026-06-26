import { useEffect, useState } from "react";
import { fetchConfig, fetchConnection } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { ConnectionInfo, OverviewSubTab, RuntimeConfigSummary } from "../../types/indexWorkspace";
import { ConfigPane } from "./ConfigPane";

type Props = {
  refreshKey: number;
  initialSubTab?: OverviewSubTab;
};

function OverviewSummary({ refreshKey }: { refreshKey: number }) {
  const { t } = useAppSettings();
  const [connection, setConnection] = useState<ConnectionInfo | null>(null);
  const [runtime, setRuntime] = useState<RuntimeConfigSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const [conn, cfg] = await Promise.all([fetchConnection(), fetchConfig()]);
        if (cancelled) return;
        setConnection(conn);
        setRuntime(cfg.runtime);
      } catch (e) {
        if (!cancelled) setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  if (loading) return <p>{t("common.loading")}</p>;
  if (error) return <p className="idx-banner--error">{error}</p>;

  return (
    <>
      <p className="idx-pane__hint">{t("overview.hint")}</p>
      {connection ? (
        <p>
          <span className="idx-badge idx-badge--ok">
            {t("connection.project", { project: connection.project })}
          </span>
        </p>
      ) : null}
      <div className="idx-overview-grid">
        <div className="idx-overview-card">
          <div className="idx-overview-card__label">{t("overview.backend")}</div>
          <div className="idx-overview-card__value">{runtime?.storage_backend ?? "—"}</div>
        </div>
        <div className="idx-overview-card">
          <div className="idx-overview-card__label">{t("overview.rawDatabase")}</div>
          <div className="idx-overview-card__value">{runtime?.raw_database ?? "—"}</div>
        </div>
        <div className="idx-overview-card">
          <div className="idx-overview-card__label">{t("overview.scopeEnabled")}</div>
          <div className="idx-overview-card__value">
            {runtime?.scope_enabled ? t("overview.yes") : t("overview.no")}
          </div>
        </div>
        <div className="idx-overview-card">
          <div className="idx-overview-card__label">{t("overview.scopeFallback")}</div>
          <div className="idx-overview-card__value">{runtime?.scope_fallback ?? "—"}</div>
        </div>
        <div className="idx-overview-card">
          <div className="idx-overview-card__label">{t("overview.subscriptionEnabled")}</div>
          <div className="idx-overview-card__value">
            {runtime?.subscription_enabled ? t("overview.yes") : t("overview.no")}
          </div>
        </div>
        <div className="idx-overview-card">
          <div className="idx-overview-card__label">{t("overview.watchProperty")}</div>
          <div className="idx-overview-card__value">{runtime?.watch_property ?? "—"}</div>
        </div>
        <div className="idx-overview-card">
          <div className="idx-overview-card__label">{t("overview.indexFieldCount")}</div>
          <div className="idx-overview-card__value">{runtime?.index_field_count ?? 0}</div>
        </div>
      </div>
    </>
  );
}

export function OverviewPane({ refreshKey, initialSubTab = "summary" }: Props) {
  const { t } = useAppSettings();
  const [subTab, setSubTab] = useState<OverviewSubTab>(initialSubTab);

  useEffect(() => {
    setSubTab(initialSubTab);
  }, [initialSubTab]);

  return (
    <div className="idx-pane idx-overview-pane">
      <header className="idx-overview-pane__header">
        <h2 className="idx-pane__title">{t("overview.title")}</h2>
        <nav className="idx-overview-tabs" aria-label={t("overview.tabsLabel")}>
          <button
            type="button"
            className={`idx-overview-tab${subTab === "summary" ? " idx-overview-tab--active" : ""}`}
            aria-current={subTab === "summary" ? "page" : undefined}
            onClick={() => setSubTab("summary")}
          >
            {t("overview.tab.summary")}
          </button>
          <button
            type="button"
            className={`idx-overview-tab${subTab === "configuration" ? " idx-overview-tab--active" : ""}`}
            aria-current={subTab === "configuration" ? "page" : undefined}
            onClick={() => setSubTab("configuration")}
          >
            {t("overview.tab.configuration")}
          </button>
        </nav>
      </header>

      <div className="idx-overview-pane__body">
        {subTab === "summary" ? (
          <OverviewSummary refreshKey={refreshKey} />
        ) : (
          <ConfigPane embedded />
        )}
      </div>
    </div>
  );
}
