import { useEffect, useState } from "react";
import { fetchConfig, fetchConnection } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { ConnectionInfo, OverviewSubTab, RuntimeConfigSummary } from "../../types/indexWorkspace";
import { ConfigPane } from "./ConfigPane";
import { createIndexTab } from "../../utils/indexTabs";
import type { IndexDocumentTab } from "../../types/indexWorkspace";
import { SectionIntro } from "../shared/SectionIntro";

type Props = {
  refreshKey: number;
  initialSubTab?: OverviewSubTab;
  onOpenTab?: (tab: IndexDocumentTab) => void;
};

function OverviewSummary({
  refreshKey,
  onOpenTab,
}: {
  refreshKey: number;
  onOpenTab?: (tab: IndexDocumentTab) => void;
}) {
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

  const openQuick = (kind: "build-metadata" | "query" | "configuration", labelKey: Parameters<typeof t>[0]) => {
    if (!onOpenTab) return;
    const label = t(labelKey);
    onOpenTab(createIndexTab(kind, label, `inverted-index/quick/${kind}`));
  };

  if (loading) return <p>{t("common.loading")}</p>;
  if (error) return <p className="idx-banner--error">{error}</p>;

  const enabledBadge = (on: boolean | undefined) => (
    <span className={`idx-badge${on ? " idx-badge--enabled" : " idx-badge--disabled"}`}>
      {on ? t("overview.yes") : t("overview.no")}
    </span>
  );

  return (
    <>
      <SectionIntro>{t("overview.hint")}</SectionIntro>
      {connection ? (
        <div className="idx-overview-hero">
          <span className="idx-badge idx-badge--ok">
            {t("connection.project", { project: connection.project })}
          </span>
        </div>
      ) : null}
      {onOpenTab ? (
        <section className="idx-overview-quick-card">
          <h3 className="idx-overview-section__title">{t("overview.quickLinks.label")}</h3>
          <div className="idx-overview-quick-links">
            <button type="button" className="idx-btn idx-btn--sm" onClick={() => openQuick("build-metadata", "overview.quickLinks.buildMetadata")}>
              {t("overview.quickLinks.buildMetadata")}
            </button>
            <button type="button" className="idx-btn idx-btn--sm" onClick={() => openQuick("query", "overview.quickLinks.query")}>
              {t("overview.quickLinks.query")}
            </button>
            <button type="button" className="idx-btn idx-btn--sm" onClick={() => openQuick("configuration", "overview.quickLinks.config")}>
              {t("overview.quickLinks.config")}
            </button>
          </div>
        </section>
      ) : null}
      <section className="idx-overview-section">
        <h3 className="idx-overview-section__title">{t("overview.section.storage")}</h3>
        <div className="idx-overview-grid">
          <div className="idx-overview-card">
            <div className="idx-overview-card__label">{t("overview.backend")}</div>
            <div className="idx-overview-card__value">{runtime?.storage_backend ?? "—"}</div>
          </div>
          <div className="idx-overview-card">
            <div className="idx-overview-card__label">{t("overview.rawDatabase")}</div>
            <div className="idx-overview-card__value">{runtime?.raw_database ?? "—"}</div>
          </div>
          {runtime?.storage_backend === "raw" ? (
            <>
              <div className="idx-overview-card">
                <div className="idx-overview-card__label">{t("overview.termPartitionEnabled")}</div>
                <div className="idx-overview-card__value">
                  {enabledBadge(runtime?.term_partition_enabled)}
                </div>
              </div>
              <div className="idx-overview-card">
                <div className="idx-overview-card__label">{t("overview.termPartitionThreshold")}</div>
                <div className="idx-overview-card__value">
                  {runtime?.term_partition_threshold ?? "—"}
                </div>
              </div>
            </>
          ) : null}
          <div className="idx-overview-card">
            <div className="idx-overview-card__label">{t("overview.indexFieldCount")}</div>
            <div className="idx-overview-card__value">{runtime?.index_field_count ?? 0}</div>
          </div>
        </div>
      </section>
      <section className="idx-overview-section">
        <h3 className="idx-overview-section__title">{t("overview.section.scope")}</h3>
        <div className="idx-overview-grid">
          <div className="idx-overview-card">
            <div className="idx-overview-card__label">{t("overview.scopeEnabled")}</div>
            <div className="idx-overview-card__value">{enabledBadge(runtime?.scope_enabled)}</div>
          </div>
          <div className="idx-overview-card">
            <div className="idx-overview-card__label">{t("overview.scopeFallback")}</div>
            <div className="idx-overview-card__value">{runtime?.scope_fallback ?? "—"}</div>
          </div>
        </div>
      </section>
      <section className="idx-overview-section">
        <h3 className="idx-overview-section__title">{t("overview.section.automation")}</h3>
        <div className="idx-overview-grid">
          <div className="idx-overview-card">
            <div className="idx-overview-card__label">{t("overview.subscriptionEnabled")}</div>
            <div className="idx-overview-card__value">{enabledBadge(runtime?.subscription_enabled)}</div>
          </div>
          <div className="idx-overview-card">
            <div className="idx-overview-card__label">{t("overview.watchProperty")}</div>
            <div className="idx-overview-card__value">{runtime?.watch_property ?? "—"}</div>
          </div>
        </div>
      </section>
    </>
  );
}

export function OverviewPane({ refreshKey, initialSubTab = "summary", onOpenTab }: Props) {
  const { t } = useAppSettings();
  const [subTab, setSubTab] = useState<OverviewSubTab>(initialSubTab);

  useEffect(() => {
    setSubTab(initialSubTab);
  }, [initialSubTab]);

  return (
    <div className="idx-pane idx-editor-page idx-overview-pane">
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
          <OverviewSummary refreshKey={refreshKey} onOpenTab={onOpenTab} />
        ) : (
          <ConfigPane embedded />
        )}
      </div>
    </div>
  );
}
