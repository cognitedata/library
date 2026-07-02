import { useEffect, useState } from "react";
import { fetchConfig, fetchConnection } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { ConnectionInfo, RuntimeConfigSummary } from "../../types/indexWorkspace";

type Props = {
  refreshKey: number;
};

export function DashboardRuntimePanel({ refreshKey }: Props) {
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

  const enabledBadge = (on: boolean | undefined) => (
    <span className={`idx-badge${on ? " idx-badge--enabled" : " idx-badge--disabled"}`}>
      {on ? t("overview.yes") : t("overview.no")}
    </span>
  );

  return (
    <div className="idx-dashboard-runtime">
      {connection ? (
        <div className="idx-dashboard-runtime__connection">
          <span className="idx-badge idx-badge--ok">
            {t("connection.project", { project: connection.project })}
          </span>
        </div>
      ) : null}
      <div className="idx-dashboard-runtime__groups">
        <div className="idx-dashboard-runtime__group">
          <h4 className="idx-overview-section__title">{t("overview.section.storage")}</h4>
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
        </div>
        <div className="idx-dashboard-runtime__group">
          <h4 className="idx-overview-section__title">{t("overview.section.scope")}</h4>
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
        </div>
        <div className="idx-dashboard-runtime__group">
          <h4 className="idx-overview-section__title">{t("overview.section.automation")}</h4>
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
        </div>
      </div>
    </div>
  );
}
