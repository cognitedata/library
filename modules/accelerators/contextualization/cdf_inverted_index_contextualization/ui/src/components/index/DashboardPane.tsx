import { useCallback, useEffect, useMemo, useState } from "react";
import {
  DASHBOARD_FILE_DELTAS_STREAM_URL,
  DASHBOARD_TAG_REUSE_STREAM_URL,
  dashboardFileDeltasBody,
  fetchDashboardSummary,
  formatTagReuseAuditResult,
  parseCsvList,
  setFileContextPrefill,
} from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import type {
  DashboardBatchDeltasResult,
  DashboardScopeRow,
  DashboardSummary,
  IndexDocumentTab,
} from "../../types/indexWorkspace";
import {
  DASHBOARD_DELTA_METRICS,
  DASHBOARD_SUMMARY_METRICS,
  TAG_REUSE_METRICS,
} from "../../utils/metricDefs";
import { createIndexTab } from "../../utils/indexTabs";
import { asHitRows } from "../../utils/resultViews";
import { DataTable } from "../shared/DataTable";
import { EditorPage } from "../shared/EditorPage";
import { MetricSummary } from "../shared/MetricSummary";
import { DashboardRuntimePanel } from "./DashboardRuntimePanel";
import { OperationConsole } from "./OperationConsole";

const WATCHLIST_STORAGE_KEY = "idx-dashboard-watchlist";

type Props = {
  refreshKey: number;
  onOpenTab?: (tab: IndexDocumentTab) => void;
};

function rowStatusBadgeClass(status: DashboardScopeRow["row_status"]): string {
  if (status === "critical") return "idx-badge idx-badge--error";
  if (status === "warn") return "idx-badge idx-badge--warn";
  return "idx-badge idx-badge--ok";
}

function formatCount(value: number | null | undefined): string {
  if (value == null) return "—";
  return value.toLocaleString();
}

export function DashboardPane({ refreshKey, onOpenTab }: Props) {
  const { t } = useAppSettings();
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [summaryError, setSummaryError] = useState<string | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(true);
  const [runtimeRefreshKey, setRuntimeRefreshKey] = useState(0);
  const [watchlistRaw, setWatchlistRaw] = useState("");
  const [fileSpace, setFileSpace] = useState("cdf_cdm");
  const [scopeKey, setScopeKey] = useState("");
  const reuseOp = useOperationRun();
  const deltasOp = useOperationRun();

  useEffect(() => {
    try {
      const stored = localStorage.getItem(WATCHLIST_STORAGE_KEY);
      if (stored) setWatchlistRaw(stored);
    } catch {
      // ignore storage errors
    }
  }, []);

  const loadSummary = useCallback(async () => {
    setSummaryLoading(true);
    setSummaryError(null);
    try {
      const data = await fetchDashboardSummary();
      setSummary(data);
    } catch (e) {
      setSummary(null);
      setSummaryError(String(e));
    } finally {
      setSummaryLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadSummary();
  }, [loadSummary, refreshKey]);

  const refreshAll = () => {
    setRuntimeRefreshKey((k) => k + 1);
    void loadSummary();
  };

  const openQuick = (kind: "build-metadata" | "query" | "configuration", labelKey: Parameters<typeof t>[0]) => {
    if (!onOpenTab) return;
    const label = t(labelKey);
    onOpenTab(createIndexTab(kind, label, `inverted-index/quick/${kind}`));
  };

  const reuseSummary = useMemo(
    () => formatTagReuseAuditResult(reuseOp.result) as Record<string, unknown> | null,
    [reuseOp.result]
  );

  const reuseRows = useMemo(() => {
    if (!reuseOp.result || typeof reuseOp.result !== "object" || Array.isArray(reuseOp.result)) {
      return [];
    }
    const reuse = (reuseOp.result as Record<string, unknown>).reuse_metrics;
    if (reuse && typeof reuse === "object" && "by_term" in reuse) {
      const byTerm = (reuse as { by_term: unknown }).by_term;
      if (Array.isArray(byTerm)) return byTerm.slice(0, 20);
    }
    return [];
  }, [reuseOp.result]);

  const deltasResult =
    deltasOp.result && typeof deltasOp.result === "object" && !Array.isArray(deltasOp.result)
      ? (deltasOp.result as DashboardBatchDeltasResult)
      : null;

  const fileDeltaRows = deltasResult?.by_file ?? [];

  const runTagReuse = () => {
    void reuseOp.run(DASHBOARD_TAG_REUSE_STREAM_URL, {});
  };

  const runBatchDeltas = () => {
    const fileIds = parseCsvList(watchlistRaw);
    if (fileIds.length === 0) return;
    try {
      localStorage.setItem(WATCHLIST_STORAGE_KEY, watchlistRaw);
    } catch {
      // ignore storage errors
    }
    void deltasOp.run(
      DASHBOARD_FILE_DELTAS_STREAM_URL,
      dashboardFileDeltasBody({
        file_external_ids: fileIds,
        file_space: fileSpace.trim() || "cdf_cdm",
        match_scope_key: scopeKey.trim() || undefined,
      })
    );
  };

  const openFileContext = (fileExternalId: string) => {
    if (!onOpenTab) return;
    setFileContextPrefill({
      file_external_id: fileExternalId,
      file_space: fileSpace.trim() || "cdf_cdm",
      match_scope_key: scopeKey.trim() || undefined,
      tab: "deltas",
    });
    const tab = createIndexTab(
      "file-context",
      t("nav.fileContext"),
      "inverted-index/file"
    );
    onOpenTab(tab);
  };

  const scopeColumns = [
    {
      id: "scope",
      headerKey: "dashboard.col.scope" as const,
      render: (row: DashboardScopeRow) => row.match_scope_key,
    },
    {
      id: "strategy",
      headerKey: "dashboard.col.strategy" as const,
      render: (row: DashboardScopeRow) => row.partition_strategy ?? "—",
    },
    {
      id: "rows",
      headerKey: "dashboard.col.rowCount" as const,
      render: (row: DashboardScopeRow) => formatCount(row.row_count),
    },
    {
      id: "estimate",
      headerKey: "dashboard.col.rowEstimate" as const,
      render: (row: DashboardScopeRow) => formatCount(row.row_count_estimate),
    },
    {
      id: "status",
      headerKey: "dashboard.col.status" as const,
      render: (row: DashboardScopeRow) => (
        <span className={rowStatusBadgeClass(row.row_status)}>
          {t(`dashboard.status.${row.row_status}`)}
        </span>
      ),
    },
    {
      id: "reshard",
      headerKey: "dashboard.col.reshard" as const,
      render: (row: DashboardScopeRow) => {
        if (row.reshard_in_progress) return t("dashboard.reshard.inProgress");
        if (row.reshard_recommended) return t("dashboard.reshard.recommended");
        return "—";
      },
    },
  ];

  const reuseTermColumns = [
    {
      id: "term",
      headerKey: "table.term" as const,
      render: (row: Record<string, unknown>) => String(row.term ?? row.normalized_term ?? "—"),
    },
    {
      id: "scopes",
      headerKey: "table.scopeCount" as const,
      render: (row: Record<string, unknown>) => String(row.scope_count ?? "—"),
    },
    {
      id: "hits",
      headerKey: "metrics.hitCount" as const,
      render: (row: Record<string, unknown>) => String(row.hit_count ?? "—"),
    },
  ];

  const fileDeltaColumns = [
    {
      id: "file",
      headerKey: "dashboard.col.file" as const,
      render: (row: DashboardBatchDeltasResult["by_file"][number]) => row.file_external_id,
    },
    {
      id: "missing",
      headerKey: "dashboard.col.missingTags" as const,
      render: (row: DashboardBatchDeltasResult["by_file"][number]) =>
        formatCount(row.missing_tags_count),
    },
    {
      id: "pattern",
      headerKey: "dashboard.col.patternFeedback" as const,
      render: (row: DashboardBatchDeltasResult["by_file"][number]) =>
        formatCount(row.pattern_feedback_count),
    },
    {
      id: "action",
      headerKey: "dashboard.col.action" as const,
      render: (row: DashboardBatchDeltasResult["by_file"][number]) =>
        onOpenTab ? (
          <button
            type="button"
            className="idx-btn idx-btn--sm"
            onClick={(e) => {
              e.stopPropagation();
              openFileContext(row.file_external_id);
            }}
          >
            {t("dashboard.openFileContext")}
          </button>
        ) : (
          "—"
        ),
    },
  ];

  const watchlistIds = parseCsvList(watchlistRaw);
  const refreshing = summaryLoading;

  return (
    <EditorPage title={t("dashboard.title")} hint={t("dashboard.hint")}>
      <div className="idx-dashboard">
        <div className="idx-dashboard__hero">
          <div className="idx-dashboard__hero-content">
            {onOpenTab ? (
              <div className="idx-dashboard__quick-actions">
                <span className="idx-dashboard__quick-label">{t("overview.quickLinks.label")}</span>
                <div className="idx-overview-quick-links">
                  <button
                    type="button"
                    className="idx-btn idx-btn--sm"
                    onClick={() => openQuick("build-metadata", "overview.quickLinks.buildMetadata")}
                  >
                    {t("overview.quickLinks.buildMetadata")}
                  </button>
                  <button
                    type="button"
                    className="idx-btn idx-btn--sm"
                    onClick={() => openQuick("query", "overview.quickLinks.query")}
                  >
                    {t("overview.quickLinks.query")}
                  </button>
                  <button
                    type="button"
                    className="idx-btn idx-btn--sm"
                    onClick={() => openQuick("configuration", "overview.quickLinks.config")}
                  >
                    {t("overview.quickLinks.config")}
                  </button>
                </div>
              </div>
            ) : null}
          </div>
          <button
            type="button"
            className="idx-btn idx-dashboard__refresh"
            onClick={refreshAll}
            disabled={refreshing}
          >
            {refreshing ? t("common.loading") : t("dashboard.refresh")}
          </button>
        </div>

        <section className="idx-panel idx-dashboard__panel">
          <header className="idx-panel__header">
            <h3 className="idx-panel__title">{t("dashboard.section.summary")}</h3>
            <p className="idx-panel__hint">{t("dashboard.section.summaryHint")}</p>
          </header>
          <div className="idx-panel__body">
            {summaryError ? <p className="idx-banner--error">{summaryError}</p> : null}
            {summary ? (
              <>
                <MetricSummary data={summary} metrics={DASHBOARD_SUMMARY_METRICS} />
                {summary.term_partition_enabled ? (
                  <p className="idx-pane__hint">
                    {t("dashboard.termPartitionThreshold", {
                      threshold: summary.activate_above_rows ?? "—",
                    })}
                  </p>
                ) : null}
              </>
            ) : summaryLoading ? (
              <p>{t("common.loading")}</p>
            ) : null}
          </div>
        </section>

        <section className="idx-panel idx-dashboard__panel">
          <header className="idx-panel__header">
            <h3 className="idx-panel__title">{t("dashboard.section.runtime")}</h3>
            <p className="idx-panel__hint">{t("dashboard.section.runtimeHint")}</p>
          </header>
          <div className="idx-panel__body">
            <DashboardRuntimePanel refreshKey={refreshKey + runtimeRefreshKey} />
          </div>
        </section>

        <section className="idx-panel idx-dashboard__panel">
          <header className="idx-panel__header">
            <h3 className="idx-panel__title">{t("dashboard.section.partitions")}</h3>
            <p className="idx-panel__hint">{t("dashboard.section.partitionsHint")}</p>
          </header>
          <div className="idx-panel__body">
            <DataTable
              columns={scopeColumns}
              rows={summary?.scopes ?? []}
              emptyMessage={summaryLoading ? t("common.loading") : t("dashboard.partitionsEmpty")}
            />
          </div>
        </section>

        <div className="idx-dashboard__ops-grid">
          <section className="idx-panel idx-dashboard__panel">
            <header className="idx-panel__header">
              <h3 className="idx-panel__title">{t("dashboard.section.tagReuse")}</h3>
              <p className="idx-panel__hint">{t("dashboard.section.tagReuseHint")}</p>
            </header>
            <div className="idx-panel__body">
              <div className="idx-dashboard__actions">
                <button
                  type="button"
                  className="idx-btn"
                  onClick={runTagReuse}
                  disabled={reuseOp.loading}
                >
                  {reuseOp.loading ? t("ops.running") : t("dashboard.runTagReuse")}
                </button>
                {reuseOp.loading ? (
                  <button type="button" className="idx-btn idx-btn--ghost" onClick={reuseOp.cancel}>
                    {t("ops.cancel")}
                  </button>
                ) : null}
              </div>
              {reuseOp.error ? <p className="idx-banner--error">{reuseOp.error}</p> : null}
              <MetricSummary data={reuseSummary} metrics={TAG_REUSE_METRICS} />
              <DataTable
                columns={reuseTermColumns}
                rows={asHitRows(reuseRows)}
                emptyMessage={
                  reuseOp.loading ? t("ops.running") : reuseOp.result ? t("tagReuse.noResults") : undefined
                }
              />
              {reuseOp.log ? (
                <OperationConsole log={reuseOp.log} loading={reuseOp.loading} />
              ) : null}
            </div>
          </section>

          <section className="idx-panel idx-dashboard__panel">
            <header className="idx-panel__header">
              <h3 className="idx-panel__title">{t("dashboard.section.deltas")}</h3>
              <p className="idx-panel__hint">{t("dashboard.section.deltasHint")}</p>
            </header>
            <div className="idx-panel__body">
              <div className="idx-dashboard__form">
                <label className="idx-label">
                  {t("dashboard.watchlist")}
                  <textarea
                    className="idx-input idx-dashboard__watchlist"
                    rows={4}
                    value={watchlistRaw}
                    onChange={(e) => setWatchlistRaw(e.target.value)}
                    placeholder={t("dashboard.watchlistPlaceholder")}
                  />
                </label>
                <div className="idx-field-row">
                  <label className="idx-label">
                    {t("fileContext.fileSpace")}
                    <input
                      className="idx-input"
                      value={fileSpace}
                      onChange={(e) => setFileSpace(e.target.value)}
                    />
                  </label>
                  <label className="idx-label">
                    {t("fileContext.scopeKey")}
                    <input
                      className="idx-input"
                      value={scopeKey}
                      onChange={(e) => setScopeKey(e.target.value)}
                      placeholder={t("fileContext.scopeKeyPlaceholder")}
                    />
                    <span className="idx-field-hint">{t("dashboard.deltasScopeHint")}</span>
                  </label>
                </div>
              </div>
              <div className="idx-dashboard__actions">
                <button
                  type="button"
                  className="idx-btn"
                  onClick={runBatchDeltas}
                  disabled={deltasOp.loading || watchlistIds.length === 0}
                >
                  {deltasOp.loading ? t("ops.running") : t("dashboard.scanDeltas")}
                </button>
                {deltasOp.loading ? (
                  <button type="button" className="idx-btn idx-btn--ghost" onClick={deltasOp.cancel}>
                    {t("ops.cancel")}
                  </button>
                ) : null}
              </div>
              {watchlistIds.length === 0 ? (
                <p className="idx-pane__hint">{t("dashboard.watchlistRequired")}</p>
              ) : null}
              {deltasOp.error ? <p className="idx-banner--error">{deltasOp.error}</p> : null}
              <MetricSummary data={deltasResult} metrics={DASHBOARD_DELTA_METRICS} />
              <DataTable
                columns={fileDeltaColumns}
                rows={fileDeltaRows}
                emptyMessage={
                  deltasOp.loading ? t("ops.running") : deltasOp.result ? t("dashboard.deltasEmpty") : undefined
                }
              />
              {deltasOp.log ? (
                <OperationConsole log={deltasOp.log} loading={deltasOp.loading} />
              ) : null}
            </div>
          </section>
        </div>
      </div>
    </EditorPage>
  );
}
