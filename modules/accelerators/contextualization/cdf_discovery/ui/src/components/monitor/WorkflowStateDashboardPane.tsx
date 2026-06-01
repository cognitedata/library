import { useEffect, useMemo, useState } from "react";
import {
  fetchMonitorSchedules,
  fetchMonitorWorkflowStateDetail,
  fetchMonitorWorkflowStateSummary,
  fetchMonitorWorkflowStates,
} from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";
import type {
  MonitorWorkflowDetail,
  MonitorWorkflowListItem,
  MonitorWorkflowRun,
  MonitorScheduleItem,
} from "../../types/monitorWorkflowState";
import {
  filterMonitorWorkflows,
  type MonitorFilterSource,
  type MonitorFilterStatus,
} from "../../utils/monitorWorkflowStateFilters";

type Translate = (key: MessageKey, vars?: Record<string, string | number>) => string;

function runStatusClass(status: string): string {
  if (status === "running") return "disc-monitor-status disc-monitor-status--running";
  if (status === "succeeded") return "disc-monitor-status disc-monitor-status--succeeded";
  if (status === "failed") return "disc-monitor-status disc-monitor-status--failed";
  return "disc-monitor-status";
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "—";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString();
}

function formatDuration(durationMs?: number | null): string {
  if (durationMs == null || !Number.isFinite(durationMs)) return "—";
  const totalSec = Math.max(0, Math.floor(durationMs / 1000));
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return `${min}m ${sec}s`;
}

function runStatusLabel(t: Translate, status: string): string {
  if (status === "running") return t("monitor.workflowState.status.running");
  if (status === "succeeded") return t("monitor.workflowState.status.succeeded");
  if (status === "failed") return t("monitor.workflowState.status.failed");
  return t("monitor.workflowState.status.unknown");
}

function sourceLabel(t: Translate, source: string): string {
  if (source === "cdf") return t("monitor.workflowState.source.cdf");
  if (source === "local") return t("monitor.workflowState.source.local");
  return source;
}

function recentRunLabel(t: Translate, run: MonitorWorkflowRun): string {
  return `${runStatusLabel(t, run.status)} · ${formatTimestamp(run.start_time)}`;
}

type MonitorSection = "workflowState" | "schedules";

type Props = {
  activeSection?: MonitorSection;
  onActiveSectionChange?: (next: MonitorSection) => void;
};

export function WorkflowStateDashboardPane({
  activeSection: activeSectionProp = "workflowState",
  onActiveSectionChange,
}: Props) {
  const { t } = useAppSettings();
  const [activeSection, setActiveSection] = useState<MonitorSection>(activeSectionProp);
  const [summary, setSummary] = useState<{
    workflow_count: number;
    run_count: number;
    running_workflows: number;
    succeeded_workflows: number;
    failed_workflows: number;
    degraded_workflows: number;
  } | null>(null);
  const [workflows, setWorkflows] = useState<MonitorWorkflowListItem[]>([]);
  const [schedules, setSchedules] = useState<MonitorScheduleItem[]>([]);
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<string>("");
  const [detail, setDetail] = useState<MonitorWorkflowDetail | null>(null);
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<MonitorFilterStatus>("all");
  const [sourceFilter, setSourceFilter] = useState<MonitorFilterSource>("all");
  const [scheduleSearch, setScheduleSearch] = useState("");
  const [scheduleTypeFilter, setScheduleTypeFilter] = useState<"all" | "pipeline" | "workflow">("all");
  const [scheduleStatusFilter, setScheduleStatusFilter] = useState<"all" | "running" | "succeeded" | "failed" | "unknown">("all");
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setActiveSection(activeSectionProp);
  }, [activeSectionProp]);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);
    Promise.all([
      fetchMonitorWorkflowStateSummary(),
      fetchMonitorWorkflowStates(),
      fetchMonitorSchedules(7),
    ])
      .then(([nextSummary, nextWorkflows, nextSchedules]) => {
        if (!mounted) return;
        setSummary(nextSummary);
        setWorkflows(nextWorkflows.workflows ?? []);
        setSchedules(nextSchedules.schedules ?? []);
      })
      .catch((err: unknown) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!mounted) return;
        setLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, []);

  const filteredWorkflows = useMemo(() => {
    return filterMonitorWorkflows(workflows, {
      search,
      status: statusFilter,
      source: sourceFilter,
    });
  }, [workflows, search, statusFilter, sourceFilter]);
  const filteredSchedules = useMemo(() => {
    const q = scheduleSearch.trim().toLowerCase();
    return schedules.filter((row) => {
      if (scheduleTypeFilter !== "all" && row.entity_type !== scheduleTypeFilter) return false;
      const lastStatus = String(row.last_status ?? "unknown");
      if (scheduleStatusFilter !== "all" && lastStatus !== scheduleStatusFilter) return false;
      if (!q) return true;
      return (
        String(row.entity_label ?? "").toLowerCase().includes(q) ||
        String(row.workflow_id ?? "").toLowerCase().includes(q) ||
        String(row.cron_expression ?? "").toLowerCase().includes(q)
      );
    });
  }, [scheduleSearch, scheduleStatusFilter, scheduleTypeFilter, schedules]);

  useEffect(() => {
    if (!selectedWorkflowId && filteredWorkflows[0]) {
      setSelectedWorkflowId(filteredWorkflows[0].workflow_id);
    }
    if (
      selectedWorkflowId &&
      filteredWorkflows.length > 0 &&
      !filteredWorkflows.some((row) => row.workflow_id === selectedWorkflowId)
    ) {
      setSelectedWorkflowId(filteredWorkflows[0].workflow_id);
    }
  }, [filteredWorkflows, selectedWorkflowId]);

  useEffect(() => {
    if (!selectedWorkflowId) {
      setDetail(null);
      return;
    }
    let mounted = true;
    setDetailLoading(true);
    fetchMonitorWorkflowStateDetail(selectedWorkflowId)
      .then((next) => {
        if (!mounted) return;
        setDetail(next);
      })
      .catch((err: unknown) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!mounted) return;
        setDetailLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, [selectedWorkflowId]);

  if (loading) {
    return <div className="disc-empty-hint">{t("monitor.workflowState.loading")}</div>;
  }
  if (error) {
    return (
      <div className="disc-doc-body">
        <p className="disc-banner--error" role="alert">
          {error}
        </p>
      </div>
    );
  }

  return (
    <div className="disc-monitor-pane">
      <header className="disc-monitor-pane__header">
        <h2 className="disc-monitor-pane__title">{t("monitor.workflowState.title")}</h2>
        <p className="disc-monitor-pane__hint">
          {activeSection === "workflowState" ? t("monitor.workflowState.subtitle") : t("monitor.schedule.subtitle")}
        </p>
      </header>

      <section className="disc-monitor-tabs" aria-label={t("monitor.tabs.label")}>
        <button
          type="button"
          className={`disc-gov-subtab${activeSection === "workflowState" ? " disc-gov-subtab--active" : ""}`}
          onClick={() => {
            setActiveSection("workflowState");
            onActiveSectionChange?.("workflowState");
          }}
        >
          {t("monitor.tabs.workflowState")}
        </button>
        <button
          type="button"
          className={`disc-gov-subtab${activeSection === "schedules" ? " disc-gov-subtab--active" : ""}`}
          onClick={() => {
            setActiveSection("schedules");
            onActiveSectionChange?.("schedules");
          }}
        >
          {t("monitor.tabs.schedules")}
        </button>
      </section>

      {activeSection === "workflowState" ? (
      <section className="disc-monitor-kpis" aria-label={t("monitor.workflowState.kpiLabel")}>
        <article className="disc-monitor-kpi-card">
          <p className="disc-monitor-kpi-card__label">{t("monitor.workflowState.kpi.totalWorkflows")}</p>
          <p className="disc-monitor-kpi-card__value">{summary?.workflow_count ?? 0}</p>
        </article>
        <article className="disc-monitor-kpi-card">
          <p className="disc-monitor-kpi-card__label">{t("monitor.workflowState.kpi.running")}</p>
          <p className="disc-monitor-kpi-card__value">{summary?.running_workflows ?? 0}</p>
        </article>
        <article className="disc-monitor-kpi-card">
          <p className="disc-monitor-kpi-card__label">{t("monitor.workflowState.kpi.succeeded")}</p>
          <p className="disc-monitor-kpi-card__value">{summary?.succeeded_workflows ?? 0}</p>
        </article>
        <article className="disc-monitor-kpi-card">
          <p className="disc-monitor-kpi-card__label">{t("monitor.workflowState.kpi.failed")}</p>
          <p className="disc-monitor-kpi-card__value">{summary?.failed_workflows ?? 0}</p>
        </article>
        <article className="disc-monitor-kpi-card">
          <p className="disc-monitor-kpi-card__label">{t("monitor.workflowState.kpi.degraded")}</p>
          <p className="disc-monitor-kpi-card__value">{summary?.degraded_workflows ?? 0}</p>
        </article>
      </section>
      ) : null}

      {activeSection === "workflowState" ? (
      <section className="disc-monitor-filters" aria-label={t("monitor.workflowState.filters.label")}>
        <label className="disc-monitor-filters__field">
          <span>{t("monitor.workflowState.filters.search")}</span>
          <input
            type="search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={t("monitor.workflowState.filters.searchPlaceholder")}
          />
        </label>
        <label className="disc-monitor-filters__field">
          <span>{t("monitor.workflowState.filters.source")}</span>
          <select
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value as MonitorFilterSource)}
          >
            <option value="all">{t("monitor.workflowState.filters.sourceAll")}</option>
            <option value="cdf">{t("monitor.workflowState.source.cdf")}</option>
            <option value="local">{t("monitor.workflowState.source.local")}</option>
          </select>
        </label>
        <label className="disc-monitor-filters__field">
          <span>{t("monitor.workflowState.filters.status")}</span>
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value as MonitorFilterStatus)}
          >
            <option value="all">{t("monitor.workflowState.filters.statusAll")}</option>
            <option value="running">{t("monitor.workflowState.status.running")}</option>
            <option value="succeeded">{t("monitor.workflowState.status.succeeded")}</option>
            <option value="failed">{t("monitor.workflowState.status.failed")}</option>
          </select>
        </label>
      </section>
      ) : null}

      {activeSection === "workflowState" ? (
      <section className="disc-monitor-content">
        <div className="disc-monitor-list">
          <h3>{t("monitor.workflowState.workflows.title")}</h3>
          {filteredWorkflows.length === 0 ? (
            <p className="disc-empty-hint">{t("monitor.workflowState.workflows.empty")}</p>
          ) : (
            <ul className="disc-monitor-workflow-list">
              {filteredWorkflows.map((row) => {
                const selected = row.workflow_id === selectedWorkflowId;
                return (
                  <li key={row.workflow_id}>
                    <button
                      type="button"
                      className={`disc-monitor-workflow-row${selected ? " is-selected" : ""}`}
                      onClick={() => setSelectedWorkflowId(row.workflow_id)}
                    >
                      <span className="disc-monitor-workflow-row__title">{row.label}</span>
                      <span className={runStatusClass(row.latest_status)}>
                        {runStatusLabel(t, row.latest_status)}
                      </span>
                      <span className="disc-monitor-workflow-row__meta">
                        {t("monitor.workflowState.workflows.runCount", { count: String(row.run_count) })}
                      </span>
                      <span className="disc-monitor-workflow-row__meta">
                        {row.sources.map((src) => sourceLabel(t, src)).join(", ")}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>

        <div className="disc-monitor-detail">
          <h3>{t("monitor.workflowState.detail.title")}</h3>
          {detailLoading ? (
            <p className="disc-empty-hint">{t("monitor.workflowState.loadingDetail")}</p>
          ) : !detail ? (
            <p className="disc-empty-hint">{t("monitor.workflowState.detail.empty")}</p>
          ) : (
            <>
              <div className="disc-monitor-detail__summary">
                <p>
                  <strong>{t("monitor.workflowState.detail.workflowId")}: </strong>
                  {detail.workflow.workflow_id}
                </p>
                <p>
                  <strong>{t("monitor.workflowState.detail.latestStatus")}: </strong>
                  {runStatusLabel(t, detail.workflow.latest_status)}
                </p>
                <p>
                  <strong>{t("monitor.workflowState.detail.lastRun")}: </strong>
                  {formatTimestamp(detail.workflow.last_run_time)}
                </p>
              </div>

              <h4>{t("monitor.workflowState.detail.recentRuns")}</h4>
              {detail.runs.length === 0 ? (
                <p className="disc-empty-hint">{t("monitor.workflowState.detail.noRuns")}</p>
              ) : (
                <ul className="disc-monitor-run-list">
                  {detail.runs.map((run) => (
                    <li key={`${run.source}:${run.run_id || run.start_time || "unknown"}`}>
                      <p>
                        <span className={runStatusClass(run.status)}>{recentRunLabel(t, run)}</span>
                      </p>
                      <p className="disc-monitor-run-list__meta">
                        {t("monitor.workflowState.detail.runInfo", {
                          source: sourceLabel(t, run.source),
                          duration: formatDuration(run.duration_ms),
                          failed: String(run.failed_tasks ?? 0),
                          total: String(run.total_tasks ?? 0),
                        })}
                      </p>
                      {run.error_summary ? (
                        <p className="disc-monitor-run-list__error">{run.error_summary}</p>
                      ) : null}
                    </li>
                  ))}
                </ul>
              )}
            </>
          )}
        </div>
      </section>
      ) : null}

      {activeSection === "schedules" ? (
      <section className="disc-monitor-list">
        <h3>{t("monitor.schedule.title")}</h3>
        <p className="disc-monitor-pane__hint">{t("monitor.schedule.subtitle")}</p>
        <section className="disc-monitor-filters" aria-label={t("monitor.schedule.filters.label")}>
          <label className="disc-monitor-filters__field">
            <span>{t("monitor.schedule.filters.search")}</span>
            <input
              type="search"
              value={scheduleSearch}
              onChange={(e) => setScheduleSearch(e.target.value)}
              placeholder={t("monitor.schedule.filters.searchPlaceholder")}
            />
          </label>
          <label className="disc-monitor-filters__field">
            <span>{t("monitor.schedule.filters.type")}</span>
            <select value={scheduleTypeFilter} onChange={(e) => setScheduleTypeFilter(e.target.value as "all" | "pipeline" | "workflow")}>
              <option value="all">{t("monitor.schedule.filters.typeAll")}</option>
              <option value="pipeline">{t("monitor.schedule.type.pipeline")}</option>
              <option value="workflow">{t("monitor.schedule.type.workflow")}</option>
            </select>
          </label>
          <label className="disc-monitor-filters__field">
            <span>{t("monitor.schedule.filters.status")}</span>
            <select
              value={scheduleStatusFilter}
              onChange={(e) =>
                setScheduleStatusFilter(
                  e.target.value as "all" | "running" | "succeeded" | "failed" | "unknown"
                )
              }
            >
              <option value="all">{t("monitor.schedule.filters.statusAll")}</option>
              <option value="running">{t("monitor.workflowState.status.running")}</option>
              <option value="succeeded">{t("monitor.workflowState.status.succeeded")}</option>
              <option value="failed">{t("monitor.workflowState.status.failed")}</option>
              <option value="unknown">{t("monitor.workflowState.status.unknown")}</option>
            </select>
          </label>
        </section>
        {filteredSchedules.length === 0 ? (
          <p className="disc-empty-hint">{t("monitor.schedule.empty")}</p>
        ) : (
          <div className="disc-monitor-schedule-table-wrap">
            <table className="disc-monitor-schedule-table">
              <thead>
                <tr>
                  <th>{t("monitor.schedule.table.name")}</th>
                  <th>{t("monitor.schedule.table.type")}</th>
                  <th>{t("monitor.schedule.table.cron")}</th>
                  <th>{t("monitor.schedule.table.avgRuntime")}</th>
                  <th>{t("monitor.schedule.table.lastRun")}</th>
                </tr>
              </thead>
              <tbody>
                {filteredSchedules.map((schedule) => (
                  <tr key={`${schedule.trigger_id}:${schedule.workflow_id}`}>
                    <td>{schedule.entity_label || schedule.workflow_id}</td>
                    <td>
                      {schedule.entity_type === "pipeline"
                        ? t("monitor.schedule.type.pipeline")
                        : t("monitor.schedule.type.workflow")}
                    </td>
                    <td>
                      <code>{schedule.cron_expression}</code>
                    </td>
                    <td>{formatDuration(schedule.avg_runtime_ms_7d ?? null)}</td>
                    <td>{formatTimestamp(schedule.last_run_time)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
      ) : null}
    </div>
  );
}
