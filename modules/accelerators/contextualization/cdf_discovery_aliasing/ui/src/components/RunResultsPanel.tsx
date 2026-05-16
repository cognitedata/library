import { useCallback, useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";

const PAGE = 200;

export type DiscoveryRunResult = {
  stem: string;
  report_rel: string;
  tasks_rel?: string;
  mtime_ms: number;
  status?: string;
  elapsed_ms?: number;
  task_count?: number;
  dry_run?: boolean;
};

type RunDetail = {
  report_rel: string;
  tasks_rel?: string | null;
  end_of_process?: Record<string, unknown>;
  summary?: {
    status?: string;
    elapsed_ms?: number;
    task_count?: number;
    dry_run?: boolean;
    failed_task_key?: string | null;
    warnings?: string[];
  };
};

type TaskRow = {
  task_id: string;
  status?: string | null;
  message?: string | null;
  parsed?: Record<string, unknown> | null;
};

type ViewMode = "overview" | "tasks";

type Props = {
  refreshKey: number;
  /** ``workflow_local`` | ``workflow_template`` | ``workflow_trigger:<rel>`` — matches ``run_scope`` in JSON. */
  runScopeKey: string;
};

function formatRunStem(stem: string): string {
  const m = /^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})$/.exec(stem);
  if (!m) return stem;
  return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:${m[6]}`;
}

function subtabClass(active: boolean): string {
  return `kea-tab${active ? " kea-tab--active" : ""}`;
}

function taskDetailsSummary(parsed: Record<string, unknown> | null | undefined): string {
  if (!parsed) return "";
  const parts: string[] = [];
  const fn = parsed.function_external_id;
  if (fn != null) parts.push(String(fn));
  for (const key of [
    "handler_id",
    "rows_read",
    "instances_written",
    "instances_listed",
    "rows_written",
    "query_source",
  ] as const) {
    if (parsed[key] != null) parts.push(`${key}=${String(parsed[key])}`);
  }
  return parts.join(" · ");
}

async function openModuleFile(rel: string, onError: (msg: string) => void): Promise<void> {
  try {
    const r = await fetch(`/api/file?rel=${encodeURIComponent(rel)}`);
    if (!r.ok) throw new Error(await r.text());
    const d = (await r.json()) as { content?: string };
    const blob = new Blob([d.content ?? ""], { type: "application/json;charset=utf-8" });
    const u = URL.createObjectURL(blob);
    window.open(u, "_blank", "noopener,noreferrer");
    window.setTimeout(() => URL.revokeObjectURL(u), 60_000);
  } catch (e) {
    onError(String(e));
  }
}

export function RunResultsPanel({ refreshKey, runScopeKey }: Props) {
  const { t } = useAppSettings();
  const [showAllScopes, setShowAllScopes] = useState(false);
  const [discoveryRuns, setDiscoveryRuns] = useState<DiscoveryRunResult[]>([]);
  const [selectedDiscoveryStem, setSelectedDiscoveryStem] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>("overview");
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [taskItems, setTaskItems] = useState<TaskRow[]>([]);
  const [taskTotal, setTaskTotal] = useState<number | null>(null);
  const [taskNextOffset, setTaskNextOffset] = useState(0);
  const [loadingDiscoveryList, setLoadingDiscoveryList] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [loadingTasksPage, setLoadingTasksPage] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const effectiveScopeKey = showAllScopes ? "" : runScopeKey.trim();

  const selectedDiscoveryRun = useMemo(
    () => discoveryRuns.find((r) => r.stem === selectedDiscoveryStem) ?? null,
    [discoveryRuns, selectedDiscoveryStem],
  );

  const loadDiscoveryRuns = useCallback(async () => {
    setLoadingDiscoveryList(true);
    setError(null);
    try {
      const url = effectiveScopeKey
        ? `/api/run-results/discovery?run_scope_key=${encodeURIComponent(effectiveScopeKey)}`
        : "/api/run-results/discovery";
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as { runs: DiscoveryRunResult[] };
      const list = data.runs ?? [];
      setDiscoveryRuns(list);
      setSelectedDiscoveryStem((prev) => {
        if (prev && list.some((x) => x.stem === prev)) return prev;
        return list[0]?.stem ?? "";
      });
    } catch (e) {
      setError(String(e));
      setDiscoveryRuns([]);
      setSelectedDiscoveryStem("");
    } finally {
      setLoadingDiscoveryList(false);
    }
  }, [effectiveScopeKey]);

  useEffect(() => {
    void loadDiscoveryRuns();
  }, [loadDiscoveryRuns, refreshKey]);

  useEffect(() => {
    setFilter("");
  }, [selectedDiscoveryStem]);

  const reportRel = selectedDiscoveryRun?.report_rel ?? "";
  const tasksRel = selectedDiscoveryRun?.tasks_rel ?? detail?.tasks_rel ?? "";

  const loadDetail = useCallback(async () => {
    if (!reportRel) {
      setDetail(null);
      return;
    }
    setLoadingDetail(true);
    try {
      const r = await fetch(`/api/run-results/discovery-detail?rel=${encodeURIComponent(reportRel)}`);
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as RunDetail;
      setDetail(data);
      const taskCount = data.summary?.task_count ?? 0;
      setViewMode((prev) => {
        if (prev !== "overview") return prev;
        if (taskCount > 0) return "tasks";
        return "overview";
      });
    } catch (e) {
      setError(String(e));
      setDetail(null);
    } finally {
      setLoadingDetail(false);
    }
  }, [reportRel]);

  useEffect(() => {
    void loadDetail();
  }, [loadDetail, refreshKey]);

  const fetchTasksPage = useCallback(
    async (offset: number, append: boolean) => {
      const rel = tasksRel || reportRel;
      if (!rel) {
        setTaskItems([]);
        setTaskTotal(null);
        setTaskNextOffset(0);
        return;
      }
      setLoadingTasksPage(true);
      setError(null);
      try {
        const url = `/api/run-results/discovery-tasks-preview?rel=${encodeURIComponent(
          rel,
        )}&offset=${offset}&limit=${PAGE}`;
        const r = await fetch(url);
        if (!r.ok) throw new Error(await r.text());
        const data = (await r.json()) as { total: number; items: TaskRow[] };
        setTaskTotal(data.total);
        if (append) {
          setTaskItems((prev) => [...prev, ...data.items]);
          setTaskNextOffset(offset + data.items.length);
        } else {
          setTaskItems(data.items);
          setTaskNextOffset(data.items.length);
        }
      } catch (e) {
        setError(String(e));
        if (!append) {
          setTaskItems([]);
          setTaskTotal(null);
          setTaskNextOffset(0);
        }
      } finally {
        setLoadingTasksPage(false);
      }
    },
    [reportRel, tasksRel],
  );

  useEffect(() => {
    if (viewMode !== "tasks") return;
    void fetchTasksPage(0, false);
  }, [viewMode, reportRel, tasksRel, fetchTasksPage, refreshKey]);

  const filteredTaskItems = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return taskItems;
    return taskItems.filter((row) => JSON.stringify(row).toLowerCase().includes(q));
  }, [taskItems, filter]);

  const canLoadMoreTasks = taskTotal != null && taskNextOffset < taskTotal;

  const summary = detail?.summary;
  const runLabel = selectedDiscoveryRun
    ? `${formatRunStem(selectedDiscoveryRun.stem)}${selectedDiscoveryRun.status ? ` · ${selectedDiscoveryRun.status}` : ""}`
    : "";

  return (
    <section className="kea-panel">
      <h2 className="kea-section-title">{t("runResults.title")}</h2>
      <p className="kea-hint" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
        {t("runResults.hint")}
      </p>
      <div
        style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", alignItems: "center", marginBottom: 12 }}
      >
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          onClick={() => void loadDiscoveryRuns()}
          disabled={loadingDiscoveryList}
        >
          {t("runResults.refresh")}
        </button>
        <label className="kea-toolbar-check">
          <input
            type="checkbox"
            checked={showAllScopes}
            onChange={(e) => setShowAllScopes(e.target.checked)}
          />
          <span>{t("runResults.showAllScopes")}</span>
        </label>
        {discoveryRuns.length > 0 && (
          <>
            <label className="kea-hint" htmlFor="kea-run-results-discovery-select">
              {t("runResults.runLabel")}
            </label>
            <select
              id="kea-run-results-discovery-select"
              value={selectedDiscoveryStem}
              onChange={(e) => setSelectedDiscoveryStem(e.target.value)}
            >
              {discoveryRuns.map((r) => {
                const label = formatRunStem(r.stem);
                const status = r.status ? ` · ${r.status}` : "";
                const tasks =
                  r.task_count != null ? ` · ${r.task_count} ${t("runResults.summaryTaskCountShort")}` : "";
                return (
                  <option key={r.stem} value={r.stem}>
                    {label}
                    {status}
                    {tasks}
                  </option>
                );
              })}
            </select>
            <button
              type="button"
              className="kea-btn kea-btn--ghost kea-btn--sm"
              disabled={!reportRel}
              onClick={() => void openModuleFile(reportRel, setError)}
            >
              {t("runResults.openRaw")}
            </button>
            <button
              type="button"
              className="kea-btn kea-btn--ghost kea-btn--sm"
              disabled={!tasksRel}
              onClick={() => void openModuleFile(String(tasksRel), setError)}
            >
              {t("runResults.openTasksJson")}
            </button>
          </>
        )}
      </div>
      {error && (
        <p className="kea-hint kea-hint--warn" role="alert">
          {t("runResults.error")} {error}
        </p>
      )}
      {!loadingDiscoveryList && discoveryRuns.length === 0 && (
        <p className="kea-hint">{t("runResults.emptyDiscovery")}</p>
      )}
      {selectedDiscoveryRun && (
        <>
          <nav
            className="kea-tabs kea-tabs--sub"
            aria-label={t("runResults.viewNav")}
            style={{ marginBottom: 12 }}
          >
            <button
              type="button"
              className={subtabClass(viewMode === "overview")}
              onClick={() => setViewMode("overview")}
            >
              {t("runResults.viewOverview")}
            </button>
            <button
              type="button"
              className={subtabClass(viewMode === "tasks")}
              onClick={() => setViewMode("tasks")}
            >
              {t("runResults.viewTasks")}
            </button>
          </nav>
          {runLabel && (
            <p className="kea-hint" style={{ marginBottom: 8 }}>
              <strong>{runLabel}</strong>
            </p>
          )}
        </>
      )}
      {viewMode === "overview" && selectedDiscoveryRun && (
        <>
          {loadingDetail && !summary && <p className="kea-hint">{t("status.loading")}</p>}
          {summary && (
            <ul className="kea-hint" style={{ marginBottom: 12, maxWidth: "72ch", listStyle: "none", padding: 0 }}>
              {summary.status != null && (
                <li>
                  <strong>{t("runResults.summaryStatus")}:</strong> {String(summary.status)}
                </li>
              )}
              {summary.elapsed_ms != null && (
                <li>
                  <strong>{t("runResults.summaryElapsed")}:</strong>{" "}
                  {t("runResults.summaryElapsedValue", { ms: summary.elapsed_ms })}
                </li>
              )}
              {summary.task_count != null && (
                <li>
                  <strong>{t("runResults.summaryTaskCount")}:</strong> {summary.task_count}
                </li>
              )}
              {summary.dry_run === true && (
                <li>
                  <strong>{t("runResults.summaryDryRun")}:</strong> {t("runResults.yes")}
                </li>
              )}
              {summary.failed_task_key != null && String(summary.failed_task_key).trim() && (
                <li>
                  <strong>{t("runResults.failedTask")}:</strong>{" "}
                  <code>{String(summary.failed_task_key)}</code>
                </li>
              )}
            </ul>
          )}
          {summary?.warnings && summary.warnings.length > 0 && (
            <ul className="kea-hint" style={{ marginBottom: 12 }}>
              {summary.warnings.map((w, i) => (
                <li key={`${i}-${w}`}>{w}</li>
              ))}
            </ul>
          )}
          <p className="kea-hint" style={{ maxWidth: "72ch" }}>
            {t("runResults.overviewHint")}
          </p>
        </>
      )}
      {viewMode === "tasks" && selectedDiscoveryRun && (
        <>
          <p className="kea-hint" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
            {t("runResults.tasksHint")}
          </p>
          {taskTotal === 0 && !loadingTasksPage && <p className="kea-hint">{t("runResults.noTasks")}</p>}
          {taskTotal != null && taskTotal > 0 && (
            <p className="kea-hint" style={{ marginBottom: 8 }}>
              {t("runResults.totalRows", { count: taskTotal })}
              {" · "}
              {t("runResults.showing", { from: 1, to: taskItems.length, total: taskTotal })}
            </p>
          )}
          <input
            type="search"
            className="kea-input"
            placeholder={t("runResults.filterPlaceholder")}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            style={{ maxWidth: 420, marginBottom: 12, display: "block" }}
          />
          {loadingTasksPage && taskItems.length === 0 && <p className="kea-hint">{t("status.loading")}</p>}
          {(!loadingTasksPage || taskItems.length > 0) && filteredTaskItems.length > 0 && (
            <div className="kea-table-wrap">
              <table className="kea-table">
                <thead>
                  <tr>
                    <th>{t("runResults.table.taskId")}</th>
                    <th>{t("runResults.table.status")}</th>
                    <th>{t("runResults.table.details")}</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredTaskItems.map((row) => {
                    const details = taskDetailsSummary(row.parsed) || (row.message ?? "").slice(0, 240);
                    return (
                      <tr key={row.task_id}>
                        <td style={{ maxWidth: 280, wordBreak: "break-all" }}>{row.task_id}</td>
                        <td>{row.status ?? ""}</td>
                        <td
                          style={{
                            maxWidth: 560,
                            wordBreak: "break-word",
                            fontFamily: "monospace",
                            fontSize: "0.85em",
                          }}
                        >
                          {details}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          {canLoadMoreTasks && (
            <button
              type="button"
              className="kea-btn kea-btn--sm"
              style={{ marginTop: 12 }}
              disabled={loadingTasksPage}
              onClick={() => void fetchTasksPage(taskNextOffset, true)}
            >
              {t("runResults.loadMore")}
            </button>
          )}
        </>
      )}
    </section>
  );
}
