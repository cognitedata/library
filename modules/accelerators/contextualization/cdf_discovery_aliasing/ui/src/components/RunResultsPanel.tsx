import { useCallback, useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";

const PAGE = 200;

export type DiscoveryRunResult = {
  stem: string;
  run_rel: string;
  mtime_ms: number;
  status?: string;
  elapsed_ms?: number;
  task_count?: number;
  persistence_node_count?: number;
  dry_run?: boolean;
};

type RunDetail = {
  run_rel: string;
  end_of_process?: Record<string, unknown>;
  summary?: {
    status?: string;
    elapsed_ms?: number;
    task_count?: number;
    persistence_node_count?: number;
    dry_run?: boolean;
    failed_task_key?: string | null;
    warnings?: string[];
  };
};

type PipelineTaskRow = {
  task_id: string;
  function_external_id?: string | null;
  category?: string;
  status?: string | null;
  duration_sec?: number | null;
  output?: Record<string, unknown> | null;
  error?: string | null;
};

type PersistenceNodeIndex = {
  task_id: string;
  kind?: string;
  function_external_id?: string;
  label?: string;
  status?: string | null;
  duration_sec?: number | null;
  snapshot_present?: boolean;
  input_cohort?: {
    entity_row_count?: number;
    truncated?: boolean;
  };
  output?: {
    kind?: string;
    row_count?: number;
    summary?: Record<string, unknown>;
  };
};

type PersistenceNodeDetail = {
  task_id: string;
  kind?: string;
  function_external_id?: string;
  label?: string;
  handler_result?: Record<string, unknown>;
  input_cohort?: {
    entity_row_count?: number;
    truncated?: boolean;
    entity_rows?: Array<{ key?: string; columns?: Record<string, unknown> }>;
    predecessor_sources?: Array<{ raw_db?: string; raw_table?: string }>;
  };
  output?: Record<string, unknown>;
};

type MergedEntities = {
  instance_count?: number;
  inverted_index_sink_row_count?: number;
  instances?: Array<{
    instance_key?: string;
    properties?: Record<string, unknown>;
    contribution_count?: number;
  }>;
};

type ViewMode = "overview" | "pipeline" | "persistence" | "merged";

type Props = {
  refreshKey: number;
  /** Configure sidebar target (workflow_local | workflow_template | workflow_trigger:…). */
  runScopeKey: string;
  /** After a local run, prefer this scope so results stay visible if the sidebar target changes. */
  preferredRunScopeKey?: string;
};

function formatRunStem(stem: string): string {
  const m = /^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})$/.exec(stem);
  if (!m) return stem;
  return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:${m[6]}`;
}

/** Prefer filesystem mtime (when the file was written) over the timestamp embedded in the filename. */
function formatRunListLabel(r: DiscoveryRunResult): string {
  if (r.mtime_ms != null && r.mtime_ms > 0) {
    const written = new Date(r.mtime_ms).toLocaleString();
    const fromName = formatRunStem(r.stem);
    if (fromName !== r.stem && !written.startsWith(fromName.slice(0, 10))) {
      return `${written} (file ${fromName})`;
    }
    return written;
  }
  return formatRunStem(r.stem);
}

function subtabClass(active: boolean): string {
  return `discovery-tab${active ? " discovery-tab--active" : ""}`;
}

function taskOutputSummary(output: Record<string, unknown> | null | undefined): string {
  if (!output) return "";
  const parts: string[] = [];
  const fn = output.function_external_id;
  if (fn != null) parts.push(String(fn));
  for (const key of [
    "handler_id",
    "rows_read",
    "instances_written",
    "instances_listed",
    "rows_written",
    "query_source",
  ] as const) {
    if (output[key] != null) parts.push(`${key}=${String(output[key])}`);
  }
  return parts.join(" · ");
}

function persistenceKindLabel(kind: string | undefined, t: (k: string) => string): string {
  switch (kind) {
    case "view_save":
      return t("runResults.kindViewSave");
    case "raw_save":
      return t("runResults.kindRawSave");
    case "classic_save":
      return t("runResults.kindClassicSave");
    case "inverted_index":
      return t("runResults.kindInvertedIndex");
    default:
      return kind ?? "—";
  }
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

export function RunResultsPanel({ refreshKey, runScopeKey, preferredRunScopeKey }: Props) {
  const { t } = useAppSettings();
  const [showAllScopes, setShowAllScopes] = useState(false);
  const [discoveryRuns, setDiscoveryRuns] = useState<DiscoveryRunResult[]>([]);
  const [selectedDiscoveryStem, setSelectedDiscoveryStem] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>("overview");
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [pipelineItems, setPipelineItems] = useState<PipelineTaskRow[]>([]);
  const [pipelineTotal, setPipelineTotal] = useState<number | null>(null);
  const [pipelineNextOffset, setPipelineNextOffset] = useState(0);
  const [pipelineCategory, setPipelineCategory] = useState("");
  const [persistenceNodes, setPersistenceNodes] = useState<PersistenceNodeIndex[]>([]);
  const [selectedPersistenceTaskId, setSelectedPersistenceTaskId] = useState("");
  const [persistenceDetail, setPersistenceDetail] = useState<PersistenceNodeDetail | null>(null);
  const [mergedData, setMergedData] = useState<MergedEntities | null>(null);
  const [loadingDiscoveryList, setLoadingDiscoveryList] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [loadingPipeline, setLoadingPipeline] = useState(false);
  const [loadingPersistence, setLoadingPersistence] = useState(false);
  const [loadingMerged, setLoadingMerged] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [totalRunsAllScopes, setTotalRunsAllScopes] = useState<number | null>(null);
  const [activeScopeFilter, setActiveScopeFilter] = useState("");

  const effectiveScopeKey = showAllScopes
    ? ""
    : (preferredRunScopeKey ?? runScopeKey).trim();

  const selectedDiscoveryRun = useMemo(
    () => discoveryRuns.find((r) => r.stem === selectedDiscoveryStem) ?? null,
    [discoveryRuns, selectedDiscoveryStem],
  );

  const runRel = selectedDiscoveryRun?.run_rel ?? "";

  const loadDiscoveryRuns = useCallback(async () => {
    setLoadingDiscoveryList(true);
    setError(null);
    try {
      const url = effectiveScopeKey
        ? `/api/run-results/discovery?run_scope_key=${encodeURIComponent(effectiveScopeKey)}`
        : "/api/run-results/discovery";
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as {
        runs: DiscoveryRunResult[];
        total_all_scopes?: number;
        scope_filter?: string;
      };
      const list = data.runs ?? [];
      setDiscoveryRuns(list);
      setTotalRunsAllScopes(
        typeof data.total_all_scopes === "number" ? data.total_all_scopes : list.length,
      );
      setActiveScopeFilter(String(data.scope_filter ?? effectiveScopeKey));
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
    setPersistenceNodes([]);
    setSelectedPersistenceTaskId("");
    setPersistenceDetail(null);
  }, [selectedDiscoveryStem]);

  const loadDetail = useCallback(async () => {
    if (!runRel) {
      setDetail(null);
      return;
    }
    setLoadingDetail(true);
    try {
      const r = await fetch(`/api/run-results/discovery-detail?rel=${encodeURIComponent(runRel)}`);
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as RunDetail;
      setDetail(data);
      const persistenceCount = data.summary?.persistence_node_count ?? 0;
      setViewMode((prev) => {
        if (prev !== "overview") return prev;
        if (persistenceCount > 0) return "persistence";
        if ((data.summary?.task_count ?? 0) > 0) return "pipeline";
        return "overview";
      });
    } catch (e) {
      setError(String(e));
      setDetail(null);
    } finally {
      setLoadingDetail(false);
    }
  }, [runRel]);

  useEffect(() => {
    void loadDetail();
  }, [loadDetail, refreshKey]);

  const fetchPipelinePage = useCallback(
    async (offset: number, append: boolean) => {
      if (!runRel) {
        setPipelineItems([]);
        setPipelineTotal(null);
        setPipelineNextOffset(0);
        return;
      }
      setLoadingPipeline(true);
      setError(null);
      try {
        const params = new URLSearchParams({
          rel: runRel,
          offset: String(offset),
          limit: String(PAGE),
        });
        if (pipelineCategory.trim()) params.set("category", pipelineCategory.trim());
        const r = await fetch(`/api/run-results/discovery-pipeline-tasks?${params}`);
        if (!r.ok) throw new Error(await r.text());
        const data = (await r.json()) as { total: number; items: PipelineTaskRow[] };
        setPipelineTotal(data.total);
        if (append) {
          setPipelineItems((prev) => [...prev, ...data.items]);
          setPipelineNextOffset(offset + data.items.length);
        } else {
          setPipelineItems(data.items);
          setPipelineNextOffset(data.items.length);
        }
      } catch (e) {
        setError(String(e));
        if (!append) {
          setPipelineItems([]);
          setPipelineTotal(null);
          setPipelineNextOffset(0);
        }
      } finally {
        setLoadingPipeline(false);
      }
    },
    [runRel, pipelineCategory],
  );

  useEffect(() => {
    if (viewMode !== "pipeline") return;
    void fetchPipelinePage(0, false);
  }, [viewMode, runRel, fetchPipelinePage, refreshKey]);

  const loadPersistenceNodes = useCallback(async () => {
    if (!runRel) {
      setPersistenceNodes([]);
      setSelectedPersistenceTaskId("");
      setPersistenceDetail(null);
      return;
    }
    setLoadingPersistence(true);
    setError(null);
    try {
      const r = await fetch(
        `/api/run-results/discovery-persistence-nodes?rel=${encodeURIComponent(runRel)}`,
      );
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as { items?: PersistenceNodeIndex[] };
      const items = data.items ?? [];
      setPersistenceNodes(items);
      setSelectedPersistenceTaskId((prev) => {
        if (prev && items.some((x) => x.task_id === prev)) return prev;
        return items[0]?.task_id ?? "";
      });
    } catch (e) {
      setError(String(e));
      setPersistenceNodes([]);
      setSelectedPersistenceTaskId("");
      setPersistenceDetail(null);
    } finally {
      setLoadingPersistence(false);
    }
  }, [runRel]);

  useEffect(() => {
    if (viewMode !== "persistence") return;
    void loadPersistenceNodes();
  }, [viewMode, loadPersistenceNodes, refreshKey]);

  useEffect(() => {
    if (viewMode !== "persistence" || !runRel || !selectedPersistenceTaskId) {
      setPersistenceDetail(null);
      return;
    }
    let cancelled = false;
    void (async () => {
      setLoadingPersistence(true);
      try {
        const r = await fetch(
          `/api/run-results/discovery-persistence-node?rel=${encodeURIComponent(runRel)}&task_id=${encodeURIComponent(selectedPersistenceTaskId)}`,
        );
        if (!r.ok) throw new Error(await r.text());
        const data = (await r.json()) as { node?: PersistenceNodeDetail };
        if (!cancelled) setPersistenceDetail(data.node ?? null);
      } catch (e) {
        if (!cancelled) {
          setError(String(e));
          setPersistenceDetail(null);
        }
      } finally {
        if (!cancelled) setLoadingPersistence(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [viewMode, runRel, selectedPersistenceTaskId, refreshKey]);

  const loadMerged = useCallback(async () => {
    if (!runRel) {
      setMergedData(null);
      return;
    }
    setLoadingMerged(true);
    setError(null);
    try {
      const r = await fetch(
        `/api/run-results/discovery-persistence-merged?rel=${encodeURIComponent(runRel)}`,
      );
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as { merged_entities?: MergedEntities };
      setMergedData(data.merged_entities ?? null);
    } catch (e) {
      setError(String(e));
      setMergedData(null);
    } finally {
      setLoadingMerged(false);
    }
  }, [runRel]);

  useEffect(() => {
    if (viewMode !== "merged") return;
    void loadMerged();
  }, [viewMode, loadMerged, refreshKey]);

  const filteredPipelineItems = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return pipelineItems;
    return pipelineItems.filter((row) => JSON.stringify(row).toLowerCase().includes(q));
  }, [pipelineItems, filter]);

  const canLoadMorePipeline = pipelineTotal != null && pipelineNextOffset < pipelineTotal;
  const summary = detail?.summary;
  const runLabel = selectedDiscoveryRun
    ? `${formatRunStem(selectedDiscoveryRun.stem)}${selectedDiscoveryRun.status ? ` · ${selectedDiscoveryRun.status}` : ""}`
    : "";

  const selectedNodeSummary = persistenceNodes.find((n) => n.task_id === selectedPersistenceTaskId);

  return (
    <section className="discovery-panel">
      <h2 className="discovery-section-title">{t("runResults.title")}</h2>
      <p className="discovery-hint" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
        {t("runResults.hint")}
      </p>
      <div
        style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", alignItems: "center", marginBottom: 12 }}
      >
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
          onClick={() => void loadDiscoveryRuns()}
          disabled={loadingDiscoveryList}
        >
          {t("runResults.refresh")}
        </button>
        <label className="discovery-toolbar-check">
          <input
            type="checkbox"
            checked={showAllScopes}
            onChange={(e) => setShowAllScopes(e.target.checked)}
          />
          <span>{t("runResults.showAllScopes")}</span>
        </label>
        {discoveryRuns.length > 0 && (
          <>
            <label className="discovery-hint" htmlFor="discovery-run-results-discovery-select">
              {t("runResults.runLabel")}
            </label>
            <select
              id="discovery-run-results-discovery-select"
              value={selectedDiscoveryStem}
              onChange={(e) => setSelectedDiscoveryStem(e.target.value)}
            >
              {discoveryRuns.map((r) => {
                const label = formatRunListLabel(r);
                const status = r.status ? ` · ${r.status}` : "";
                const tasks =
                  r.task_count != null ? ` · ${r.task_count} ${t("runResults.summaryTaskCountShort")}` : "";
                const persist =
                  r.persistence_node_count != null && r.persistence_node_count > 0
                    ? ` · ${r.persistence_node_count} ${t("runResults.summaryPersistenceShort")}`
                    : "";
                return (
                  <option key={r.stem} value={r.stem}>
                    {label}
                    {status}
                    {tasks}
                    {persist}
                  </option>
                );
              })}
            </select>
            <button
              type="button"
              className="discovery-btn discovery-btn--ghost discovery-btn--sm"
              disabled={!runRel}
              onClick={() => void openModuleFile(runRel, setError)}
            >
              {t("runResults.openRunJson")}
            </button>
          </>
        )}
      </div>
      {error && (
        <p className="discovery-hint discovery-hint--warn" role="alert">
          {t("runResults.error")} {error}
        </p>
      )}
      {!loadingDiscoveryList && discoveryRuns.length === 0 && (
        <p className="discovery-hint">
          {activeScopeFilter &&
          totalRunsAllScopes != null &&
          totalRunsAllScopes > 0
            ? t("runResults.emptyDiscoveryScopeFiltered", {
                scope: activeScopeFilter,
                total: String(totalRunsAllScopes),
              })
            : t("runResults.emptyDiscovery")}
        </p>
      )}
      {selectedDiscoveryRun && (
        <>
          <nav
            className="discovery-tabs discovery-tabs--sub"
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
              className={subtabClass(viewMode === "persistence")}
              onClick={() => setViewMode("persistence")}
            >
              {t("runResults.viewPersistence")}
            </button>
            <button
              type="button"
              className={subtabClass(viewMode === "pipeline")}
              onClick={() => setViewMode("pipeline")}
            >
              {t("runResults.viewPipeline")}
            </button>
            <button
              type="button"
              className={subtabClass(viewMode === "merged")}
              onClick={() => setViewMode("merged")}
            >
              {t("runResults.viewMerged")}
            </button>
          </nav>
          {runLabel && (
            <p className="discovery-hint" style={{ marginBottom: 8 }}>
              <strong>{runLabel}</strong>
            </p>
          )}
        </>
      )}
      {viewMode === "overview" && selectedDiscoveryRun && (
        <>
          {loadingDetail && !summary && <p className="discovery-hint">{t("status.loading")}</p>}
          {summary && (
            <ul className="discovery-hint" style={{ marginBottom: 12, maxWidth: "72ch", listStyle: "none", padding: 0 }}>
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
              {summary.persistence_node_count != null && (
                <li>
                  <strong>{t("runResults.summaryPersistenceCount")}:</strong>{" "}
                  {summary.persistence_node_count}
                </li>
              )}
              {summary.dry_run && (
                <li>
                  <strong>{t("runResults.summaryDryRun")}:</strong> {t("runResults.yes")}
                </li>
              )}
              {summary.failed_task_key && (
                <li>
                  <strong>{t("runResults.failedTask")}:</strong> {summary.failed_task_key}
                </li>
              )}
            </ul>
          )}
          <p className="discovery-hint" style={{ maxWidth: "72ch" }}>
            {t("runResults.overviewHint")}
          </p>
        </>
      )}
      {viewMode === "persistence" && selectedDiscoveryRun && (
        <>
          <p className="discovery-hint" style={{ maxWidth: "72ch", marginBottom: 8 }}>
            {t("runResults.persistenceHint")}
          </p>
          {loadingPersistence && persistenceNodes.length === 0 && (
            <p className="discovery-hint">{t("status.loading")}</p>
          )}
          {!loadingPersistence && persistenceNodes.length === 0 && (
            <p className="discovery-hint">{t("runResults.noPersistence")}</p>
          )}
          {persistenceNodes.length > 0 && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.75rem", alignItems: "flex-start" }}>
              <div style={{ minWidth: 220 }}>
                <label className="discovery-hint" htmlFor="discovery-persistence-node-select">
                  {t("runResults.persistenceSelect")}
                </label>
                <select
                  id="discovery-persistence-node-select"
                  value={selectedPersistenceTaskId}
                  onChange={(e) => setSelectedPersistenceTaskId(e.target.value)}
                  style={{ display: "block", width: "100%", marginTop: 4 }}
                >
                  {persistenceNodes.map((n) => (
                    <option key={n.task_id} value={n.task_id}>
                      {persistenceKindLabel(n.kind, t)} — {n.label || n.task_id}
                      {n.status ? ` (${n.status})` : ""}
                    </option>
                  ))}
                </select>
                <ul className="discovery-hint" style={{ marginTop: 8, paddingLeft: 0, listStyle: "none" }}>
                  {persistenceNodes.map((n) => (
                    <li key={n.task_id}>
                      <button
                        type="button"
                        className={`discovery-btn discovery-btn--ghost discovery-btn--sm${n.task_id === selectedPersistenceTaskId ? " discovery-btn--active" : ""}`}
                        onClick={() => setSelectedPersistenceTaskId(n.task_id)}
                      >
                        {persistenceKindLabel(n.kind, t)}
                      </button>
                      {n.input_cohort?.entity_row_count != null && (
                        <span>
                          {" "}
                          · {n.input_cohort.entity_row_count} {t("runResults.cohortRows")}
                        </span>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
              <div style={{ flex: 1, minWidth: 280 }}>
                {selectedNodeSummary && (
                  <p className="discovery-hint" style={{ marginBottom: 8 }}>
                    <strong>{selectedNodeSummary.label || selectedNodeSummary.task_id}</strong>
                    {" · "}
                    {selectedNodeSummary.function_external_id}
                    {selectedNodeSummary.duration_sec != null &&
                      ` · ${selectedNodeSummary.duration_sec.toFixed(2)}s`}
                  </p>
                )}
                {persistenceDetail && (
                  <>
                    {persistenceDetail.handler_result &&
                      Object.keys(persistenceDetail.handler_result).length > 0 && (
                        <details open style={{ marginBottom: 8 }}>
                          <summary>{t("runResults.persistenceHandlerResult")}</summary>
                          <pre className="discovery-code-block" style={{ maxHeight: 200, overflow: "auto" }}>
                            {JSON.stringify(persistenceDetail.handler_result, null, 2)}
                          </pre>
                        </details>
                      )}
                    <details open style={{ marginBottom: 8 }}>
                      <summary>
                        {t("runResults.persistenceInputCohort")}
                        {persistenceDetail.input_cohort?.truncated &&
                          ` (${t("runResults.cohortTruncated")})`}
                      </summary>
                      <pre className="discovery-code-block" style={{ maxHeight: 240, overflow: "auto" }}>
                        {JSON.stringify(persistenceDetail.input_cohort ?? {}, null, 2)}
                      </pre>
                    </details>
                    <details open>
                      <summary>{t("runResults.persistenceOutput")}</summary>
                      <pre className="discovery-code-block" style={{ maxHeight: 240, overflow: "auto" }}>
                        {JSON.stringify(persistenceDetail.output ?? {}, null, 2)}
                      </pre>
                    </details>
                  </>
                )}
              </div>
            </div>
          )}
        </>
      )}
      {viewMode === "pipeline" && selectedDiscoveryRun && (
        <>
          <p className="discovery-hint" style={{ maxWidth: "72ch", marginBottom: 8 }}>
            {t("runResults.pipelineHint")}
          </p>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", marginBottom: 8 }}>
            <select
              value={pipelineCategory}
              onChange={(e) => setPipelineCategory(e.target.value)}
              aria-label={t("runResults.pipelineCategoryFilter")}
            >
              <option value="">{t("runResults.pipelineCategoryAll")}</option>
              <option value="query">query</option>
              <option value="transform">transform</option>
              <option value="validate">validate</option>
              <option value="filter">filter</option>
              <option value="persistence">persistence</option>
              <option value="cleanup">cleanup</option>
              <option value="other">other</option>
            </select>
            <input
              type="search"
              className="discovery-input"
              placeholder={t("runResults.filterPlaceholder")}
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
            />
          </div>
          {pipelineItems.length === 0 && !loadingPipeline && (
            <p className="discovery-hint">{t("runResults.noPipeline")}</p>
          )}
          {filteredPipelineItems.length > 0 && (
            <div className="discovery-table-wrap">
              <table className="discovery-table">
                <thead>
                  <tr>
                    <th>{t("runResults.table.taskId")}</th>
                    <th>{t("runResults.table.category")}</th>
                    <th>{t("runResults.table.status")}</th>
                    <th>{t("runResults.table.duration")}</th>
                    <th>{t("runResults.table.details")}</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredPipelineItems.map((row) => (
                    <tr key={row.task_id}>
                      <td>
                        <code>{row.task_id}</code>
                      </td>
                      <td>{row.category ?? "—"}</td>
                      <td>{row.status ?? "—"}</td>
                      <td>
                        {row.duration_sec != null ? `${row.duration_sec.toFixed(3)}s` : "—"}
                      </td>
                      <td className="discovery-hint">{taskOutputSummary(row.output) || row.error || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {pipelineTotal != null && pipelineTotal > 0 && (
            <p className="discovery-hint" style={{ marginTop: 8 }}>
              {t("runResults.showing", {
                from: 1,
                to: Math.min(pipelineNextOffset, pipelineTotal),
                total: pipelineTotal,
              })}
            </p>
          )}
          {canLoadMorePipeline && (
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              disabled={loadingPipeline}
              onClick={() => void fetchPipelinePage(pipelineNextOffset, true)}
            >
              {t("runResults.loadMore")}
            </button>
          )}
        </>
      )}
      {viewMode === "merged" && selectedDiscoveryRun && (
        <>
          <p className="discovery-hint" style={{ maxWidth: "72ch", marginBottom: 8 }}>
            {t("runResults.mergedHint")}
          </p>
          {loadingMerged && <p className="discovery-hint">{t("status.loading")}</p>}
          {!loadingMerged && (!mergedData || !mergedData.instance_count) && (
            <p className="discovery-hint">{t("runResults.noMerged")}</p>
          )}
          {mergedData && (mergedData.instance_count ?? 0) > 0 && (
            <>
              <ul className="discovery-hint" style={{ listStyle: "none", padding: 0 }}>
                <li>
                  <strong>{t("runResults.mergedInstanceCount")}:</strong> {mergedData.instance_count}
                </li>
                {mergedData.inverted_index_sink_row_count != null && (
                  <li>
                    <strong>{t("runResults.mergedIndexSinkCount")}:</strong>{" "}
                    {mergedData.inverted_index_sink_row_count}
                  </li>
                )}
              </ul>
              <pre className="discovery-code-block" style={{ maxHeight: 400, overflow: "auto" }}>
                {JSON.stringify(mergedData.instances ?? [], null, 2)}
              </pre>
            </>
          )}
        </>
      )}
    </section>
  );
}
