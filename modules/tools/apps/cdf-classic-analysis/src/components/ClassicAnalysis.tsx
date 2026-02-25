import { useState, useRef, useEffect } from "react";
import { useLegacySDK } from "../auth";
import "./ClassicAnalysis.css";
import {
  runAnalysis,
  getMetadataKeysList,
  getTotalCount,
  getDatasetResourceCounts,
  getAggregateCountNoFilter,
  getGlobalExtendedCounts,
  type AnalysisResult,
  type ResourceType,
  type DataSetIdEither,
  type DatasetResourceCounts,
  type GlobalExtendedCounts,
} from "../analysis";
import {
  selectFilterKeysForDeepAnalysis,
  slugForFileName,
} from "../deepAnalysis";

const DEEP_ANALYSIS_RETRY_ATTEMPTS = 3;
const DEEP_ANALYSIS_RETRY_DELAY_MS = 1500;
const DEEP_ANALYSIS_PACE_DELAY_MS = 400;

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function withRetry<T>(
  fn: () => Promise<T>,
  opts: { maxAttempts?: number; delayMs?: number } = {}
): Promise<T> {
  const maxAttempts = opts.maxAttempts ?? DEEP_ANALYSIS_RETRY_ATTEMPTS;
  const delayMs = opts.delayMs ?? DEEP_ANALYSIS_RETRY_DELAY_MS;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      if (attempt < maxAttempts) await delay(delayMs);
    }
  }
  throw lastErr;
}

type AllDatasetsCountsState = (DatasetResourceCounts & Partial<GlobalExtendedCounts>) | "loading" | null;

const RESOURCE_OPTIONS: { value: ResourceType; label: string }[] = [
  { value: "assets", label: "Assets" },
  { value: "timeseries", label: "Time series" },
  { value: "events", label: "Events" },
  { value: "sequences", label: "Sequences" },
  { value: "files", label: "Files" },
];

/** Strip trailing " (count)" so "part number (28)" → "part number" for API. */
function filterKeyForApi(raw: string): string {
  const s = raw.trim();
  const m = s.match(/^(.+)\s\((\d+)\)$/);
  return m ? m[1].trim() : s;
}

/** Format extended count for All Datasets: "…" while loading, number or "—". */
function fmtExtended(counts: AllDatasetsCountsState, key: keyof GlobalExtendedCounts): string {
  if (counts === "loading") return "…";
  if (!counts || typeof counts !== "object") return "—";
  const n = counts[key];
  return typeof n === "number" ? n.toLocaleString() : "…";
}


function is429(err: unknown): boolean {
  if (err == null) return false;
  const e = err as { status?: number; code?: number; message?: string };
  return e.status === 429 || e.code === 429 || (typeof e.message === "string" && e.message.includes("429"));
}

function stripSortKeys(body: Record<string, unknown>): Record<string, unknown> {
  const out = { ...body };
  delete out.orderBy;
  delete out.sort;
  delete out.sortOrder;
  return out;
}

function sortItemsByCount(res: { items?: unknown[] }): { items?: unknown[] } {
  if (!Array.isArray(res.items) || res.items.length === 0) return res;
  const first = res.items[0] as { count?: number } | undefined;
  if (first == null || typeof first.count !== "number") return res;
  const sorted = [...res.items].sort((a, b) => ((b as { count?: number }).count ?? 0) - ((a as { count?: number }).count ?? 0));
  return { ...res, items: sorted };
}

const parseListResponseFromRaw = (raw: unknown): { items?: unknown[] } =>
  ((raw as { data?: { items?: unknown[] }; items?: unknown[] }).data ?? raw ?? {}) as { items?: unknown[] };

export type ClientAdapter = {
  post: (path: string, body: Record<string, unknown>) => Promise<{ items?: unknown[] }>;
  get?: (path: string, params?: Record<string, unknown>) => Promise<{ items?: unknown[] }>;
};

function getClientAdapter(sdk: unknown): ClientAdapter {
  const c = sdk as {
    post?: (path: string, options?: { data?: unknown }) => Promise<{ data?: unknown }>;
    get?: (path: string, options?: { params?: Record<string, unknown> }) => Promise<{ data?: unknown }>;
    project?: string;
  };
  if (typeof c?.post === "function") {
    const parseResponse = parseListResponseFromRaw;
    const adapter: ClientAdapter = {
      post: async (path, body) => {
        const isAggregateCount =
          path.includes("/aggregate") &&
          (body.aggregate === "count" ||
            (body.advancedFilter != null && body.aggregate == null) ||
            ((body.filter as { dataSetIds?: unknown[] } | undefined)?.dataSetIds != null && body.aggregate == null));
        const buildPayload = (b: Record<string, unknown>) =>
          isAggregateCount ? { ...b, data: b } : { data: b };

        try {
          const payload = buildPayload(body);
          const raw = await c.post!(path, payload as Record<string, unknown>);
          return parseResponse(raw);
        } catch (e) {
          if (!is429(e)) throw e;
          await new Promise((r) => setTimeout(r, 1000));
          const noSortBody = stripSortKeys(body);
          const payload = buildPayload(noSortBody);
          const raw = await c.post!(path, payload as Record<string, unknown>);
          const res = parseResponse(raw);
          return sortItemsByCount(res);
        }
      },
    };
    if (typeof c.get === "function") {
      adapter.get = async (path, params) => {
        const raw = await c.get!(path, { params: params ?? {} });
        const body =
          typeof (raw as { json?: () => Promise<unknown> })?.json === "function"
            ? await (raw as { json: () => Promise<unknown> }).json()
            : raw;
        return parseResponse(body);
      };
    }
    return adapter;
  }
  const alt = sdk as { request?: (method: string, path: string, body?: unknown, opts?: { params?: Record<string, unknown> }) => Promise<unknown> };
  if (typeof alt?.request === "function") {
    const adapter: ClientAdapter = {
      post: async (path, body) => {
        try {
          return (await alt.request!("POST", path, body)) as { items?: unknown[] };
        } catch (e) {
          if (!is429(e)) throw e;
          await new Promise((r) => setTimeout(r, 1000));
          const noSortBody = stripSortKeys(body);
          const res = (await alt.request!("POST", path, noSortBody)) as { items?: unknown[] };
          return sortItemsByCount(res);
        }
      },
    };
    adapter.get = async (path, params) => {
      if (!params || Object.keys(params).length === 0)
        return (await alt.request!("GET", path)) as { items?: unknown[] };
      const sp = new URLSearchParams();
      for (const [k, v] of Object.entries(params)) if (v != null) sp.set(k, String(v));
      const q = sp.toString();
      const url = q ? `${path}?${q}` : path;
      return (await alt.request!("GET", url)) as { items?: unknown[] };
    };
    return adapter;
  }
  throw new Error("CDF SDK does not expose post or request. Classic analysis requires an authenticated client with aggregate API support.");
}

export interface DataSetOption {
  id: number;
  externalId?: string;
  name?: string;
}

export function ClassicAnalysis() {
  const { sdk } = useLegacySDK();
  const [resourceType, setResourceType] = useState<ResourceType>("assets");
  const [filterKey, setFilterKey] = useState("");
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [metadataKeysList, setMetadataKeysList] = useState<{ key: string; count: number }[]>([]);
  const [loadingKeys, setLoadingKeys] = useState(false);
  const [datasets, setDatasets] = useState<DataSetOption[]>([]);
  const [selectedDatasetIds, setSelectedDatasetIds] = useState<number[]>([]);
  const [loadingDatasets, setLoadingDatasets] = useState(false);
  const [datasetCounts, setDatasetCounts] = useState<Record<number, DatasetResourceCounts | "loading">>({});
  const [loadingCounts, setLoadingCounts] = useState(false);
  const filterKeyInputRef = useRef<HTMLInputElement>(null);
  const [allDatasetsCounts, setAllDatasetsCounts] = useState<AllDatasetsCountsState>(null);
  const [deepAnalysisProgress, setDeepAnalysisProgress] = useState<{
    resourceTypeLabel: string;
    percent: number;
  } | null>(null);
  const [deepAnalysisResourceTypes, setDeepAnalysisResourceTypes] = useState<Record<ResourceType, boolean>>({
    assets: true,
    timeseries: true,
    events: true,
    sequences: true,
    files: true,
  });
  const [deepCoveragePct, setDeepCoveragePct] = useState(60);
  const [deepResults, setDeepResults] = useState<{ rt: string; rtLabel: string; count: number; keys: string[]; report: string }[]>([]);

  useEffect(() => {
    if (!sdk) return;
    const project = (sdk as { project?: string }).project;
    if (!project) return;
    setAllDatasetsCounts("loading");
    const client = getClientAdapter(sdk);
    (async () => {
      try {
        const assets = await getAggregateCountNoFilter(client, project, "assets");
        const timeseries = await getAggregateCountNoFilter(client, project, "timeseries");
        const events = await getAggregateCountNoFilter(client, project, "events");
        const sequences = await getAggregateCountNoFilter(client, project, "sequences");
        const files = await getAggregateCountNoFilter(client, project, "files");
        setAllDatasetsCounts({ assets, timeseries, events, sequences, files });
        try {
          const extended = await getGlobalExtendedCounts(client, project);
          setAllDatasetsCounts((prev) =>
            prev && typeof prev !== "string" ? { ...prev, ...extended } : prev
          );
        } catch {
          setAllDatasetsCounts((prev) =>
            prev && typeof prev !== "string"
              ? { ...prev, transformations: 0, functions: 0, workflows: 0, rawTables: 0 }
              : prev
          );
        }
      } catch {
        setAllDatasetsCounts(null);
      }
    })();
  }, [sdk]);
  const resourceTypeSelectRef = useRef<HTMLSelectElement>(null);

  const COUNT_LOAD_CAP = 50;

  const loadDatasets = async () => {
    if (!sdk) return;
    setLoadingDatasets(true);
    setDatasetCounts({});
    try {
      const listApi = (sdk as { datasets?: { list: (q?: unknown) => { autoPagingToArray: (opts: { limit: number }) => Promise<DataSetOption[]> } } }).datasets?.list?.();
      if (!listApi?.autoPagingToArray) {
        setDatasets([]);
        return;
      }
      const arr = await listApi.autoPagingToArray({ limit: 500 });
      const list = Array.isArray(arr) ? arr : [];
      const sorted = [...list].sort((a, b) => {
        const na = (a.name ?? a.externalId ?? `ID ${a.id}`).toLowerCase();
        const nb = (b.name ?? b.externalId ?? `ID ${b.id}`).toLowerCase();
        return na.localeCompare(nb, undefined, { sensitivity: "base" });
      });
      setDatasets(sorted);
      const project = (sdk as { project?: string }).project;
      if (project && sorted.length > 0) {
        const client = getClientAdapter(sdk);
        const toLoad = sorted.slice(0, COUNT_LOAD_CAP);
        toLoad.forEach((d) => setDatasetCounts((prev) => ({ ...prev, [d.id]: "loading" })));
        setLoadingCounts(true);
        try {
          for (const d of toLoad) {
            try {
              const counts = await getDatasetResourceCounts(client, project, { id: d.id });
              setDatasetCounts((prev) => ({ ...prev, [d.id]: counts }));
            } catch {
              setDatasetCounts((prev) => ({ ...prev, [d.id]: { assets: 0, timeseries: 0, events: 0, sequences: 0, files: 0 } }));
            }
          }
        } finally {
          setLoadingCounts(false);
        }
      }
    } catch {
      setDatasets([]);
    } finally {
      setLoadingDatasets(false);
    }
  };

  const toggleDatasetSelection = (id: number) => {
    setSelectedDatasetIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  };

  /** Always pass numeric id so aggregate/list filters apply; CDF filter uses dataSetId as number. */
  const dataSetIdsForApi: DataSetIdEither[] | undefined =
    selectedDatasetIds.length > 0 ? selectedDatasetIds.map((id) => ({ id })) : undefined;

  const loadMetadataKeys = async () => {
    if (!sdk) return;
    const project = (sdk as { project?: string }).project;
    if (!project) return;
    setLoadingKeys(true);
    try {
      const client = getClientAdapter(sdk);
      const list = await getMetadataKeysList(client, resourceType, project, dataSetIdsForApi);
      setMetadataKeysList(list);
      if (list.length === 0) setFilterKey("no metadata");
      else setFilterKey("");
    } catch {
      setMetadataKeysList([]);
      setFilterKey("no metadata");
    } finally {
      setLoadingKeys(false);
    }
  };

  const run = async () => {
    if (!sdk) return;
    const project = (sdk as { project?: string }).project;
    if (!project) {
      setResult({
        resourceType,
        filterKey,
        rows: [],
        error: "SDK has no project configured. Use the app from Fusion in a CDF project.",
      });
      return;
    }
    setLoading(true);
    setResult(null);
    try {
      const client = getClientAdapter(sdk);
      const rawKey = filterKey.trim();
      if (!rawKey || rawKey.toLowerCase() === "no metadata") {
        setResult({ resourceType, filterKey: rawKey || "", rows: [], error: "Enter a filter key (or load metadata keys and choose one)." });
        return;
      }
      const key = filterKeyForApi(rawKey);
      const res = await runAnalysis(client, resourceType, key, project, dataSetIdsForApi);
      setResult(res);
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      let message = err.message;
      const causeVal = "cause" in err ? (err as { cause?: unknown }).cause : undefined;
      const cause = causeVal instanceof Error ? causeVal.message : causeVal != null ? String(causeVal) : "";
      if (cause && cause !== message) message += ` (${cause})`;
      if (message === "Failed to fetch" || message.startsWith("Failed to fetch")) {
        message += ". Ensure this app is deployed for the current CDF project (app.json deployments) and that you have network access to the CDF cluster.";
      }
      setResult({
        resourceType,
        filterKey,
        rows: [],
        error: message,
      });
    } finally {
      setLoading(false);
    }
  };

  const downloadTxt = () => {
    if (!result?.rows.length) return;
    const text = result.rows.map((r) => r.text).join("");
    const name = `${result.resourceType}_analysis_results_${result.filterKey}.txt`;
    const blob = new Blob([text], { type: "text/plain" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = name;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const runDeepAnalysis = async () => {
    if (!sdk) return;
    const project = (sdk as { project?: string }).project;
    if (!project) return;
    const client = getClientAdapter(sdk);
    const dataSetIds = dataSetIdsForApi;
    const resourceTypes = (RESOURCE_OPTIONS.map((o) => o.value).filter((rt) => deepAnalysisResourceTypes[rt])) as ResourceType[];
    if (resourceTypes.length === 0) {
      setDeepAnalysisProgress(null);
      return;
    }
    setDeepResults([]);
    setDeepAnalysisProgress({ resourceTypeLabel: "…", percent: 0 });
    let totalSteps = 0;
    const resourceTypesWithCount: { rt: ResourceType; totalCount: number }[] = [];
    for (const rt of resourceTypes) {
      const totalCount = await withRetry(() => getTotalCount(client, project, rt, dataSetIds));
      if (totalCount > 0) {
        resourceTypesWithCount.push({ rt, totalCount });
        const metaList = await withRetry(() => getMetadataKeysList(client, rt, project, dataSetIds));
        const keys = selectFilterKeysForDeepAnalysis(metaList, totalCount, rt, deepCoveragePct / 100);
        totalSteps += keys.length;
      }
    }
    let stepsDone = 0;
    const datasetLines =
      selectedDatasetIds.length > 0
        ? selectedDatasetIds
            .map((id) => {
              const d = datasets.find((x) => x.id === id);
              return d ? (d.name ?? d.externalId ?? `ID ${d.id}`) : String(id);
            })
            .join("\n  - ")
        : "All datasets";
    const collected: typeof deepResults = [];
    try {
      for (let rtIdx = 0; rtIdx < resourceTypesWithCount.length; rtIdx++) {
      const { rt, totalCount } = resourceTypesWithCount[rtIdx]!;
      const label = RESOURCE_OPTIONS.find((o) => o.value === rt)?.label ?? rt;
      const metaList = await withRetry(() => getMetadataKeysList(client, rt, project, dataSetIds));
      const keys = selectFilterKeysForDeepAnalysis(metaList, totalCount, rt, deepCoveragePct / 100);
      const lines: string[] = [
          "CDF Project: " + project,
          "",
          "Resource type: " + label,
          "Aggregate count: " + totalCount.toLocaleString(),
          "Instance count threshold: " + deepCoveragePct + "%",
          "",
          "Datasets:",
          "  - " + datasetLines,
          "",
          "Metadata keys analysed: " + keys.length,
          ...keys.map((k) => "  - " + k),
          "",
          "---",
          "",
        ];
        for (let ki = 0; ki < keys.length; ki++) {
          const key = keys[ki]!;
          setDeepAnalysisProgress({
            resourceTypeLabel: label,
            percent: totalSteps > 0 ? (stepsDone / totalSteps) * 100 : 0,
          });
          const apiKey = filterKeyForApi(key);
          await delay(DEEP_ANALYSIS_PACE_DELAY_MS);
          try {
            const res = await withRetry(() => runAnalysis(client, rt, apiKey, project, dataSetIds));
            lines.push(`=== Filter key: ${key} ===`);
            lines.push("");
            if (res.error) {
              lines.push("Error: " + res.error);
            } else {
              lines.push(res.rows.map((r) => r.text).join(""));
            }
            lines.push("");
          } catch (e) {
            lines.push(`=== Filter key: ${key} ===`);
            lines.push("");
            lines.push("Error: " + (e instanceof Error ? e.message : String(e)));
            lines.push("");
          }
          stepsDone++;
          setDeepAnalysisProgress({
            resourceTypeLabel: label,
            percent: totalSteps > 0 ? (stepsDone / totalSteps) * 100 : 0,
          });
        }
        collected.push({ rt, rtLabel: label, count: totalCount, keys, report: lines.join("\n") });
      }
      setDeepResults(collected);
    } finally {
      setDeepAnalysisProgress(null);
    }
  };

  const downloadDeepReport = () => {
    const combined = deepResults.map((r) => r.report).join("\n\n");
    const project = (sdk as { project?: string })?.project ?? "project";
    const projectSlug = slugForFileName(project, 24);
    const firstDatasetSlug =
      selectedDatasetIds.length > 0
        ? (() => {
            const d = datasets.find((x) => x.id === selectedDatasetIds[0]);
            const label = d?.name ?? d?.externalId ?? String(selectedDatasetIds[0]);
            return slugForFileName(String(label), 36);
          })()
        : "all";
    const rtSlugs = deepResults.map((r) => r.rt).join("_");
    const fileName = `${projectSlug}_${firstDatasetSlug}_${rtSlugs}_deep_analysis.txt`;
    const blob = new Blob([combined], { type: "text/plain" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = fileName;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  if (!sdk) {
    return (
      <section className="ca-section">
        <p className="ca-sign-in-msg">Sign in to CDF to run classic analysis.</p>
      </section>
    );
  }

  return (
    <section className="ca-section">
      <div className="ca-block">
        {/* ---- All Datasets summary ---- */}
        <span className="ca-heading">All Datasets</span>
        <div className="ca-table-wrap">
          <div className="ca-table-box">
            <div className="ca-table-scroll">
              <table className="ca-table">
                <thead>
                  <tr className="ca-thead-row">
                    <th className="ca-th ca-th--check" />
                    <th className="ca-th">Dataset</th>
                    <th className="ca-th ca-th--right ca-th--assets">Assets</th>
                    <th className="ca-th ca-th--right ca-th--timeseries">Timeseries</th>
                    <th className="ca-th ca-th--right ca-th--events">Events</th>
                    <th className="ca-th ca-th--right ca-th--sequences">Sequences</th>
                    <th className="ca-th ca-th--right ca-th--files">Files</th>
                    <th className="ca-th ca-th--right ca-th--transformations">Transformations</th>
                    <th className="ca-th ca-th--right ca-th--functions">Functions</th>
                    <th className="ca-th ca-th--right ca-th--workflows">Workflows</th>
                    <th className="ca-th ca-th--right ca-th--raw">Raw tables</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="ca-tbody-row">
                    <td className="ca-td ca-td--check" />
                    <td className="ca-td ca-td--left">All Datasets</td>
                    <td className="ca-td">
                      {allDatasetsCounts === "loading" ? "…" : allDatasetsCounts ? allDatasetsCounts.assets.toLocaleString() : "—"}
                    </td>
                    <td className="ca-td">
                      {allDatasetsCounts === "loading" ? "…" : allDatasetsCounts ? allDatasetsCounts.timeseries.toLocaleString() : "—"}
                    </td>
                    <td className="ca-td">
                      {allDatasetsCounts === "loading" ? "…" : allDatasetsCounts ? allDatasetsCounts.events.toLocaleString() : "—"}
                    </td>
                    <td className="ca-td">
                      {allDatasetsCounts === "loading" ? "…" : allDatasetsCounts ? allDatasetsCounts.sequences.toLocaleString() : "—"}
                    </td>
                    <td className="ca-td">
                      {allDatasetsCounts === "loading" ? "…" : allDatasetsCounts ? allDatasetsCounts.files.toLocaleString() : "—"}
                    </td>
                    <td className="ca-td">
                      {fmtExtended(allDatasetsCounts, "transformations")}
                    </td>
                    <td className="ca-td">
                      {fmtExtended(allDatasetsCounts, "functions")}
                    </td>
                    <td className="ca-td">
                      {fmtExtended(allDatasetsCounts, "workflows")}
                    </td>
                    <td className="ca-td">
                      {fmtExtended(allDatasetsCounts, "rawTables")}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* ---- Datasets (optional) ---- */}
        <div className="ca-datasets-bar">
          <span className="ca-heading">Datasets (optional)</span>
          <button
            type="button"
            onClick={loadDatasets}
            disabled={loadingDatasets}
            className="ca-btn"
          >
            {loadingDatasets ? "Loading…" : "Load datasets"}
          </button>
          {selectedDatasetIds.length > 0 && (
            <button
              type="button"
              onClick={() => setSelectedDatasetIds([])}
              className="ca-btn ca-btn--small"
            >
              Clear selection ({selectedDatasetIds.length})
            </button>
          )}
          {loadingCounts && <span className="ca-loading-hint">Loading counts…</span>}
          {!datasets.length && !loadingDatasets && (
            <span className="ca-loading-hint">Click Load datasets to list available datasets and their resource counts.</span>
          )}
        </div>
        <div className="ca-table-wrap">
          {datasets.length > 0 && (
            <div className="ca-table-box">
              <div className="ca-table-scroll ca-table-scroll--tall">
                <table className="ca-table">
                  <thead>
                    <tr className="ca-thead-row ca-thead-row--sticky">
                      <th className="ca-th ca-th--check" />
                      <th className="ca-th">Dataset</th>
                      <th className="ca-th ca-th--right ca-th--assets">Assets</th>
                      <th className="ca-th ca-th--right ca-th--timeseries">Timeseries</th>
                      <th className="ca-th ca-th--right ca-th--events">Events</th>
                      <th className="ca-th ca-th--right ca-th--sequences">Sequences</th>
                      <th className="ca-th ca-th--right ca-th--files">Files</th>
                    </tr>
                  </thead>
                  <tbody>
                    {datasets.map((d) => {
                      const counts = datasetCounts[d.id];
                      const selected = selectedDatasetIds.includes(d.id);
                      return (
                        <tr
                          key={d.id}
                          className={selected ? "ca-tbody-row ca-tbody-row--selected" : "ca-tbody-row"}
                        >
                          <td className="ca-td ca-td--check ca-td--check-input">
                            <input
                              type="checkbox"
                              checked={selected}
                              onChange={() => toggleDatasetSelection(d.id)}
                              title="Select to limit analysis to this dataset"
                              className="ca-checkbox"
                            />
                          </td>
                          <td className="ca-td ca-td--left ca-td--name">
                            {d.name ?? d.externalId ?? `ID ${d.id}`}
                          </td>
                          <td className="ca-td">
                            {counts === "loading" ? "…" : counts ? counts.assets.toLocaleString() : "—"}
                          </td>
                          <td className="ca-td">
                            {counts === "loading" ? "…" : counts ? counts.timeseries.toLocaleString() : "—"}
                          </td>
                          <td className="ca-td">
                            {counts === "loading" ? "…" : counts ? counts.events.toLocaleString() : "—"}
                          </td>
                          <td className="ca-td">
                            {counts === "loading" ? "…" : counts ? counts.sequences.toLocaleString() : "—"}
                          </td>
                          <td className="ca-td">
                            {counts === "loading" ? "…" : counts ? counts.files.toLocaleString() : "—"}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
          {datasets.length > 0 && (
            <span className="ca-loading-hint">
              {selectedDatasetIds.length > 0 ? `${selectedDatasetIds.length} dataset(s) selected — analysis limited to these.` : "None selected = all datasets."}
              {datasets.length > COUNT_LOAD_CAP ? ` Counts shown for first ${COUNT_LOAD_CAP} datasets.` : ""}
            </span>
          )}
        </div>

        {/* ---- Analysis controls (grid aligns Run buttons) ---- */}
        <div className="ca-analysis-grid">
          {/* Row 1: Single key analysis */}
          <div className="ca-form-row">
            <label className="ca-label">
              <span className="ca-label-text">Resource type</span>
              <div className="ca-select-wrap">
                <select
                  ref={resourceTypeSelectRef}
                  value={resourceType}
                  onChange={(e) => {
                    const r = e.target.value as ResourceType;
                    setResourceType(r);
                    setFilterKey("");
                    setMetadataKeysList([]);
                    setResult(null);
                  }}
                  className="ca-select"
                >
                  {RESOURCE_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
                <div
                  role="button"
                  tabIndex={-1}
                  onClick={() => resourceTypeSelectRef.current?.click()}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      resourceTypeSelectRef.current?.click();
                    }
                  }}
                  className="ca-dropdown-arrow"
                  aria-hidden
                  title="Open list"
                >
                  <span className="ca-dropdown-arrow-char">▼</span>
                </div>
              </div>
            </label>
            <label className="ca-label">
              <span className="ca-label-text">
                Filter key (metadata or &quot;type&quot;
                {resourceType === "timeseries" && ' , "is step", "is string", "unit"'}
                {resourceType === "files" && ' , "type", "labels", "author", "source"'}
                )
              </span>
              <div className="ca-filter-row">
                <div className={`ca-input-wrap${metadataKeysList.length > 0 ? " ca-input-wrap--has-list" : ""}`}>
                  <input
                    ref={filterKeyInputRef}
                    type="text"
                    list="metadata-keys-datalist"
                    value={filterKey}
                    onChange={(e) => setFilterKey(e.target.value)}
                    onFocus={() => {
                      if (metadataKeysList.length > 0) setFilterKey("");
                    }}
                    placeholder="Select or type a key"
                    className="ca-input"
                  />
                  {metadataKeysList.length > 0 && (
                    <div
                      role="button"
                      tabIndex={-1}
                      onClick={() => filterKeyInputRef.current?.focus()}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault();
                          filterKeyInputRef.current?.focus();
                        }
                      }}
                      className="ca-dropdown-arrow"
                      aria-hidden
                      title="Open list"
                    >
                      <span className="ca-dropdown-arrow-char">▼</span>
                    </div>
                  )}
                </div>
                <datalist id="metadata-keys-datalist">
                  {metadataKeysList.map((item) => (
                    <option key={item.key} value={`${item.key} (${item.count})`} />
                  ))}
                </datalist>
                <button
                  type="button"
                  onClick={loadMetadataKeys}
                  disabled={loadingKeys}
                  className="ca-btn"
                  title="Load metadata keys for this resource type (with counts) to choose from"
                >
                  {loadingKeys ? "Loading…" : "Load metadata keys"}
                </button>
              </div>
            </label>
          </div>
          <div className="ca-grid-btn-cell">
            <button
              type="button"
              onClick={run}
              disabled={loading}
              className="ca-btn ca-btn--primary"
            >
              {loading ? "Running…" : "Run analysis"}
            </button>
          </div>

          {/* Row 2: Deep analysis */}
          <div className="ca-deep-controls">
            <label className="ca-threshold-label">
              <span className="ca-threshold-text">Instance count threshold (%)</span>
              <input
                type="number"
                min={0}
                max={100}
                step={5}
                value={deepCoveragePct}
                onChange={(e) => setDeepCoveragePct(Math.max(0, Math.min(100, Number(e.target.value) || 0)))}
                disabled={!!deepAnalysisProgress}
                className="ca-threshold-input"
              />
            </label>
            <span className="ca-deep-checks" aria-label="Resource types to include">
              {RESOURCE_OPTIONS.map((o) => (
                <label key={o.value} className="ca-deep-check">
                  <input
                    type="checkbox"
                    checked={!!deepAnalysisResourceTypes[o.value]}
                    onChange={() =>
                      setDeepAnalysisResourceTypes((prev) => ({ ...prev, [o.value]: !prev[o.value] }))
                    }
                    disabled={!!deepAnalysisProgress}
                    className="ca-checkbox"
                  />
                  <span className="ca-deep-check-label">{o.label}</span>
                </label>
              ))}
            </span>
            {deepAnalysisProgress && (
              <span className="ca-deep-progress">
                <span className="ca-deep-progress-label">
                  {deepAnalysisProgress.resourceTypeLabel} {Math.round(deepAnalysisProgress.percent)}%
                </span>
                <span
                  className="ca-deep-progress-bar-wrap"
                  role="progressbar"
                  aria-valuenow={deepAnalysisProgress.percent}
                  aria-valuemin={0}
                  aria-valuemax={100}
                  style={{ ["--ca-deep-pct" as string]: `${deepAnalysisProgress.percent}%` }}
                >
                  <span className="ca-deep-progress-bar" />
                </span>
              </span>
            )}
          </div>
          <div className="ca-grid-btn-cell">
            <button
              type="button"
              onClick={runDeepAnalysis}
              disabled={!!deepAnalysisProgress || loadingDatasets || RESOURCE_OPTIONS.every((o) => !deepAnalysisResourceTypes[o.value])}
              className="ca-btn ca-btn--primary"
              title="Run analysis for selected resource types and download one report file per type"
            >
              Run deep analysis
            </button>
          </div>
        </div>
      </div>

      {/* ---- Results actions bar ---- */}
      {(result || deepResults.length > 0) && (
        <div className="ca-results-bar">
          {result && result.rows.length > 0 && (
            <button type="button" onClick={downloadTxt} className="ca-btn ca-btn--secondary">
              Download .txt
            </button>
          )}
          {deepResults.length > 0 && (
            <button type="button" onClick={downloadDeepReport} className="ca-btn ca-btn--secondary">
              Download report
            </button>
          )}
          <button
            type="button"
            onClick={() => { setResult(null); setDeepResults([]); }}
            className="ca-btn ca-btn--small"
          >
            Clear
          </button>
        </div>
      )}

      {/* ---- Results (both single-key and deep) ---- */}
      {result && (
        <div className="ca-result-block">
          {result.error && (
            <p className="ca-result-error">
              {result.error}
              {result.error.toLowerCase().includes("failed to fetch") && (
                <>
                  {" "}
                  If the app is deployed for this project, the CDF project may need to allow your Fusion origin for API access (CORS). Check the browser Network tab for the failing request.
                </>
              )}
            </p>
          )}
          <p className="ca-result-summary">
            {result.rows.length} entries (sorted by count descending)
          </p>
          <div className="ca-result-pre">
            {result.rows.length ? (
              result.rows.map((r, i) =>
                r.filterKeyPart != null ? (
                  <div key={i} className="ca-result-row">
                    <span className="ca-result-key">{r.filterKeyPart}</span>
                    {r.countPart && <span className="ca-result-count">{r.countPart}</span>}
                    <span className="ca-result-meta">{r.metadataKeysPart}</span>
                  </div>
                ) : (
                  <div key={i} className="ca-result-row">{r.text}</div>
                )
              )
            ) : (
              "(no results)"
            )}
          </div>
        </div>
      )}
      {deepResults.length > 0 && (
        <div className="ca-deep-results">
          {deepResults.map((res) => (
            <div key={res.rt} className="ca-result-block">
              <p className="ca-result-summary">
                {res.rtLabel} — {res.count.toLocaleString()} resources, {res.keys.length} metadata keys
              </p>
              <div className="ca-result-pre">{res.report}</div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
