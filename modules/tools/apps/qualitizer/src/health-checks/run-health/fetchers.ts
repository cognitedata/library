import type { CogniteClient } from "@cognite/sdk";
import { toTimestampLoose } from "@/shared/time-utils";
import {
  CDF_BROWSER_URL,
  CDF_CLUSTER,
  getFunctionsPageUrl,
  getTransformationRunHistoryUrl,
  getWorkflowEditorUrl,
} from "@/shared/cdf-browser-url";
import {
  FAILED_STATUSES,
  MAX_RECENT_RUNS,
  SUCCESS_STATUSES,
  classifyHealth,
  isFailed,
  isSuccess,
  normalize,
  uptimePercentage,
} from "./uptime";
import type { ResourceHealth, ResourceReport, RunEntry } from "./types";

const API_LIST_LIMIT = 500;
const API_RUNS_LIMIT = 100;

type FetcherOpts = {
  sdk: CogniteClient;
  datasetId: number | null;
  startMs: number;
  endMs: number;
  thresholdPct: number;
  signal?: { cancelled: boolean };
};

function getExtractionPipelineUrl(project: string, externalId: string): string {
  return `${CDF_BROWSER_URL}/${project}/extpipes/extpipe/${encodeURIComponent(
    externalId
  )}?cluster=${CDF_CLUSTER}&workspace=data-management`;
}

function matchesDataset(dataSetId: number | undefined | null, filterId: number | null): boolean {
  if (filterId == null) return true;
  return dataSetId === filterId;
}

function sortRecentRuns(runs: RunEntry[]): RunEntry[] {
  const failedFirst = [...runs].sort((a, b) => {
    const aFailed = isFailed(a.status) ? 0 : 1;
    const bFailed = isFailed(b.status) ? 0 : 1;
    if (aFailed !== bFailed) return aFailed - bFailed;
    return (b.timeMs ?? 0) - (a.timeMs ?? 0);
  });
  return failedFirst.slice(0, MAX_RECENT_RUNS);
}

function finalizeResource(
  base: Omit<ResourceHealth, "runsInWindow" | "successful" | "failed" | "uptimePercentage" | "recentRuns" | "lastStatus" | "lastRunMs">,
  runs: RunEntry[]
): ResourceHealth {
  const inWindow = runs;
  const successful = inWindow.filter((r) => isSuccess(r.status)).length;
  const failed = inWindow.filter((r) => isFailed(r.status)).length;
  const last = [...inWindow].sort((a, b) => (b.timeMs ?? 0) - (a.timeMs ?? 0))[0];
  return {
    ...base,
    runsInWindow: inWindow.length,
    successful,
    failed,
    uptimePercentage: uptimePercentage(successful, failed),
    recentRuns: sortRecentRuns(inWindow),
    lastStatus: last?.status,
    lastRunMs: last?.timeMs,
  };
}

function sortByFailureFirst(resources: ResourceHealth[]): ResourceHealth[] {
  return [...resources].sort((a, b) => {
    const aFailed = isFailed(a.lastStatus) ? 0 : 1;
    const bFailed = isFailed(b.lastStatus) ? 0 : 1;
    if (aFailed !== bFailed) return aFailed - bFailed;
    return a.name.localeCompare(b.name);
  });
}

function buildReport(
  kindLabel: ResourceReport["kindLabel"],
  resources: ResourceHealth[],
  thresholdPct: number,
  extraErrors: ResourceReport["errors"]
): ResourceReport {
  const sorted = sortByFailureFirst(resources);
  let healthy = 0;
  let unhealthy = 0;
  let noRuns = 0;
  let totalSuccess = 0;
  let totalFailed = 0;
  const errors = [...extraErrors];
  for (const r of sorted) {
    const cls = classifyHealth(r.runsInWindow, r.uptimePercentage, thresholdPct);
    if (cls === "healthy") healthy += 1;
    else if (cls === "unhealthy") unhealthy += 1;
    else noRuns += 1;
    totalSuccess += r.successful;
    totalFailed += r.failed;
    for (const run of r.recentRuns) {
      if (isFailed(run.status)) {
        errors.push({
          resource: `${kindLabel}: ${r.name}`,
          status: run.status,
          timeMs: run.timeMs,
          message: run.message,
        });
      }
    }
  }
  const aggregateUptime = uptimePercentage(totalSuccess, totalFailed);
  return {
    kindLabel,
    resources: sorted,
    summary: { total: sorted.length, healthy, unhealthy, noRuns, aggregateUptime },
    errors,
    error: null,
  };
}

function errorReport(kindLabel: ResourceReport["kindLabel"], message: string): ResourceReport {
  return {
    kindLabel,
    resources: [],
    summary: { total: 0, healthy: 0, unhealthy: 0, noRuns: 0, aggregateUptime: 100 },
    errors: [{ resource: kindLabel, status: "error", message }],
    error: message,
  };
}

/* ---------- Extraction pipelines ---------- */

type ExtPipeSummary = {
  id: number;
  externalId: string;
  name?: string;
  description?: string;
  dataSetId?: number;
};

type ExtPipeRun = {
  id?: number;
  status?: string;
  message?: string;
  createdTime?: number;
};

export async function fetchExtractionPipelineHealth(opts: FetcherOpts): Promise<ResourceReport> {
  const { sdk, datasetId, startMs, endMs, thresholdPct, signal } = opts;
  try {
    const configs: ExtPipeSummary[] = [];
    let cursor: string | undefined;
    do {
      const response = await sdk.get<{
        items?: ExtPipeSummary[];
        nextCursor?: string | null;
      }>(`/api/v1/projects/${sdk.project}/extpipes`, {
        params: { limit: "100", cursor },
      });
      configs.push(...(response.data?.items ?? []));
      cursor = response.data?.nextCursor ?? undefined;
    } while (cursor);

    const filtered = configs.filter((c) => matchesDataset(c.dataSetId, datasetId));
    const resources: ResourceHealth[] = [];

    for (const cfg of filtered) {
      if (signal?.cancelled) break;
      try {
        const response = await sdk.post<{
          items?: ExtPipeRun[];
        }>(`/api/v1/projects/${sdk.project}/extpipes/runs/list`, {
          data: {
            limit: API_RUNS_LIMIT,
            filter: {
              externalId: cfg.externalId,
              createdTime: { min: startMs, max: endMs },
            },
          },
        });
        const runs: RunEntry[] = (response.data?.items ?? [])
          .map<RunEntry>((r) => ({
            status: r.status ?? "",
            timeMs: toTimestampLoose(r.createdTime),
            message: r.message,
          }))
          .filter((r) => (r.timeMs ?? 0) >= startMs && (r.timeMs ?? 0) <= endMs);
        resources.push(
          finalizeResource(
            {
              id: String(cfg.id),
              name: cfg.name ?? cfg.externalId,
              externalId: cfg.externalId,
              datasetId: cfg.dataSetId,
              fusionUrl: getExtractionPipelineUrl(sdk.project, cfg.externalId),
            },
            runs
          )
        );
      } catch (err) {
        resources.push(
          finalizeResource(
            {
              id: String(cfg.id),
              name: cfg.name ?? cfg.externalId,
              externalId: cfg.externalId,
              datasetId: cfg.dataSetId,
              extraLabel: err instanceof Error ? err.message : "Runs fetch failed",
            },
            []
          )
        );
      }
    }

    return buildReport("Extraction pipeline", resources, thresholdPct, []);
  } catch (err) {
    return errorReport(
      "Extraction pipeline",
      err instanceof Error ? err.message : "Failed to load extraction pipelines"
    );
  }
}

/* ---------- Workflows ---------- */

type WorkflowListItem = {
  externalId: string;
  description?: string;
  dataSetId?: number;
};

type WorkflowExec = {
  id?: string;
  workflowExternalId?: string;
  status?: string;
  startTime?: number;
  endTime?: number;
  createdTime?: number;
  reasonForIncompletion?: string;
};

export async function fetchWorkflowHealth(opts: FetcherOpts): Promise<ResourceReport> {
  const { sdk, datasetId, startMs, endMs, thresholdPct, signal } = opts;
  try {
    const workflows: WorkflowListItem[] = [];
    let cursor: string | undefined;
    do {
      const response = await sdk.get<{
        items?: WorkflowListItem[];
        nextCursor?: string | null;
      }>(`/api/v1/projects/${sdk.project}/workflows`, {
        params: { limit: "1000", cursor },
      });
      workflows.push(...(response.data?.items ?? []));
      cursor = response.data?.nextCursor ?? undefined;
    } while (cursor);

    const filtered = workflows.filter((w) => matchesDataset(w.dataSetId, datasetId));

    const executions: WorkflowExec[] = [];
    let execCursor: string | undefined;
    do {
      if (signal?.cancelled) break;
      const response = await sdk.post<{
        items?: WorkflowExec[];
        nextCursor?: string | null;
      }>(`/api/v1/projects/${sdk.project}/workflows/executions/list`, {
        data: {
          limit: 1000,
          cursor: execCursor,
          filter: { createdTimeStart: startMs, createdTimeEnd: endMs },
        },
      });
      executions.push(...(response.data?.items ?? []));
      execCursor = response.data?.nextCursor ?? undefined;
    } while (execCursor);

    const execsByWorkflow = new Map<string, RunEntry[]>();
    for (const e of executions) {
      if (!e.workflowExternalId) continue;
      const list = execsByWorkflow.get(e.workflowExternalId) ?? [];
      list.push({
        status: e.status ?? "",
        timeMs: toTimestampLoose(e.endTime ?? e.startTime ?? e.createdTime),
        message: e.reasonForIncompletion,
      });
      execsByWorkflow.set(e.workflowExternalId, list);
    }

    const resources: ResourceHealth[] = filtered.map((w) =>
      finalizeResource(
        {
          id: w.externalId,
          name: w.externalId,
          externalId: w.externalId,
          datasetId: w.dataSetId,
          fusionUrl: getWorkflowEditorUrl(sdk.project, w.externalId),
        },
        execsByWorkflow.get(w.externalId) ?? []
      )
    );

    return buildReport("Workflow", resources, thresholdPct, []);
  } catch (err) {
    return errorReport(
      "Workflow",
      err instanceof Error ? err.message : "Failed to load workflows"
    );
  }
}

/* ---------- Transformations ---------- */

type TransformationListItem = {
  id: number;
  externalId?: string;
  name?: string;
  dataSetId?: number;
};

type TransformationJob = {
  id?: number;
  transformationId?: number;
  status?: string;
  error?: string;
  startedTime?: number;
  finishedTime?: number;
  createdTime?: number;
};

export async function fetchTransformationHealth(opts: FetcherOpts): Promise<ResourceReport> {
  const { sdk, datasetId, startMs, endMs, thresholdPct, signal } = opts;
  try {
    const transformations: TransformationListItem[] = [];
    let cursor: string | undefined;
    do {
      const response = await sdk.get<{
        items?: TransformationListItem[];
        nextCursor?: string | null;
      }>(`/api/v1/projects/${sdk.project}/transformations`, {
        params: { limit: "1000", cursor },
      });
      transformations.push(...(response.data?.items ?? []));
      cursor = response.data?.nextCursor ?? undefined;
    } while (cursor);

    const filtered = transformations.filter((t) => matchesDataset(t.dataSetId, datasetId));

    const resources: ResourceHealth[] = [];
    for (const t of filtered) {
      if (signal?.cancelled) break;
      try {
        const response = await sdk.get<{ items?: TransformationJob[] }>(
          `/api/v1/projects/${sdk.project}/transformations/jobs`,
          { params: { limit: String(API_RUNS_LIMIT), transformationId: String(t.id) } }
        );
        const runs: RunEntry[] = (response.data?.items ?? [])
          .map<RunEntry>((j) => ({
            status: j.status ?? "",
            timeMs: toTimestampLoose(j.finishedTime ?? j.startedTime ?? j.createdTime),
            message: j.error,
          }))
          .filter((r) => (r.timeMs ?? 0) >= startMs && (r.timeMs ?? 0) <= endMs);
        resources.push(
          finalizeResource(
            {
              id: String(t.id),
              name: t.name ?? t.externalId ?? String(t.id),
              externalId: t.externalId,
              datasetId: t.dataSetId,
              fusionUrl: getTransformationRunHistoryUrl(sdk.project, t.id),
            },
            runs
          )
        );
      } catch (err) {
        resources.push(
          finalizeResource(
            {
              id: String(t.id),
              name: t.name ?? t.externalId ?? String(t.id),
              externalId: t.externalId,
              datasetId: t.dataSetId,
              extraLabel: err instanceof Error ? err.message : "Jobs fetch failed",
            },
            []
          )
        );
      }
    }

    return buildReport("Transformation", resources, thresholdPct, []);
  } catch (err) {
    return errorReport(
      "Transformation",
      err instanceof Error ? err.message : "Failed to load transformations"
    );
  }
}

/* ---------- Functions ---------- */

type FunctionListItem = {
  id: string | number;
  externalId?: string;
  name?: string;
  description?: string;
  status?: string;
  fileId?: number;
};

type FunctionCall = {
  id?: string | number;
  status?: string;
  error?: { message?: string } | string;
  startTime?: number;
  endTime?: number;
  createdTime?: number;
};

export async function fetchFunctionHealth(opts: FetcherOpts): Promise<ResourceReport> {
  const { sdk, datasetId, startMs, endMs, thresholdPct, signal } = opts;
  try {
    const functions: FunctionListItem[] = [];
    let cursor: string | undefined;
    do {
      const response = await sdk.post<{
        items?: FunctionListItem[];
        nextCursor?: string | null;
      }>(`/api/v1/projects/${sdk.project}/functions/list`, {
        data: JSON.stringify({ limit: 100, cursor }),
      });
      functions.push(...(response.data?.items ?? []));
      cursor = response.data?.nextCursor ?? undefined;
    } while (cursor);

    let dataSetByFileId = new Map<number, number>();
    if (datasetId != null) {
      const fileIds = Array.from(
        new Set(functions.map((f) => f.fileId).filter((id): id is number => typeof id === "number"))
      );
      if (fileIds.length > 0) {
        try {
          const response = await sdk.post<{
            items?: Array<{ id: number; dataSetId?: number }>;
          }>(`/api/v1/projects/${sdk.project}/files/byids`, {
            data: JSON.stringify({
              items: fileIds.map((id) => ({ id })),
              ignoreUnknownIds: true,
            }),
          });
          for (const file of response.data?.items ?? []) {
            if (file.dataSetId != null) dataSetByFileId.set(file.id, file.dataSetId);
          }
        } catch {
          dataSetByFileId = new Map();
        }
      }
    }

    const filtered = functions.filter((f) => {
      if (datasetId == null) return true;
      if (typeof f.fileId !== "number") return false;
      return dataSetByFileId.get(f.fileId) === datasetId;
    });

    const extraErrors: ResourceReport["errors"] = [];
    const resources: ResourceHealth[] = [];
    for (const fn of filtered) {
      if (signal?.cancelled) break;
      if (normalize(fn.status) === "failed") {
        extraErrors.push({
          resource: `Function: ${fn.name ?? fn.externalId ?? fn.id}`,
          status: fn.status ?? "failed",
          message: "Function deployment failed",
        });
      }
      try {
        const response = await sdk.post<{ items?: FunctionCall[] }>(
          `/api/v1/projects/${sdk.project}/functions/${fn.id}/calls/list`,
          {
            data: JSON.stringify({
              filter: { startTime: { min: startMs, max: endMs } },
              limit: API_RUNS_LIMIT,
            }),
          }
        );
        const runs: RunEntry[] = (response.data?.items ?? []).map<RunEntry>((c) => {
          const msg =
            typeof c.error === "string"
              ? c.error
              : typeof c.error === "object" && c.error
                ? c.error.message
                : undefined;
          return {
            status: c.status ?? "",
            timeMs: toTimestampLoose(c.endTime ?? c.startTime ?? c.createdTime),
            message: msg,
          };
        });
        resources.push(
          finalizeResource(
            {
              id: String(fn.id),
              name: fn.name ?? fn.externalId ?? String(fn.id),
              externalId: fn.externalId,
              fusionUrl: getFunctionsPageUrl(sdk.project),
              extraLabel: fn.status,
            },
            runs
          )
        );
      } catch (err) {
        resources.push(
          finalizeResource(
            {
              id: String(fn.id),
              name: fn.name ?? fn.externalId ?? String(fn.id),
              externalId: fn.externalId,
              extraLabel: err instanceof Error ? err.message : "Calls fetch failed",
            },
            []
          )
        );
      }
    }

    return buildReport("Function", resources, thresholdPct, extraErrors);
  } catch (err) {
    return errorReport(
      "Function",
      err instanceof Error ? err.message : "Failed to load functions"
    );
  }
}

export const __internal_testing = {
  SUCCESS_STATUSES,
  FAILED_STATUSES,
  API_LIST_LIMIT,
  API_RUNS_LIMIT,
};
