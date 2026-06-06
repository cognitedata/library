import { useCallback, useEffect, useMemo, useState } from "react";
import { normalizeStatus, toTimestamp } from "@/shared/time-utils";
import {
  DEFAULT_PROCESSING_EXECUTION_CAP,
  FUNCTION_LIST_PAGE_SIZE,
  type FunctionRunSummary,
  type FunctionSummary,
  type LoadState,
  type ProcessingDataLoadProgress,
  type ProcessingRequestStats,
} from "./types";
import { useI18n } from "@/shared/i18n";
import { withTransientRetries } from "@/shared/transient-http-retry";
import {
  isStaleProcessingFetch,
  noteForbiddenFailure,
  processingRequestStats,
  useProcessingSeriesFetchLoading,
  useProcessingWindowSessionReset,
} from "./processing-request-stats";

type FunctionCallLogsApiResponse = {
  data?: {
    items?: { message?: string }[];
  };
};

type FunctionsListApiResponse = {
  data?: {
    items?: FunctionSummary[];
    nextCursor?: string | null;
  };
};

type FunctionCallsListApiResponse = {
  data?: {
    items?: FunctionRunSummary[];
    nextCursor?: string | null;
  };
};

type UseFunctionDataArgs = {
  isSdkLoading: boolean;
  sdk: { project: string; post: Function; get: Function };
  windowRange: { start: number; end: number } | null;
  /** When false, diagram data for this series is not fetched (serial diagram loading). */
  fetchEnabled?: boolean;
  /** Changes only when the selected hour window changes. */
  windowSessionKey?: string;
  /** Bumps when this series becomes active in the serial pass. */
  fetchGeneration?: number;
  /** Re-fetch runs only; keep catalog maps (Load all on a capped series). */
  refetchExecutionsOnly?: boolean;
  /** Max function executions to collect; `null` means no limit (Load all). */
  executionLimit?: number | null;
};

export function useFunctionData({
  isSdkLoading,
  sdk,
  windowRange,
  fetchEnabled = true,
  windowSessionKey = "",
  fetchGeneration = 0,
  refetchExecutionsOnly = false,
  executionLimit = DEFAULT_PROCESSING_EXECUTION_CAP,
}: UseFunctionDataArgs) {
  const { t } = useI18n();
  const [status, setStatus] = useState<LoadState>("idle");
  const [executionsTruncated, setExecutionsTruncated] = useState(false);
  const [functionsCatalogMayBeIncomplete, setFunctionsCatalogMayBeIncomplete] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [availabilityMessage, setAvailabilityMessage] = useState<string | null>(null);
  const [runs, setRuns] = useState<FunctionRunSummary[]>([]);
  const [logMap, setLogMap] = useState<Record<string, Record<string, { message?: string }[]>>>({});
  const [functionNameMap, setFunctionNameMap] = useState<Record<string, string>>({});
  const [functionMetaMap, setFunctionMetaMap] = useState<Record<string, FunctionSummary>>({});
  const [loadProgress, setLoadProgress] = useState<ProcessingDataLoadProgress | null>(null);
  const [requestStats, setRequestStats] = useState<ProcessingRequestStats | null>(null);
  const resetForNewWindow = useCallback(() => {
    setStatus("idle");
    setLoadProgress(null);
    setExecutionsTruncated(false);
    setFunctionsCatalogMayBeIncomplete(false);
    setErrorMessage(null);
    setAvailabilityMessage(null);
    setRequestStats(null);
    setRuns([]);
    setLogMap({});
    setFunctionNameMap({});
    setFunctionMetaMap({});
  }, []);

  useProcessingWindowSessionReset(windowSessionKey, resetForNewWindow);
  useProcessingSeriesFetchLoading(
    fetchEnabled,
    isSdkLoading,
    windowRange,
    fetchGeneration,
    setStatus
  );

  const getFailureColor = (run: FunctionRunSummary) => {
    const funcId = run.functionId ?? "";
    const callId = run.id ?? "";
    const entries = logMap[funcId]?.[callId] ?? [];
    if (entries.length === 0) return "#ef4444";
    for (const logEntry of entries) {
      const message = logEntry.message ?? "";
      if (message.includes("out of memory")) return "#f87171";
      if (
        message.includes("Too many concurrent requests") ||
        message.includes("maximum 100 concurrent jobs")
      ) {
        return "#dc2626";
      }
      if (message.includes("Internal server error")) return "#fb7185";
      if (message.includes("upstream request timeout")) return "#e11d48";
    }
    return "#ef4444";
  };

  const getRunDuration = (run: FunctionRunSummary) => {
    const start = toTimestamp(run.startTime ?? run.createdTime);
    const end = toTimestamp(run.endTime ?? run.lastUpdatedTime);
    if (!start) return null;
    if (end && end >= start) return end - start;
    const statusValue = normalizeStatus(run.status);
    if (statusValue.includes("failed") || statusValue.includes("timeout")) {
      return 1;
    }
    return null;
  };

  const getRadius = (run: FunctionRunSummary) => {
    const duration = getRunDuration(run);
    if (duration == null) return 6;
    const minutes = duration / 60000;
    const scaled = 4 + Math.sqrt(minutes) * 6;
    return Math.min(18, Math.max(4, scaled));
  };

  const getColor = (run: FunctionRunSummary) => {
    const statusValue = normalizeStatus(run.status);
    if (statusValue.includes("completed")) return "#16a34a";
    if (statusValue.includes("timeout")) return "#7c3aed";
    if (statusValue.includes("failed")) return getFailureColor(run);
    if (statusValue.includes("running")) return "#2563eb";
    return "#a855f7";
  };

  const fetchRunLogs = async (run: FunctionRunSummary) => {
    if (!run.functionId || !run.id) return;
    try {
      const response = (await withTransientRetries(() =>
        sdk.get(
          `/api/v1/projects/${sdk.project}/functions/${run.functionId}/calls/${run.id}/logs`
        )
      )) as FunctionCallLogsApiResponse;
      const logs = response.data?.items ?? [];
      if (logs.length > 0) {
        setLogMap((prev) => ({
          ...prev,
          [run.functionId ?? "unknown"]: {
            ...(prev[run.functionId ?? "unknown"] ?? {}),
            [run.id ?? "unknown"]: logs,
          },
        }));
      }
    } catch {
      // Ignore log failures.
    }
  };

  useEffect(() => {
    if (!fetchEnabled) {
      setLoadProgress(null);
      return;
    }
    if (isSdkLoading) return;
    if (!windowRange) return;

    const generation = fetchGeneration;
    let cancelled = false;
    const loadRuns = async () => {
      const keepCatalog =
        refetchExecutionsOnly &&
        !functionsCatalogMayBeIncomplete &&
        Object.keys(functionMetaMap).length > 0;
      setExecutionsTruncated(false);
      setErrorMessage(null);
      setAvailabilityMessage(null);
      setRequestStats(null);
      setLogMap({});
      if (!keepCatalog) {
        setFunctionsCatalogMayBeIncomplete(false);
        setRuns([]);
        setFunctionNameMap({});
        setFunctionMetaMap({});
        setLoadProgress({ kind: "functions_list", loaded: 0 });
      } else {
        setLoadProgress({
          kind: "functions_runs",
          current: 0,
          total: Object.keys(functionMetaMap).length,
        });
      }

      try {
        const endWindow = windowRange.end;
        const startWindow = windowRange.start;
        let failedRequests = 0;
        let totalRequests = 0;
        const permissionsDenied = { current: false };

        const listFunctions = async () => {
          const items: FunctionSummary[] = [];
          let cursor: string | undefined;
          let listPages = 0;
          let lastPageCount = 0;
          do {
            totalRequests++;
            listPages += 1;
            try {
              const response = (await withTransientRetries(() =>
                sdk.post(`/api/v1/projects/${sdk.project}/functions/list`, {
                  data: JSON.stringify({ limit: FUNCTION_LIST_PAGE_SIZE, cursor }),
                })
              )) as FunctionsListApiResponse;
              const pageItems = response.data?.items ?? [];
              lastPageCount = pageItems.length;
              items.push(...pageItems);
              cursor = response.data?.nextCursor ?? undefined;
            } catch (e) {
              failedRequests++;
              noteForbiddenFailure(permissionsDenied, e);
              throw e;
            }
            if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
              setLoadProgress({
                kind: "functions_list",
                loaded: items.length,
                pages: listPages,
                pageSize: FUNCTION_LIST_PAGE_SIZE,
              });
            }
          } while (cursor);
          return {
            items,
            listPages,
            mayBeIncomplete:
              listPages === 1 &&
              lastPageCount === FUNCTION_LIST_PAGE_SIZE &&
              items.length === FUNCTION_LIST_PAGE_SIZE,
          };
        };

        const listRunsForFunction = async (functionId: string, cursor?: string) => {
          const requestBody = {
            filter: {
              startTime: {
                min: startWindow,
                max: endWindow,
              },
            },
            limit: 10000,
            cursor,
          };
          totalRequests++;
          const response = (await withTransientRetries(() =>
            sdk.post(
              `/api/v1/projects/${sdk.project}/functions/${functionId}/calls/list`,
              {
                data: JSON.stringify(requestBody),
              }
            )
          )) as FunctionCallsListApiResponse;
          return {
            items: response.data?.items ?? [],
            nextCursor: response.data?.nextCursor ?? null,
          };
        };

        let functions: FunctionSummary[];
        if (keepCatalog) {
          functions = Object.values(functionMetaMap);
        } else {
          const functionCatalog = await listFunctions();
          functions = functionCatalog.items;
          if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
            setFunctionsCatalogMayBeIncomplete(functionCatalog.mayBeIncomplete);
            const nameMap: Record<string, string> = {};
            const metaMap: Record<string, FunctionSummary> = {};
            for (const fn of functions) {
              nameMap[fn.id] = fn.name ?? t("processing.function.defaultName", { id: fn.id });
              metaMap[fn.id] = fn;
            }
            setFunctionNameMap(nameMap);
            setFunctionMetaMap(metaMap);
          }
        }

        const totalFns = functions.length;
        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setLoadProgress({ kind: "functions_runs", current: 0, total: totalFns });
        }

        const collected: FunctionRunSummary[] = [];
        let fnIndex = 0;
        const cap = executionLimit;
        let hitExecutionCap = false;
        outer: for (const fn of functions) {
          let cursor: string | undefined;
          try {
            do {
              const response = await listRunsForFunction(fn.id, cursor);
              const items = response.items ?? [];
              for (const run of items) {
                const start = toTimestamp(run.startTime ?? run.createdTime);
                if (start && start >= startWindow) {
                  if (cap != null && collected.length >= cap) {
                    hitExecutionCap = true;
                    break outer;
                  }
                  if (!run.endTime && normalizeStatus(run.status).includes("failed")) {
                    run.endTime = (run.startTime ?? start) + 1;
                  }
                  collected.push(run);
                }
              }
              cursor = response.nextCursor ?? undefined;
            } while (cursor);
          } catch (e) {
            failedRequests++;
            noteForbiddenFailure(permissionsDenied, e);
          }
          fnIndex += 1;
          if (
            !cancelled &&
            !isStaleProcessingFetch(fetchGeneration, generation) &&
            (fnIndex % 3 === 0 || fnIndex === totalFns)
          ) {
            setLoadProgress({ kind: "functions_runs", current: fnIndex, total: totalFns });
          }
          if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
            setRuns([...collected]);
          }
        }

        for (const run of collected) {
          if (cancelled || isStaleProcessingFetch(fetchGeneration, generation)) return;
          const statusValue = normalizeStatus(run.status);
          if (!statusValue.includes("failed") && !statusValue.includes("timeout")) continue;
          if (!run.functionId || !run.id) continue;
          await fetchRunLogs(run);
        }

        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setRuns(collected);
          setExecutionsTruncated(hitExecutionCap);
          setLoadProgress(null);
          setRequestStats(
            processingRequestStats(failedRequests, totalRequests, permissionsDenied.current)
          );
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setLoadProgress(null);
          setRequestStats(null);
          const message = error instanceof Error ? error.message : t("processing.error.runs");
          setErrorMessage(message);
          if (message.toLowerCase().includes("404")) {
            setAvailabilityMessage(t("processing.unavailable.functions"));
            setStatus("success");
          } else {
            setStatus("error");
          }
        }
      }
    };

    loadRuns();
    return () => {
      cancelled = true;
    };
  }, [
    executionLimit,
    fetchEnabled,
    fetchGeneration,
    isSdkLoading,
    refetchExecutionsOnly,
    sdk,
    windowRange?.start,
    windowRange?.end,
    t,
  ]);

  const failureDurationMs = useMemo(() => {
    const failureStatuses = ["failed", "failure", "timeout", "timed_out"];
    return runs.reduce((total, run) => {
      const statusValue = run.status?.toLowerCase() ?? "";
      if (!failureStatuses.some((value) => statusValue.includes(value))) return total;
      const start = toTimestamp(run.startTime ?? run.createdTime);
      const end = toTimestamp(run.endTime ?? run.lastUpdatedTime);
      if (!start || !end || end <= start) return total;
      return total + (end - start);
    }, 0);
  }, [runs]);

  return {
    status,
    executionsTruncated,
    functionsCatalogMayBeIncomplete,
    functionsCatalogCount:
      status === "success" || status === "loading" ? Object.keys(functionNameMap).length : 0,
    loadProgress,
    requestStats,
    errorMessage,
    availabilityMessage,
    runs,
    functionNameMap,
    functionMetaMap,
    logMap,
    failureDurationMs,
    fetchRunLogs,
    getRunDuration,
    getRadius,
    getColor,
    setRuns,
    setLogMap,
    setFunctionNameMap,
    setFunctionMetaMap,
    setStatus,
    setErrorMessage,
    setAvailabilityMessage,
  };
}
