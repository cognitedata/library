import { useEffect, useMemo, useState } from "react";
import { normalizeStatus, toTimestamp } from "@/shared/time-utils";
import type { FunctionRunSummary, FunctionSummary, LoadState } from "./types";
import { useI18n } from "@/shared/i18n";

type UseFunctionDataArgs = {
  isSdkLoading: boolean;
  sdk: { project: string; post: Function; get: Function };
  windowRange: { start: number; end: number } | null;
};

export function useFunctionData({ isSdkLoading, sdk, windowRange }: UseFunctionDataArgs) {
  const { t } = useI18n();
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [availabilityMessage, setAvailabilityMessage] = useState<string | null>(null);
  const [runs, setRuns] = useState<FunctionRunSummary[]>([]);
  const [logMap, setLogMap] = useState<Record<string, Record<string, { message?: string }[]>>>({});
  const [functionNameMap, setFunctionNameMap] = useState<Record<string, string>>({});
  const [functionMetaMap, setFunctionMetaMap] = useState<Record<string, FunctionSummary>>({});

  const getFailureColor = (run: FunctionRunSummary) => {
    const funcId = run.functionId ?? "";
    const callId = run.id ?? "";
    const entries = logMap[funcId]?.[callId] ?? [];
    if (entries.length === 0) return "#ff0000";
    for (const logEntry of entries) {
      const message = logEntry.message ?? "";
      if (message.includes("out of memory")) return "#EDB95E";
      if (
        message.includes("Too many concurrent requests") ||
        message.includes("maximum 100 concurrent jobs")
      ) {
        return "#0045ff";
      }
      if (message.includes("Internal server error")) return "#f0ff00";
      if (message.includes("upstream request timeout")) return "#e300ff";
    }
    return "orange";
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
      const response = await sdk.get<{
        items?: { message?: string }[];
      }>(`/api/v1/projects/${sdk.project}/functions/${run.functionId}/calls/${run.id}/logs`);
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
    if (isSdkLoading) return;
    if (!windowRange) return;

    let cancelled = false;
    const loadRuns = async () => {
      setStatus("loading");
      setErrorMessage(null);
      setAvailabilityMessage(null);
      setRuns([]);
      setLogMap({});
      setFunctionNameMap({});
      setFunctionMetaMap({});

      try {
        const endWindow = windowRange.end;
        const startWindow = windowRange.start;

        const listFunctions = async () => {
          const items: FunctionSummary[] = [];
          let cursor: string | undefined;
          do {
            const response = await sdk.post<{
              items?: FunctionSummary[];
              nextCursor?: string | null;
            }>(`/api/v1/projects/${sdk.project}/functions/list`, {
              data: JSON.stringify({ limit: 100, cursor }),
            });
            items.push(...(response.data?.items ?? []));
            cursor = response.data?.nextCursor ?? undefined;
          } while (cursor);
          return items;
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
          const response = await sdk.post<{
            items?: FunctionRunSummary[];
            nextCursor?: string | null;
          }>(`/api/v1/projects/${sdk.project}/functions/${functionId}/calls/list`, {
            data: JSON.stringify(requestBody),
          });
          return {
            items: response.data?.items ?? [],
            nextCursor: response.data?.nextCursor ?? null,
          };
        };

        const functions = await listFunctions();
        if (!cancelled) {
          const nameMap: Record<string, string> = {};
          const metaMap: Record<string, FunctionSummary> = {};
          for (const fn of functions) {
            nameMap[fn.id] = fn.name ?? t("processing.function.defaultName", { id: fn.id });
            metaMap[fn.id] = fn;
          }
          setFunctionNameMap(nameMap);
          setFunctionMetaMap(metaMap);
        }

        const collected: FunctionRunSummary[] = [];
        for (const fn of functions) {
          let cursor: string | undefined;
          do {
            const response = await listRunsForFunction(fn.id, cursor);
            const items = response.items ?? [];
            for (const run of items) {
              const start = toTimestamp(run.startTime ?? run.createdTime);
              if (start && start >= startWindow) {
                if (!run.endTime && normalizeStatus(run.status).includes("failed")) {
                  run.endTime = (run.startTime ?? start) + 1;
                }
                collected.push(run);
              }
            }
            cursor = response.nextCursor ?? undefined;
          } while (cursor);
          if (!cancelled) {
            setRuns([...collected]);
          }
        }

        if (!cancelled) {
          setRuns(collected);
          setStatus("success");
        }

        for (const run of collected) {
          if (cancelled) return;
          const statusValue = normalizeStatus(run.status);
          if (!statusValue.includes("failed") && !statusValue.includes("timeout")) continue;
          if (!run.functionId || !run.id) continue;
          await fetchRunLogs(run);
        }
      } catch (error) {
        if (!cancelled) {
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
  }, [isSdkLoading, sdk, windowRange, t]);

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
