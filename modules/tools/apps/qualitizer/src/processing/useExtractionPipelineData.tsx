import { useCallback, useEffect, useMemo, useState } from "react";
import { normalizeStatus, toTimestampLoose } from "@/shared/time-utils";
import {
  DEFAULT_PROCESSING_EXECUTION_CAP,
  type ExtPipeConfigSummary,
  type ExtPipeRunSummary,
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

type ExtPipesListApiResponse = {
  data?: {
    items?: ExtPipeConfigSummary[];
    nextCursor?: string | null;
  };
};

type ExtPipeRunsListApiResponse = {
  data?: {
    items?: Array<ExtPipeRunSummary & { createdTime?: unknown }>;
    nextCursor?: string | null;
  };
};

type UseExtractionPipelineDataArgs = {
  isSdkLoading: boolean;
  sdk: { project: string; get: Function; post: Function };
  windowRange: { start: number; end: number } | null;
  fetchEnabled?: boolean;
  windowSessionKey?: string;
  fetchGeneration?: number;
  refetchExecutionsOnly?: boolean;
  executionLimit?: number | null;
};

export function useExtractionPipelineData({
  isSdkLoading,
  sdk,
  windowRange,
  fetchEnabled = true,
  windowSessionKey = "",
  fetchGeneration = 0,
  refetchExecutionsOnly = false,
  executionLimit = DEFAULT_PROCESSING_EXECUTION_CAP,
}: UseExtractionPipelineDataArgs) {
  const { t } = useI18n();
  const [extractorsStatus, setExtractorsStatus] = useState<LoadState>("idle");
  const [executionsTruncated, setExecutionsTruncated] = useState(false);
  const [extractorsError, setExtractorsError] = useState<string | null>(null);
  const [extractorConfigs, setExtractorConfigs] = useState<ExtPipeConfigSummary[]>([]);
  const [extractorRunsAll, setExtractorRunsAll] = useState<
    Array<ExtPipeRunSummary & { externalId: string }>
  >([]);
  const [loadProgress, setLoadProgress] = useState<ProcessingDataLoadProgress | null>(null);
  const [requestStats, setRequestStats] = useState<ProcessingRequestStats | null>(null);

  const resetForNewWindow = useCallback(() => {
    setExtractorsStatus("idle");
    setLoadProgress(null);
    setExecutionsTruncated(false);
    setExtractorsError(null);
    setRequestStats(null);
    setExtractorConfigs([]);
    setExtractorRunsAll([]);
  }, []);

  useProcessingWindowSessionReset(windowSessionKey, resetForNewWindow);
  useProcessingSeriesFetchLoading(
    fetchEnabled,
    isSdkLoading,
    windowRange,
    fetchGeneration,
    setExtractorsStatus
  );

  useEffect(() => {
    if (!fetchEnabled) {
      setLoadProgress(null);
      return;
    }
    if (isSdkLoading) return;
    if (refetchExecutionsOnly && extractorConfigs.length > 0) return;

    const generation = fetchGeneration;
    let cancelled = false;
    const loadExtractorConfigs = async () => {
      setExtractorsError(null);
      setRequestStats(null);
      setExtractorConfigs([]);
      setExtractorRunsAll([]);
      setLoadProgress({ kind: "extractors_list", loaded: 0 });
      try {
        const configs: ExtPipeConfigSummary[] = [];
        let cursor: string | undefined;
        do {
          const response = (await withTransientRetries(() =>
            sdk.get(`/api/v1/projects/${sdk.project}/extpipes`, {
              params: { limit: "100", cursor },
            })
          )) as ExtPipesListApiResponse;
          configs.push(...(response.data?.items ?? []));
          cursor = response.data?.nextCursor ?? undefined;
          if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
            setLoadProgress({ kind: "extractors_list", loaded: configs.length });
          }
        } while (cursor);

        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setExtractorConfigs(configs);
          setLoadProgress(null);
          if (configs.length === 0) {
            setExtractorRunsAll([]);
            setExtractorsStatus("success");
          }
        }
      } catch (error) {
        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setLoadProgress(null);
          setRequestStats(null);
          setExtractorsError(
            error instanceof Error ? error.message : t("processing.error.extractors")
          );
          setExtractorsStatus("error");
        }
      }
    };

    loadExtractorConfigs();
    return () => {
      cancelled = true;
    };
  }, [
    fetchEnabled,
    fetchGeneration,
    isSdkLoading,
    refetchExecutionsOnly,
    sdk,
    t,
    windowRange?.start,
    windowRange?.end,
  ]);

  useEffect(() => {
    if (!fetchEnabled) {
      setLoadProgress(null);
      return;
    }
    if (isSdkLoading || !windowRange) return;
    if (extractorConfigs.length === 0) {
      if (!refetchExecutionsOnly) setExtractorRunsAll([]);
      return;
    }

    const generation = fetchGeneration;
    let cancelled = false;
    const loadExtractorRuns = async () => {
      setExecutionsTruncated(false);
      setExtractorsError(null);
      setRequestStats(null);
      if (!refetchExecutionsOnly) {
        setExtractorRunsAll([]);
      }
      const totalPipelines = extractorConfigs.length;
      if (totalPipelines > 0) {
        setLoadProgress({ kind: "extractors_runs", current: 0, total: totalPipelines });
      }
      try {
        const runs: Array<ExtPipeRunSummary & { externalId: string }> = [];
        let pipelineIndex = 0;
        let failedRequests = 0;
        let totalRequests = 0;
        const permissionsDenied = { current: false };
        const cap = executionLimit;
        let hitExecutionCap = false;
        const pushRun = (run: ExtPipeRunSummary & { externalId: string }) => {
          if (cap != null && runs.length >= cap) {
            hitExecutionCap = true;
            return false;
          }
          runs.push(run);
          return true;
        };
        pipelineLoop: for (const config of extractorConfigs) {
          if (hitExecutionCap) break pipelineLoop;
          let cursor: string | undefined;
          const seenTimes: number[] = [];
          const startMessages: Array<ExtPipeRunSummary & { createdTime: number }> = [];
          const stopMessages: Array<ExtPipeRunSummary & { createdTime: number }> = [];
          const otherEvents: Array<ExtPipeRunSummary & { createdTime: number }> = [];
          do {
            totalRequests++;
            let response: ExtPipeRunsListApiResponse;
            try {
              response = (await withTransientRetries(() =>
                sdk.post(`/api/v1/projects/${sdk.project}/extpipes/runs/list`, {
                  data: {
                    limit: 100,
                    cursor,
                    filter: {
                      externalId: config.externalId,
                      createdTime: {
                        min: windowRange.start,
                        max: windowRange.end,
                      },
                    },
                  },
                })
              )) as ExtPipeRunsListApiResponse;
            } catch (e) {
              failedRequests++;
              noteForbiddenFailure(permissionsDenied, e);
              break;
            }
            for (const item of response.data?.items ?? []) {
              const createdTime = toTimestampLoose(item.createdTime);
              if (createdTime == null) continue;
              const status = item.status?.toLowerCase?.() ?? item.status ?? "";
              const message = (item.message ?? "").toLowerCase();
              if (status === "seen") {
                seenTimes.push(createdTime);
                continue;
              }
              if (status === "success" || status === "failure") {
                if (message.includes("extractor started")) {
                  startMessages.push({ ...item, createdTime });
                  continue;
                }
                if (message.includes("successful shutdown")) {
                  stopMessages.push({ ...item, createdTime });
                  continue;
                }
              }
              otherEvents.push({ ...item, createdTime });
            }
            cursor = response.data?.nextCursor ?? undefined;
          } while (cursor);

          const sortedSeen = [...seenTimes].sort((a, b) => a - b);
          const sortedStarts = [...startMessages].sort((a, b) => a.createdTime - b.createdTime);
          const sortedStops = [...stopMessages].sort((a, b) => a.createdTime - b.createdTime);
          const startCandidates = [
            ...(sortedSeen.length > 0 ? [sortedSeen[0]] : []),
            ...(sortedStarts.length > 0 ? [sortedStarts[0].createdTime] : []),
          ].sort((a, b) => a - b);
          const startTime = startCandidates[0];
          const lastSeen = sortedSeen.length > 0 ? sortedSeen[sortedSeen.length - 1] : undefined;
          const stopEvent = startTime
            ? sortedStops.find((event) => event.createdTime >= startTime)
            : undefined;

          if (startTime != null) {
            const endTime = stopEvent?.createdTime ?? lastSeen ?? startTime;
            if (
              !pushRun({
                id: stopEvent?.id ?? Math.floor(startTime),
                status: stopEvent?.status ?? "seen",
                message:
                  stopEvent?.message ??
                  (sortedSeen.length > 0
                    ? t("processing.extractor.seenEvents", { count: sortedSeen.length })
                    : t("processing.extractor.started")),
                createdTime: startTime,
                endTime,
                externalId: config.externalId,
              })
            ) {
              break pipelineLoop;
            }
          }

          for (const event of otherEvents) {
            if (
              !pushRun({
                ...event,
                createdTime: event.createdTime,
                externalId: config.externalId,
              })
            ) {
              break pipelineLoop;
            }
          }
          pipelineIndex += 1;
          if (
            !cancelled &&
            !isStaleProcessingFetch(fetchGeneration, generation) &&
            (pipelineIndex % 3 === 0 || pipelineIndex === totalPipelines)
          ) {
            setLoadProgress({
              kind: "extractors_runs",
              current: pipelineIndex,
              total: totalPipelines,
            });
          }
        }

        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setExtractorRunsAll(runs);
          setExecutionsTruncated(hitExecutionCap);
          setLoadProgress(null);
          setRequestStats(
            processingRequestStats(failedRequests, totalRequests, permissionsDenied.current)
          );
          setExtractorsStatus("success");
        }
      } catch (error) {
        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setLoadProgress(null);
          setRequestStats(null);
          setExtractorsError(
            error instanceof Error ? error.message : t("processing.error.extractors")
          );
          setExtractorsStatus("error");
        }
      }
    };

    loadExtractorRuns();
    return () => {
      cancelled = true;
    };
  }, [
    fetchGeneration,
    executionLimit,
    extractorConfigs.length,
    fetchEnabled,
    isSdkLoading,
    refetchExecutionsOnly,
    sdk,
    windowRange?.start,
    windowRange?.end,
    t,
  ]);

  const extractorConfigMap = useMemo(() => {
    return extractorConfigs.reduce<Record<string, ExtPipeConfigSummary>>((acc, config) => {
      acc[config.externalId] = config;
      return acc;
    }, {});
  }, [extractorConfigs]);

  const filteredExtractorRuns = useMemo(() => {
    if (!windowRange) return [];
    const startWindow = windowRange.start;
    const endWindow = windowRange.end;
    return extractorRunsAll.filter((run) => {
      const start = run.createdTime;
      const end = run.endTime ?? run.createdTime;
      return start <= endWindow && end >= startWindow;
    });
  }, [extractorRunsAll, windowRange]);

  const getExtractorRadius = (run: ExtPipeRunSummary) => {
    const scaled = 6 + Math.sqrt(run.id % 100) * 0.8;
    return Math.min(16, Math.max(4, scaled));
  };

  const getExtractorColor = (run: ExtPipeRunSummary) => {
    const status = normalizeStatus(run.status);
    if (status === "seen") return "#06b6d4";
    if (status.includes("completed") || status.includes("success")) return "#16a34a";
    if (status.includes("failed") || status.includes("error")) return "#f97316";
    if (status.includes("running")) return "#2563eb";
    return "#a855f7";
  };

  return {
    extractorsStatus,
    executionsTruncated,
    loadProgress,
    requestStats,
    extractorsError,
    extractorConfigs,
    extractorRunsAll,
    extractorConfigMap,
    filteredExtractorRuns,
    getExtractorRadius,
    getExtractorColor,
  };
}
