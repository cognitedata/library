import type { CogniteClient } from "@cognite/sdk";
import { useCallback, useEffect, useMemo, useState } from "react";
import { normalizeStatus } from "@/shared/time-utils";
import {
  DEFAULT_PROCESSING_EXECUTION_CAP,
  type LoadState,
  type ProcessingDataLoadProgress,
  type ProcessingRequestStats,
  type TransformationJobSummary,
  type TransformationSummary,
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
import {
  cachedTransformationsList,
} from "@/transformations/transformations-cache";
import { loadTransformationJobsForWindow } from "@/transformations/transformation-jobs-service";

type UseTransformationDataArgs = {
  isSdkLoading: boolean;
  sdk: Pick<CogniteClient, "project" | "get">;
  windowRange: { start: number; end: number } | null;
  fetchEnabled?: boolean;
  windowSessionKey?: string;
  fetchGeneration?: number;
  refetchExecutionsOnly?: boolean;
  executionLimit?: number | null;
};

export function useTransformationData({
  isSdkLoading,
  sdk,
  windowRange,
  fetchEnabled = true,
  windowSessionKey = "",
  fetchGeneration = 0,
  refetchExecutionsOnly = false,
  executionLimit = DEFAULT_PROCESSING_EXECUTION_CAP,
}: UseTransformationDataArgs) {
  const { t } = useI18n();
  const [transformationsStatus, setTransformationsStatus] = useState<LoadState>("idle");
  const [executionsTruncated, setExecutionsTruncated] = useState(false);
  const [transformationsError, setTransformationsError] = useState<string | null>(null);
  const [transformationJobsAll, setTransformationJobsAll] = useState<TransformationJobSummary[]>([]);
  const [transformationNameMap, setTransformationNameMap] = useState<Record<string, string>>({});
  const [transformationMetaMap, setTransformationMetaMap] = useState<
    Record<string, TransformationSummary>
  >({});
  const [loadProgress, setLoadProgress] = useState<ProcessingDataLoadProgress | null>(null);
  const [requestStats, setRequestStats] = useState<ProcessingRequestStats | null>(null);

  const resetForNewWindow = useCallback(() => {
    setTransformationsStatus("idle");
    setLoadProgress(null);
    setExecutionsTruncated(false);
    setTransformationsError(null);
    setRequestStats(null);
    setTransformationJobsAll([]);
    setTransformationNameMap({});
    setTransformationMetaMap({});
  }, []);

  useProcessingWindowSessionReset(windowSessionKey, resetForNewWindow);
  useProcessingSeriesFetchLoading(
    fetchEnabled,
    isSdkLoading,
    windowRange,
    fetchGeneration,
    setTransformationsStatus
  );

  useEffect(() => {
    if (!fetchEnabled) {
      setLoadProgress(null);
      return;
    }
    if (isSdkLoading) return;
    if (!windowRange) return;

    const generation = fetchGeneration;
    let cancelled = false;
    const loadTransformations = async () => {
      setExecutionsTruncated(false);
      setTransformationsError(null);
      setRequestStats(null);
      if (!refetchExecutionsOnly) {
        setTransformationJobsAll([]);
        setTransformationNameMap({});
        setTransformationMetaMap({});
        setLoadProgress({ kind: "transformations_list" });
      } else {
        setTransformationJobsAll([]);
        setLoadProgress({
          kind: "transformations_jobs",
          current: 0,
          total: Object.keys(transformationMetaMap).length,
        });
      }
      try {
        let failedRequests = 0;
        let totalRequests = 0;
        const permissionsDenied = { current: false };
        let transformations: TransformationSummary[];
        if (refetchExecutionsOnly && Object.keys(transformationMetaMap).length > 0) {
          transformations = Object.values(transformationMetaMap);
        } else {
          totalRequests++;
          const response = (await withTransientRetries(() =>
            cachedTransformationsList(sdk, {
              includePublic: "true",
              limit: "1000",
            })
          )) as { data?: { items?: TransformationSummary[] } };
          transformations = response.data?.items ?? [];

          const nameMap: Record<string, string> = {};
          const metaMap: Record<string, TransformationSummary> = {};
          for (const transformation of transformations) {
            nameMap[String(transformation.id)] =
              transformation.name ??
              t("processing.transformation.defaultName", { id: transformation.id });
            metaMap[String(transformation.id)] = transformation;
          }
          if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
            setTransformationNameMap(nameMap);
            setTransformationMetaMap(metaMap);
          }
        }

        const total = transformations.length;
        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setLoadProgress({ kind: "transformations_jobs", current: 0, total });
        }

        const jobs: TransformationJobSummary[] = [];
        let index = 0;
        const cap = executionLimit;
        let hitExecutionCap = false;
        for (const transformation of transformations) {
          totalRequests++;
          try {
            const jobResponse = (await withTransientRetries(() =>
              loadTransformationJobsForWindow({
                sdk,
                transformationId: String(transformation.id),
                windowStart: windowRange.start,
                windowEnd: windowRange.end,
              })
            )) as TransformationJobSummary[];
            for (const job of jobResponse ?? []) {
              if (cap != null && jobs.length >= cap) {
                hitExecutionCap = true;
                break;
              }
              jobs.push(job);
            }
          } catch (e) {
            failedRequests++;
            noteForbiddenFailure(permissionsDenied, e);
          }
          if (hitExecutionCap) break;
          index += 1;
          if (
            !cancelled &&
            !isStaleProcessingFetch(fetchGeneration, generation) &&
            (index % 5 === 0 || index === total)
          ) {
            setTransformationJobsAll([...jobs]);
            setLoadProgress({ kind: "transformations_jobs", current: index, total });
          }
        }

        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setTransformationJobsAll(jobs);
          setExecutionsTruncated(hitExecutionCap);
          setLoadProgress(null);
          setRequestStats(
            processingRequestStats(failedRequests, totalRequests, permissionsDenied.current)
          );
          setTransformationsStatus("success");
        }
      } catch (error) {
        if (!cancelled && !isStaleProcessingFetch(fetchGeneration, generation)) {
          setLoadProgress(null);
          setRequestStats(null);
          setTransformationsError(
            error instanceof Error ? error.message : t("processing.error.transformations")
          );
          setTransformationsStatus("error");
        }
      }
    };

    loadTransformations();
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
    t,
    windowRange?.start,
    windowRange?.end,
  ]);

  const filteredTransformationJobs = useMemo(() => {
    if (!windowRange) return [];
    const startWindow = windowRange.start;
    const endWindow = windowRange.end;
    return transformationJobsAll.filter((job) => {
      const start = job.startedTime ?? startWindow;
      const end = job.finishedTime ?? endWindow;
      return start <= endWindow && end >= startWindow;
    });
  }, [transformationJobsAll, windowRange]);

  const getTransformationDuration = (job: TransformationJobSummary) => {
    if (!job.startedTime) return null;
    if (job.finishedTime && job.finishedTime >= job.startedTime) {
      return job.finishedTime - job.startedTime;
    }
    return null;
  };

  const getTransformationRadius = (job: TransformationJobSummary) => {
    const duration = getTransformationDuration(job);
    if (duration == null) return 6;
    const minutes = duration / 60000;
    const scaled = 4 + Math.sqrt(minutes) * 6;
    return Math.min(18, Math.max(4, scaled));
  };

  const getTransformationColor = (job: TransformationJobSummary) => {
    const status = normalizeStatus(job.status);
    if (status.includes("completed")) return "#16a34a";
    if (status.includes("timeout")) return "#7c3aed";
    if (status.includes("failed")) return "#f97316";
    if (status.includes("running")) return "#2563eb";
    return "#a855f7";
  };

  return {
    transformationsStatus,
    executionsTruncated,
    loadProgress,
    requestStats,
    transformationsError,
    transformationJobsAll,
    transformationNameMap,
    transformationMetaMap,
    filteredTransformationJobs,
    getTransformationDuration,
    getTransformationRadius,
    getTransformationColor,
  };
}
