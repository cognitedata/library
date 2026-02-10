import { useEffect, useMemo, useState } from "react";
import { normalizeStatus } from "@/shared/time-utils";
import type { LoadState, TransformationJobSummary, TransformationSummary } from "./types";
import { useI18n } from "@/shared/i18n";

type UseTransformationDataArgs = {
  isSdkLoading: boolean;
  sdk: { project: string; get: Function };
  windowRange: { start: number; end: number } | null;
};

export function useTransformationData({ isSdkLoading, sdk, windowRange }: UseTransformationDataArgs) {
  const { t } = useI18n();
  const [transformationsStatus, setTransformationsStatus] = useState<LoadState>("idle");
  const [transformationsError, setTransformationsError] = useState<string | null>(null);
  const [transformationJobsAll, setTransformationJobsAll] = useState<TransformationJobSummary[]>([]);
  const [transformationNameMap, setTransformationNameMap] = useState<Record<string, string>>({});
  const [transformationMetaMap, setTransformationMetaMap] = useState<
    Record<string, TransformationSummary>
  >({});

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const loadTransformations = async () => {
      setTransformationsStatus("loading");
      setTransformationsError(null);
      setTransformationJobsAll([]);
      setTransformationNameMap({});
      setTransformationMetaMap({});
      try {
        const response = await sdk.get<{
          items?: TransformationSummary[];
        }>(`/api/v1/projects/${sdk.project}/transformations`, {
          params: { includePublic: "true", limit: "1000" },
        });
        const transformations = response.data?.items ?? [];

        const nameMap: Record<string, string> = {};
        const metaMap: Record<string, TransformationSummary> = {};
        for (const transformation of transformations) {
          nameMap[String(transformation.id)] =
            transformation.name ??
            t("processing.transformation.defaultName", { id: transformation.id });
          metaMap[String(transformation.id)] = transformation;
        }

        const jobs: TransformationJobSummary[] = [];
        for (const transformation of transformations) {
          const jobResponse = await sdk.get<{
            items?: TransformationJobSummary[];
          }>(`/api/v1/projects/${sdk.project}/transformations/jobs`, {
            params: { limit: "1000", transformationId: String(transformation.id) },
          });
          jobs.push(...(jobResponse.data?.items ?? []));
        }

        if (!cancelled) {
          setTransformationJobsAll(jobs);
          setTransformationNameMap(nameMap);
          setTransformationMetaMap(metaMap);
          setTransformationsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
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
  }, [isSdkLoading, sdk, t]);

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
