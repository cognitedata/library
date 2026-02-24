import { useEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Loader } from "@/shared/Loader";
import { FunctionRunModal } from "./FunctionRunModal";
import { TransformationRunModal } from "./TransformationRunModal";
import { WorkflowRunModal } from "./WorkflowRunModal";
import { ExtractionPipelineRunModal } from "./ExtractionPipelineRunModal";
import { ProcessingChart } from "./ProcessingChart";
import { ProcessingHelpModal } from "./ProcessingHelpModal";
import { ProcessingHeatmapHelpModal } from "./ProcessingHeatmapHelpModal";
import { ProcessingBubbleLegend } from "./ProcessingBubbleLegend";
import { useExtractionPipelineData } from "./useExtractionPipelineData";
import { useFunctionData } from "./useFunctionData";
import { useTransformationData } from "./useTransformationData";
import { useWorkflowData } from "./useWorkflowData";
import { useI18n } from "@/shared/i18n";
import { ApiError } from "@/shared/ApiError";
import {
  formatTimeFields,
  formatUtcRangeCompact,
  formatZonedRangeCompact,
  getTimeZoneLabel,
  getUserTimeZone,
  toTimestamp,
} from "@/shared/time-utils";
import type {
  ExtPipeRunSummary,
  FunctionRunSummary,
  LoadState,
  TransformationJobSummary,
  WorkflowExecutionSummary,
} from "./types";

const hoursWindow = 1;
const bucketSeconds = 15;

export function Processing() {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { t } = useI18n();
  const [windowOffsetHours, setWindowOffsetHours] = useState(0);
  const [windowRange, setWindowRange] = useState<{ start: number; end: number } | null>(null);
  const [showLoader, setShowLoader] = useState(false);
  const [showHelp, setShowHelp] = useState(false);
  const [showHeatmapHelp, setShowHeatmapHelp] = useState(false);
  const [visibleSeries, setVisibleSeries] = useState({
    functions: true,
    transformations: true,
    workflows: true,
    extractors: false,
  });
  const [loaderDismissed, setLoaderDismissed] = useState(false);
  const loaderWasLoadingRef = useRef(false);
  const [selectedRun, setSelectedRun] = useState<FunctionRunSummary | null>(null);
  const [selectedFunction, setSelectedFunction] = useState<Record<string, unknown> | null>(null);
  const [selectedLogs, setSelectedLogs] = useState<Array<{ message?: string }>>([]);
  const [selectedLogsStatus, setSelectedLogsStatus] = useState<LoadState>("idle");
  const [selectedLogsError, setSelectedLogsError] = useState<string | null>(null);
  const [selectedTransformationJob, setSelectedTransformationJob] =
    useState<TransformationJobSummary | null>(null);
  const [selectedTransformation, setSelectedTransformation] =
    useState<Record<string, unknown> | null>(null);
  const [selectedWorkflowExecution, setSelectedWorkflowExecution] =
    useState<WorkflowExecutionSummary | null>(null);
  const [selectedExtractorRun, setSelectedExtractorRun] =
    useState<(ExtPipeRunSummary & { externalId: string }) | null>(null);
  const [selectedExtractorConfig, setSelectedExtractorConfig] = useState<{
    name?: string;
    externalId?: string;
  } | null>(null);
  const [scheduleStatus, setScheduleStatus] = useState<LoadState>("idle");
  const [scheduleError, setScheduleError] = useState<string | null>(null);
  type ScheduleEntryType = "function" | "transformation" | "workflow";
  const [scheduleEntries, setScheduleEntries] = useState<
    Array<{ cron: string; name: string; type: ScheduleEntryType }>
  >([]);
  const [heatmapVisibleTypes, setHeatmapVisibleTypes] = useState({
    functions: true,
    transformations: true,
    workflows: true,
  });
  const [hoverCell, setHoverCell] = useState<{
    hour: number;
    minute: number;
    count: number;
    names: string[];
  } | null>(null);
  const [pinnedHeatmapCell, setPinnedHeatmapCell] = useState<{
    hour: number;
    minute: number;
    count: number;
    names: string[];
  } | null>(null);
  const lastHoverKeyRef = useRef<string | null>(null);

  const {
    status,
    errorMessage,
    availabilityMessage,
    runs,
    functionNameMap,
    functionMetaMap,
    failureDurationMs,
    getRunDuration,
    getRadius,
    getColor,
  } = useFunctionData({ isSdkLoading, sdk, windowRange });

  const {
    transformationsStatus,
    transformationsError,
    transformationNameMap,
    transformationMetaMap,
    filteredTransformationJobs,
    getTransformationDuration,
    getTransformationRadius,
    getTransformationColor,
  } = useTransformationData({ isSdkLoading, sdk, windowRange });

  const {
    workflowsStatus,
    workflowsError,
    filteredWorkflowExecutions,
    workflowDetails,
    workflowDetailsStatus,
    workflowDetailsError,
    getWorkflowDuration,
    getWorkflowRadius,
    getWorkflowColor,
    fetchWorkflowDetails,
    resetWorkflowDetails,
  } = useWorkflowData({ isSdkLoading, sdk, windowRange });

  const {
    extractorsStatus,
    extractorsError,
    extractorConfigMap,
    filteredExtractorRuns,
    getExtractorRadius,
    getExtractorColor,
  } = useExtractionPipelineData({ isSdkLoading, sdk, windowRange });

  const isProcessingLoading =
    status === "loading" ||
    transformationsStatus === "loading" ||
    workflowsStatus === "loading" ||
    extractorsStatus === "loading";

  useEffect(() => {
    const wasLoading = loaderWasLoadingRef.current;
    if (isProcessingLoading && !wasLoading) {
      setLoaderDismissed(false);
    }
    loaderWasLoadingRef.current = isProcessingLoading;
    if (!isProcessingLoading) {
      setShowLoader(false);
      return;
    }
    if (!loaderDismissed) {
      setShowLoader(true);
    }
  }, [isProcessingLoading, loaderDismissed]);

  useEffect(() => {
    if (isSdkLoading) return;
    const now = new Date();
    const currentHourStart = new Date(now);
    currentHourStart.setMinutes(0, 0, 0);
    if (windowOffsetHours === 0) {
      setWindowRange({ start: currentHourStart.getTime(), end: now.getTime() });
      return;
    }
    const endWindow = new Date(currentHourStart);
    endWindow.setHours(endWindow.getHours() - windowOffsetHours);
    const startWindow = new Date(endWindow);
    startWindow.setHours(startWindow.getHours() - hoursWindow);
    setWindowRange({ start: startWindow.getTime(), end: endWindow.getTime() });
  }, [isSdkLoading, windowOffsetHours]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;

    const readCron = (item: Record<string, unknown>) => {
      const direct = item.cron ?? item.cronExpression;
      if (typeof direct === "string" && direct.trim()) return direct.trim();
      const triggerRule = item.triggerRule as Record<string, unknown> | undefined;
      const triggerDirect = triggerRule?.cron ?? triggerRule?.cronExpression;
      if (typeof triggerDirect === "string" && triggerDirect.trim()) return triggerDirect.trim();
      const schedule = item.schedule as Record<string, unknown> | undefined;
      const nested = schedule?.cron ?? schedule?.cronExpression;
      if (typeof nested === "string" && nested.trim()) return nested.trim();
      return "";
    };

    const loadSchedules = async () => {
      setScheduleStatus("loading");
      setScheduleError(null);
      setScheduleEntries([]);
      try {
        const entries: Array<{ cron: string; name: string; type: ScheduleEntryType }> = [];

        const functionSchedules = (await sdk.post(
          `/api/v1/projects/${sdk.project}/functions/schedules/list`,
          {
            data: { limit: 1000 },
          }
        )) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of functionSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const name =
            (item.name as string | undefined) ??
            (item.functionExternalId as string | undefined) ??
            (item.functionId as string | undefined) ??
            t("processing.heatmap.unknownFunction");
          entries.push({ cron, name, type: "function" });
        }

        const transformationSchedules = (await sdk.get(
          `/api/v1/projects/${sdk.project}/transformations/schedules`,
          {
            params: { limit: "1000" },
          }
        )) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of transformationSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const name =
            (item.name as string | undefined) ??
            (item.transformationExternalId as string | undefined) ??
            (item.transformationId as string | undefined) ??
            t("processing.heatmap.unknownTransformation");
          entries.push({ cron, name, type: "transformation" });
        }

        let triggerCursor: string | undefined;
        do {
          const workflowTriggers = (await sdk.get(
            `/api/v1/projects/${sdk.project}/workflows/triggers`,
            {
              params: { limit: "1000", cursor: triggerCursor },
            }
          )) as { data?: { items?: Array<Record<string, unknown>>; nextCursor?: string } };
          for (const item of workflowTriggers.data?.items ?? []) {
            const cron = readCron(item);
            if (!cron) continue;
            const workflowId = item.workflowExternalId as string | undefined;
            const triggerId =
              (item.externalId as string | undefined) ??
              (item.id as string | undefined) ??
              "trigger";
            const name = workflowId ? `${workflowId} · ${triggerId}` : triggerId;
            entries.push({ cron, name, type: "workflow" });
          }
          triggerCursor = workflowTriggers.data?.nextCursor;
        } while (triggerCursor);

        if (!cancelled) {
          setScheduleEntries(entries);
          setScheduleStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setScheduleError(
            error instanceof Error ? error.message : t("processing.heatmap.error")
          );
          setScheduleStatus("error");
        }
      }
    };

    loadSchedules();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t]);

  const heatmapCounts = useMemo(() => {
    if (scheduleStatus !== "success") return [];
    const typeToKey: Record<ScheduleEntryType, keyof typeof heatmapVisibleTypes> = {
      function: "functions",
      transformation: "transformations",
      workflow: "workflows",
    };
    const filtered = scheduleEntries.filter(
      (entry) => heatmapVisibleTypes[typeToKey[entry.type]]
    );
    const parseField = (field: string, min: number, max: number) => {
      if (!field) return [];
      const values = new Set<number>();
      const segments = field.split(",");
      for (const segment of segments) {
        const [rangePart, stepPart] = segment.split("/");
        const step = stepPart ? Number(stepPart) : 1;
        if (!Number.isFinite(step) || step <= 0) continue;
        let start = min;
        let end = max;
        if (rangePart && rangePart !== "*") {
          if (rangePart.includes("-")) {
            const [rawStart, rawEnd] = rangePart.split("-");
            start = Number(rawStart);
            end = Number(rawEnd);
          } else {
            start = Number(rangePart);
            end = Number(rangePart);
          }
        }
        if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
        for (let value = start; value <= end; value += step) {
          if (value >= min && value <= max) values.add(value);
        }
      }
      return Array.from(values).sort((a, b) => a - b);
    };

    const counts = Array.from({ length: 60 }, () =>
      Array.from({ length: 24 }, () => ({ count: 0, names: [] as string[] }))
    );
    for (const entry of filtered) {
      const parts = entry.cron.trim().split(/\s+/);
      if (parts.length < 2) continue;
      const minutes = parseField(parts[0], 0, 59);
      const hours = parseField(parts[1], 0, 23);
      if (minutes.length === 0 || hours.length === 0) continue;
      for (const minute of minutes) {
        for (const hour of hours) {
          counts[minute][hour].count += 1;
          counts[minute][hour].names.push(entry.name);
        }
      }
    }
    return counts;
  }, [scheduleEntries, scheduleStatus, heatmapVisibleTypes]);

  const getHeatColor = (count: number) => {
    if (count <= 0) return "#e0f2fe";
    if (count >= 10) return "#ef4444";
    const green = [34, 197, 94];
    const yellow = [250, 204, 21];
    const red = [239, 68, 68];
    const mix = (from: number[], to: number[], ratio: number) => {
      const clamped = Math.max(0, Math.min(1, ratio));
      const channel = (index: number) =>
        Math.round(from[index] + (to[index] - from[index]) * clamped);
      return `rgb(${channel(0)}, ${channel(1)}, ${channel(2)})`;
    };
    if (count <= 5) {
      return mix(green, yellow, (count - 1) / 4);
    }
    return mix(yellow, red, (count - 5) / 5);
  };

  const parallelSeries = useMemo(() => {
    if (!windowRange) return [];
    const endWindow = windowRange.end;
    const startWindow = windowRange.start;
    const bucketMs = bucketSeconds * 1000;
    const bucketCount = Math.ceil((endWindow - startWindow) / bucketMs);
    const buckets = Array.from({ length: bucketCount }, (_, index) => ({
      time: startWindow + index * bucketMs,
      count: 0,
    }));

    const getRunStart = (run: FunctionRunSummary) =>
      toTimestamp(run.startTime ?? run.createdTime) ?? startWindow;
    const getRunEnd = (run: FunctionRunSummary) =>
      toTimestamp(run.endTime ?? run.lastUpdatedTime) ?? endWindow;

    for (const run of runs) {
      const start = getRunStart(run);
      const end = getRunEnd(run);
      for (const bucket of buckets) {
        const bucketStart = bucket.time;
        const bucketEnd = bucketStart + bucketMs;
        if (start <= bucketEnd && end >= bucketStart) {
          bucket.count += 1;
        }
      }
    }
    return buckets;
  }, [runs, windowRange]);

  const transformationSeries = useMemo(() => {
    if (!windowRange) return [];
    const endWindow = windowRange.end;
    const startWindow = windowRange.start;
    const bucketMs = bucketSeconds * 1000;
    const bucketCount = Math.ceil((endWindow - startWindow) / bucketMs);
    const buckets = Array.from({ length: bucketCount }, (_, index) => ({
      time: startWindow + index * bucketMs,
      count: 0,
    }));

    for (const job of filteredTransformationJobs) {
      const start = job.startedTime ?? startWindow;
      const end = job.finishedTime ?? endWindow;
      for (const bucket of buckets) {
        const bucketStart = bucket.time;
        const bucketEnd = bucketStart + bucketMs;
        if (start <= bucketEnd && end >= bucketStart) {
          bucket.count += 1;
        }
      }
    }
    return buckets;
  }, [filteredTransformationJobs, windowRange]);

  const workflowSeries = useMemo(() => {
    if (!windowRange) return [];
    const endWindow = windowRange.end;
    const startWindow = windowRange.start;
    const bucketMs = bucketSeconds * 1000;
    const bucketCount = Math.ceil((endWindow - startWindow) / bucketMs);
    const buckets = Array.from({ length: bucketCount }, (_, index) => ({
      time: startWindow + index * bucketMs,
      count: 0,
    }));

    for (const execution of filteredWorkflowExecutions) {
      const start = execution.startTime ?? execution.createdTime;
      const end = execution.endTime ?? execution.startTime ?? execution.createdTime;
      for (const bucket of buckets) {
        const bucketStart = bucket.time;
        const bucketEnd = bucketStart + bucketMs;
        if (start <= bucketEnd && end >= bucketStart) {
          bucket.count += 1;
        }
      }
    }
    return buckets;
  }, [filteredWorkflowExecutions, windowRange]);

  const extractorSeries = useMemo(() => {
    if (!windowRange) return [];
    const endWindow = windowRange.end;
    const startWindow = windowRange.start;
    const bucketMs = bucketSeconds * 1000;
    const bucketCount = Math.ceil((endWindow - startWindow) / bucketMs);
    const buckets = Array.from({ length: bucketCount }, (_, index) => ({
      time: startWindow + index * bucketMs,
      count: 0,
    }));

    for (const run of filteredExtractorRuns) {
      const start = run.createdTime;
      const end = run.endTime ?? run.createdTime;
      for (const bucket of buckets) {
        const bucketStart = bucket.time;
        const bucketEnd = bucketStart + bucketMs;
        if (start <= bucketEnd && end >= bucketStart) {
          bucket.count += 1;
        }
      }
    }
    return buckets;
  }, [filteredExtractorRuns, windowRange]);

  const maxParallel = useMemo(() => {
    return parallelSeries.reduce((max, bucket) => Math.max(max, bucket.count), 0);
  }, [parallelSeries]);

  const maxTransformParallel = useMemo(() => {
    return transformationSeries.reduce((max, bucket) => Math.max(max, bucket.count), 0);
  }, [transformationSeries]);

  const maxWorkflowParallel = useMemo(() => {
    return workflowSeries.reduce((max, bucket) => Math.max(max, bucket.count), 0);
  }, [workflowSeries]);

  const maxExtractorParallel = useMemo(() => {
    return extractorSeries.reduce((max, bucket) => Math.max(max, bucket.count), 0);
  }, [extractorSeries]);

  const visibleParallelSeries = visibleSeries.functions ? parallelSeries : [];
  const visibleTransformationSeries = visibleSeries.transformations ? transformationSeries : [];
  const visibleWorkflowSeries = visibleSeries.workflows ? workflowSeries : [];
  const visibleExtractorSeries = visibleSeries.extractors ? extractorSeries : [];
  const visibleMaxParallel = visibleSeries.functions ? maxParallel : 0;
  const visibleMaxTransform = visibleSeries.transformations ? maxTransformParallel : 0;
  const visibleMaxWorkflow = visibleSeries.workflows ? maxWorkflowParallel : 0;
  const visibleMaxExtractor = visibleSeries.extractors ? maxExtractorParallel : 0;

  const fetchSelectedRunLogs = async (run: FunctionRunSummary) => {
    if (!run.functionId || !run.id) return;
    setSelectedLogsStatus("loading");
    setSelectedLogsError(null);
    setSelectedLogs([]);
    try {
      const response = await sdk.get<{
        items?: { message?: string }[];
      }>(`/api/v1/projects/${sdk.project}/functions/${run.functionId}/calls/${run.id}/logs`);
      setSelectedLogs(response.data?.items ?? []);
      setSelectedLogsStatus("success");
    } catch (error) {
      setSelectedLogsError(error instanceof Error ? error.message : t("processing.modal.logs.error"));
      setSelectedLogsStatus("error");
    }
  };

  return (
    <section className="flex flex-col gap-4">
      <header className="relative flex flex-col gap-1">
        <h2 className="text-2xl font-semibold text-slate-900">{t("processing.title")}</h2>
        <p className="text-sm text-slate-500">
          {t("processing.subtitle", { hoursWindow })}
        </p>
        <button
          type="button"
          className="absolute right-0 top-0 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
          onClick={() => setShowHelp(true)}
        >
          {t("shared.help.button")}
        </button>
        {windowRange ? (
          <div className="mt-2 flex flex-wrap items-center gap-3 text-xs text-slate-600">
            <span className="rounded-md border border-slate-200 bg-white px-2 py-1">
              {t("processing.time.utc")}{" "}
              {formatUtcRangeCompact(windowRange.start, windowRange.end)}
            </span>
            <span className="rounded-md border border-slate-200 bg-white px-2 py-1">
              {t("processing.time.local", { tzLabel: getTimeZoneLabel(getUserTimeZone()) })}{" "}
              {formatZonedRangeCompact(
                windowRange.start,
                windowRange.end,
                getUserTimeZone()
              )}
            </span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                className="rounded-md border border-slate-200 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50"
                onClick={() => setWindowOffsetHours((prev) => prev + 1)}
              >
                {t("processing.action.previous")}
              </button>
              <button
                type="button"
                className="rounded-md border border-slate-200 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                onClick={() => setWindowOffsetHours((prev) => Math.max(0, prev - 1))}
                disabled={windowOffsetHours === 0}
              >
                {t("processing.action.next")}
              </button>
            </div>
          </div>
        ) : null}
      </header>
      <Card>
        <CardHeader>
          <CardTitle>{t("processing.card.concurrency.title")}</CardTitle>
          <CardDescription>
            {t("processing.card.concurrency.description", { bucketSeconds })}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {status !== "success" ||
          transformationsStatus !== "success" ||
          workflowsStatus !== "success" ||
          extractorsStatus !== "success" ? (
            <div className="mb-3 space-y-2 text-xs text-slate-600">
              {status !== "success" ? (
                <div className="flex items-center gap-2">
                  <span className="w-28">{t("processing.legend.functions")}</span>
                  {status === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : (
                    <span className="h-2 w-40 rounded-sm bg-slate-200/80 animate-pulse" />
                  )}
                </div>
              ) : null}
              {transformationsStatus !== "success" ? (
                <div className="flex items-center gap-2">
                  <span className="w-28">{t("processing.legend.transformations")}</span>
                  {transformationsStatus === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : (
                    <span className="h-2 w-40 rounded-sm bg-slate-200/80 animate-pulse" />
                  )}
                </div>
              ) : null}
              {workflowsStatus !== "success" ? (
                <div className="flex items-center gap-2">
                  <span className="w-28">{t("processing.legend.workflows")}</span>
                  {workflowsStatus === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : (
                    <span className="h-2 w-40 rounded-sm bg-slate-200/80 animate-pulse" />
                  )}
                </div>
              ) : null}
              {extractorsStatus !== "success" ? (
                <div className="flex items-center gap-2">
                  <span className="w-28">{t("processing.legend.extractors")}</span>
                  {extractorsStatus === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : (
                    <span className="h-2 w-40 rounded-sm bg-slate-200/80 animate-pulse" />
                  )}
                </div>
              ) : null}
            </div>
          ) : null}
          {status === "error" ? (
            <ApiError
              message={errorMessage ?? t("processing.error.runs")}
              api="POST /functions/{functionId}/calls/list"
            />
          ) : null}
          {availabilityMessage ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              {availabilityMessage ?? t("processing.unavailable.functions")}
            </div>
          ) : null}
          {transformationsStatus === "error" ? (
            <ApiError
              message={transformationsError ?? t("processing.error.transformations")}
              api="GET /transformations"
            />
          ) : null}
          {workflowsStatus === "error" ? (
            <ApiError
              message={workflowsError ?? t("processing.error.workflows")}
              api="POST /workflows/executions/list"
            />
          ) : null}
          {extractorsStatus === "error" ? (
            <ApiError
              message={extractorsError ?? t("processing.error.extractors")}
              api="POST /extpipes/runs/list"
            />
          ) : null}
          {!availabilityMessage ? (
            <div className="rounded-md border border-slate-200 bg-white p-3">
              <div className="text-sm text-slate-700">
                {t("processing.stats.executions", { count: runs.length, peak: maxParallel })}
              </div>
              <div className="mt-2 flex flex-wrap items-center gap-3 text-xs text-slate-600">
                <button
                  type="button"
                  className={`flex items-center gap-2 rounded-md px-2 py-1 ${
                    visibleSeries.functions
                      ? "text-slate-700 hover:bg-slate-50"
                      : "text-slate-400 line-through"
                  }`}
                  onClick={() =>
                    setVisibleSeries((prev) => ({ ...prev, functions: !prev.functions }))
                  }
                >
                  <span className="h-2 w-4 rounded-sm bg-blue-600" />
                  {t("processing.legend.functions")}
                </button>
                <button
                  type="button"
                  className={`flex items-center gap-2 rounded-md px-2 py-1 ${
                    visibleSeries.transformations
                      ? "text-slate-700 hover:bg-slate-50"
                      : "text-slate-400 line-through"
                  }`}
                  onClick={() =>
                    setVisibleSeries((prev) => ({
                      ...prev,
                      transformations: !prev.transformations,
                    }))
                  }
                >
                  <span className="h-2 w-4 rounded-sm bg-orange-500" />
                  {t("processing.legend.transformations")}
                </button>
                <button
                  type="button"
                  className={`flex items-center gap-2 rounded-md px-2 py-1 ${
                    visibleSeries.workflows
                      ? "text-slate-700 hover:bg-slate-50"
                      : "text-slate-400 line-through"
                  }`}
                  onClick={() =>
                    setVisibleSeries((prev) => ({ ...prev, workflows: !prev.workflows }))
                  }
                >
                  <span className="h-2 w-4 rounded-sm bg-purple-500" />
                  {t("processing.legend.workflows")}
                </button>
                <button
                  type="button"
                  className={`flex items-center gap-2 rounded-md px-2 py-1 ${
                    visibleSeries.extractors
                      ? "text-slate-700 hover:bg-slate-50"
                      : "text-slate-400 line-through"
                  }`}
                  onClick={() =>
                    setVisibleSeries((prev) => ({ ...prev, extractors: !prev.extractors }))
                  }
                >
                  <span className="h-2 w-4 rounded-sm bg-cyan-500" />
                  {t("processing.legend.extractors")}
                </button>
              </div>
              <div className="mt-4 overflow-x-auto">
                <ProcessingChart
                  windowRange={windowRange}
                  parallelSeries={visibleParallelSeries}
                  transformationSeries={visibleTransformationSeries}
                  workflowSeries={visibleWorkflowSeries}
                  extractorSeries={visibleExtractorSeries}
                  maxParallel={visibleMaxParallel}
                  maxTransformParallel={visibleMaxTransform}
                  maxWorkflowParallel={visibleMaxWorkflow}
                  maxExtractorParallel={visibleMaxExtractor}
                  runs={runs}
                  getRunDuration={getRunDuration}
                  getRadius={getRadius}
                  getColor={getColor}
                  transformationJobs={filteredTransformationJobs}
                  getTransformationDuration={getTransformationDuration}
                  getTransformationRadius={getTransformationRadius}
                  getTransformationColor={getTransformationColor}
                  workflowExecutions={filteredWorkflowExecutions}
                  getWorkflowDuration={getWorkflowDuration}
                  getWorkflowRadius={getWorkflowRadius}
                  getWorkflowColor={getWorkflowColor}
                  extractorRuns={filteredExtractorRuns}
                  extractorConfigMap={extractorConfigMap}
                  getExtractorRadius={getExtractorRadius}
                  getExtractorColor={getExtractorColor}
                  onRunClick={(run) => {
                    setSelectedRun(run);
                    const functionId = run.functionId ?? "";
                    setSelectedFunction(functionMetaMap[functionId] ?? null);
                    void fetchSelectedRunLogs(run);
                    setSelectedExtractorRun(null);
                    setSelectedExtractorConfig(null);
                  }}
                  onTransformationClick={(job) => {
                    setSelectedTransformationJob(job);
                    setSelectedTransformation(
                      transformationMetaMap[String(job.transformationId ?? "")] ?? null
                    );
                    setSelectedRun(null);
                    setSelectedFunction(null);
                    setSelectedLogs([]);
                    setSelectedLogsStatus("idle");
                    setSelectedLogsError(null);
                    setSelectedExtractorRun(null);
                    setSelectedExtractorConfig(null);
                  }}
                  onWorkflowClick={(execution) => {
                    setSelectedWorkflowExecution(execution);
                    setSelectedRun(null);
                    setSelectedFunction(null);
                    setSelectedLogs([]);
                    setSelectedLogsStatus("idle");
                    setSelectedLogsError(null);
                    setSelectedTransformationJob(null);
                    setSelectedTransformation(null);
                    setSelectedExtractorRun(null);
                    setSelectedExtractorConfig(null);
                    void fetchWorkflowDetails(execution.id);
                  }}
                  onExtractorClick={(run) => {
                    setSelectedExtractorRun(run);
                    setSelectedExtractorConfig(extractorConfigMap[run.externalId] ?? null);
                    setSelectedRun(null);
                    setSelectedFunction(null);
                    setSelectedLogs([]);
                    setSelectedLogsStatus("idle");
                    setSelectedLogsError(null);
                    setSelectedTransformationJob(null);
                    setSelectedTransformation(null);
                    setSelectedWorkflowExecution(null);
                  }}
                  functionNameMap={functionNameMap}
                  bandStatusLabels={{
                    functions:
                      status === "loading"
                        ? t("processing.bubbles.loading")
                        : status === "error"
                          ? t("processing.status.error")
                          : runs.length === 0
                            ? t("processing.bubbles.empty")
                            : "",
                    transformations:
                      transformationsStatus === "loading"
                        ? t("processing.bubbles.loading")
                        : transformationsStatus === "error"
                          ? t("processing.status.error")
                          : filteredTransformationJobs.length === 0
                            ? t("processing.bubbles.empty")
                            : "",
                    workflows:
                      workflowsStatus === "loading"
                        ? t("processing.bubbles.loading")
                        : workflowsStatus === "error"
                          ? t("processing.status.error")
                          : filteredWorkflowExecutions.length === 0
                            ? t("processing.bubbles.empty")
                            : "",
                    extractors:
                      extractorsStatus === "loading"
                        ? t("processing.bubbles.loading")
                        : extractorsStatus === "error"
                          ? t("processing.status.error")
                          : filteredExtractorRuns.length === 0
                            ? t("processing.bubbles.empty")
                            : "",
                  }}
                />
              </div>
              <details className="mt-4 rounded-md border border-slate-200 bg-white text-xs text-slate-600">
                <summary className="cursor-pointer select-none px-3 py-2 text-sm font-medium text-slate-700">
                  {t("processing.legend.panel")}
                </summary>
                <div className="border-t border-slate-200 px-3 py-3">
                  <ProcessingBubbleLegend />
                </div>
              </details>
            </div>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{t("processing.failed.title")}</CardTitle>
          <CardDescription>{t("processing.failed.description")}</CardDescription>
        </CardHeader>
        <CardContent>
          {status === "loading" ? (
            <div className="text-sm text-slate-600">{t("processing.loading.stats")}</div>
          ) : null}
          {status === "error" ? (
            <ApiError
              message={errorMessage ?? t("processing.error.runs")}
              api="POST /functions/{functionId}/calls/list"
            />
          ) : null}
          {status !== "error" && !availabilityMessage ? (
            <div className="rounded-md border border-slate-200 bg-white p-3 text-sm text-slate-700">
              {t("processing.failed.minutes", {
                minutes: Math.round(failureDurationMs / 60000),
              })}
            </div>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="relative">
          <div className="flex flex-col gap-1">
            <CardTitle>{t("processing.heatmap.title")}</CardTitle>
            <CardDescription>{t("processing.heatmap.description")}</CardDescription>
          </div>
          <button
            type="button"
            className="absolute right-0 top-0 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
            onClick={() => setShowHeatmapHelp(true)}
          >
            {t("shared.help.button")}
          </button>
        </CardHeader>
        <CardContent>
          {scheduleStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("processing.heatmap.loading")}</div>
          ) : null}
          {scheduleStatus === "error" ? (
            <ApiError message={scheduleError ?? t("processing.heatmap.error")} />
          ) : null}
          {scheduleStatus === "success" ? (
            heatmapCounts.length === 0 ? (
              <div className="text-sm text-slate-600">{t("processing.heatmap.empty")}</div>
            ) : (
              <div className="space-y-3">
                <div className="flex flex-wrap items-center gap-3 text-xs text-slate-600">
                  <button
                    type="button"
                    className={`flex items-center gap-2 rounded-md px-2 py-1 ${
                      heatmapVisibleTypes.functions
                        ? "text-slate-700 hover:bg-slate-50"
                        : "text-slate-400 line-through"
                    }`}
                    onClick={() =>
                      setHeatmapVisibleTypes((prev) => ({
                        ...prev,
                        functions: !prev.functions,
                      }))
                    }
                  >
                    <span className="h-2 w-4 rounded-sm bg-blue-600" />
                    {t("processing.legend.functions")}
                  </button>
                  <button
                    type="button"
                    className={`flex items-center gap-2 rounded-md px-2 py-1 ${
                      heatmapVisibleTypes.transformations
                        ? "text-slate-700 hover:bg-slate-50"
                        : "text-slate-400 line-through"
                    }`}
                    onClick={() =>
                      setHeatmapVisibleTypes((prev) => ({
                        ...prev,
                        transformations: !prev.transformations,
                      }))
                    }
                  >
                    <span className="h-2 w-4 rounded-sm bg-orange-500" />
                    {t("processing.legend.transformations")}
                  </button>
                  <button
                    type="button"
                    className={`flex items-center gap-2 rounded-md px-2 py-1 ${
                      heatmapVisibleTypes.workflows
                        ? "text-slate-700 hover:bg-slate-50"
                        : "text-slate-400 line-through"
                    }`}
                    onClick={() =>
                      setHeatmapVisibleTypes((prev) => ({
                        ...prev,
                        workflows: !prev.workflows,
                      }))
                    }
                  >
                    <span className="h-2 w-4 rounded-sm bg-purple-500" />
                    {t("processing.legend.workflows")}
                  </button>
                </div>
                <div className="flex gap-4 items-stretch">
                  <div className="flex-1 overflow-auto rounded-md border border-slate-200 bg-white p-3">
                    <div
                      className="grid gap-0.5"
                      style={{ gridTemplateColumns: "40px repeat(24, minmax(0, 1fr))" }}
                      onMouseLeave={() => {
                        lastHoverKeyRef.current = null;
                        setHoverCell(null);
                      }}
                    >
                      <div />
                      {Array.from({ length: 24 }, (_, hour) => (
                        <div
                          key={`hour-${hour}`}
                          className="text-center text-[10px] text-slate-500"
                        >
                          {hour}
                        </div>
                      ))}
                      {Array.from({ length: 60 }, (_, minute) => (
                        <div key={`row-${minute}`} className="contents">
                          <div className="text-[10px] text-slate-500">
                            {minute % 5 === 0 ? String(minute).padStart(2, "0") : ""}
                          </div>
                          {Array.from({ length: 24 }, (_, hour) => {
                            const cell = heatmapCounts[minute]?.[hour];
                            const count = cell?.count ?? 0;
                            const names = cell?.names ?? [];
                            const hoverKey = `${hour}:${minute}:${count}:${names.length}`;
                            const isPinned =
                              pinnedHeatmapCell?.hour === hour &&
                              pinnedHeatmapCell?.minute === minute;
                            return (
                              <div
                                key={`cell-${minute}-${hour}`}
                                className="h-3 w-full rounded-sm border border-slate-100 cursor-pointer"
                                style={{
                                  backgroundColor: getHeatColor(count),
                                  outline: isPinned ? "2px solid #3b82f6" : undefined,
                                  outlineOffset: -1,
                                }}
                                onMouseMove={() => {
                                  if (lastHoverKeyRef.current === hoverKey) return;
                                  lastHoverKeyRef.current = hoverKey;
                                  setHoverCell({ hour, minute, count, names });
                                }}
                                onClick={() => {
                                  setPinnedHeatmapCell((prev) =>
                                    prev?.hour === hour && prev?.minute === minute
                                      ? null
                                      : { hour, minute, count, names }
                                  );
                                }}
                              />
                            );
                          })}
                        </div>
                      ))}
                    </div>
                    <div className="mt-3 flex flex-wrap items-center gap-3 text-[11px] text-slate-600">
                      <span className="flex items-center gap-1">
                        <span className="h-3 w-3 rounded-sm border border-slate-200 bg-white" />
                        {t("processing.heatmap.legend.none")}
                      </span>
                      <span className="flex items-center gap-1">
                        <span
                          className="h-3 w-3 rounded-sm border border-slate-200"
                          style={{ backgroundColor: getHeatColor(1) }}
                        />
                        {t("processing.heatmap.legend.one")}
                      </span>
                      <span className="flex items-center gap-1">
                        <span
                          className="h-3 w-3 rounded-sm border border-slate-200"
                          style={{ backgroundColor: getHeatColor(5) }}
                        />
                        {t("processing.heatmap.legend.mid")}
                      </span>
                      <span className="flex items-center gap-1">
                        <span
                          className="h-3 w-3 rounded-sm border border-slate-200"
                          style={{ backgroundColor: getHeatColor(10) }}
                        />
                        {t("processing.heatmap.legend.high")}
                      </span>
                    </div>
                  </div>
                  <div className="w-64 flex flex-col rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700 min-h-0">
                    {(pinnedHeatmapCell ?? hoverCell) ? (
                      <>
                        <div className="flex items-center justify-between gap-2 shrink-0">
                          <div className="font-semibold">
                            {t("processing.heatmap.hover.title", {
                              time: `${String(
                                (pinnedHeatmapCell ?? hoverCell)!.hour
                              ).padStart(2, "0")}:${String(
                                (pinnedHeatmapCell ?? hoverCell)!.minute
                              ).padStart(2, "0")}`,
                              count: (pinnedHeatmapCell ?? hoverCell)!.count,
                            })}
                            {pinnedHeatmapCell ? (
                              <span className="ml-1 text-slate-500 font-normal">
                                ({t("processing.heatmap.pinned")})
                              </span>
                            ) : null}
                          </div>
                          {pinnedHeatmapCell ? (
                            <button
                              type="button"
                              className="shrink-0 rounded px-1.5 py-0.5 text-[11px] text-slate-500 hover:bg-slate-200"
                              onClick={() => setPinnedHeatmapCell(null)}
                            >
                              {t("processing.heatmap.unpin")}
                            </button>
                          ) : null}
                        </div>
                        {(pinnedHeatmapCell ?? hoverCell)!.names.length > 0 ? (
                          <>
                            <div className="mt-1 flex shrink-0">
                              <button
                                type="button"
                                className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] font-medium text-slate-700 hover:bg-slate-100"
                                onClick={() => {
                                  const names = (pinnedHeatmapCell ?? hoverCell)!.names;
                                  void navigator.clipboard.writeText(names.join("\n"));
                                }}
                              >
                                {t("processing.heatmap.copyList")}
                              </button>
                            </div>
                            <ul className="mt-1 flex-1 min-h-0 list-disc space-y-0.5 overflow-auto pl-4">
                              {(pinnedHeatmapCell ?? hoverCell)!.names.map((name, index) => (
                                <li key={`${name}-${index}`}>{name}</li>
                              ))}
                            </ul>
                          </>
                        ) : (
                          <div className="mt-1 text-slate-500 flex-1">
                            {t("processing.heatmap.hover.none")}
                          </div>
                        )}
                      </>
                    ) : (
                      <div className="text-slate-500 flex-1 flex items-center">
                        {t("processing.heatmap.hover.none")}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )
          ) : null}
        </CardContent>
      </Card>
      <FunctionRunModal
        open={!!selectedRun}
        onClose={() => {
          setSelectedRun(null);
          setSelectedFunction(null);
          setSelectedLogs([]);
          setSelectedLogsStatus("idle");
          setSelectedLogsError(null);
          setSelectedTransformationJob(null);
          setSelectedTransformation(null);
        }}
        functionName={
          functionNameMap[selectedRun?.functionId ?? ""] ??
          selectedRun?.functionId ??
          t("processing.unknown")
        }
        selectedFunction={selectedFunction}
        selectedRun={selectedRun as Record<string, unknown> | null}
        selectedLogs={selectedLogs}
        selectedLogsStatus={selectedLogsStatus}
        selectedLogsError={selectedLogsError}
        formatTimeFields={formatTimeFields}
      />
      <TransformationRunModal
        open={!!selectedTransformationJob}
        onClose={() => {
          setSelectedTransformationJob(null);
          setSelectedTransformation(null);
        }}
        transformationName={
          transformationNameMap[String(selectedTransformationJob?.transformationId ?? "")] ??
          t("processing.unknown.transformation")
        }
        selectedTransformation={selectedTransformation}
        selectedTransformationJob={selectedTransformationJob as Record<string, unknown> | null}
        formatTimeFields={formatTimeFields}
      />
      <WorkflowRunModal
        open={!!selectedWorkflowExecution}
        onClose={() => {
          setSelectedWorkflowExecution(null);
          setSelectedLogs([]);
          setSelectedLogsStatus("idle");
          setSelectedLogsError(null);
          resetWorkflowDetails();
        }}
        workflowExternalId={selectedWorkflowExecution?.workflowExternalId ?? ""}
        selectedExecution={selectedWorkflowExecution as Record<string, unknown> | null}
        workflowDetails={workflowDetails}
        workflowDetailsStatus={workflowDetailsStatus}
        workflowDetailsError={workflowDetailsError}
        formatTimeFields={formatTimeFields}
      />
      <ExtractionPipelineRunModal
        open={!!selectedExtractorRun}
        onClose={() => {
          setSelectedExtractorRun(null);
          setSelectedExtractorConfig(null);
        }}
        pipelineName={selectedExtractorConfig?.name ?? selectedExtractorRun?.externalId ?? ""}
        selectedPipeline={selectedExtractorConfig}
        selectedRun={selectedExtractorRun as Record<string, unknown> | null}
        formatTimeFields={formatTimeFields}
      />
      <Loader
        open={showLoader}
        onClose={() => {
          setShowLoader(false);
          setLoaderDismissed(true);
        }}
        title={t("processing.loader.title")}
      />
      <ProcessingHelpModal open={showHelp} onClose={() => setShowHelp(false)} />
      <ProcessingHeatmapHelpModal
        open={showHeatmapHelp}
        onClose={() => setShowHeatmapHelp(false)}
      />
    </section>
  );
}
