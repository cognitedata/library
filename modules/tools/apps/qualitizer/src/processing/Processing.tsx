import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Loader } from "@/shared/Loader";
import { FunctionRunModal } from "./FunctionRunModal";
import { TransformationRunModal } from "./TransformationRunModal";
import { TransformationDebugModal } from "./TransformationDebugModal";
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
import { usePrivateMode } from "@/shared/PrivateModeContext";
import { ApiError } from "@/shared/ApiError";
import {
  formatTimeFields,
  formatUtcRangeCompact,
  formatZonedRangeCompact,
  getTimeZoneLabel,
  getUserTimeZone,
  toTimestamp,
} from "@/shared/time-utils";
import {
  getFunctionsPageUrl,
  getTransformationPreviewUrl,
  getWorkflowEditorUrl,
} from "@/shared/cdf-browser-url";
import {
  DEFAULT_PROCESSING_EXECUTION_CAP,
  FUNCTION_LIST_PAGE_SIZE,
  PROCESSING_DIAGRAM_SERIES,
  type ExtPipeRunSummary,
  type ProcessingDiagramSeries,
  type FunctionRunSummary,
  type LoadState,
  type ProcessingDataLoadProgress,
  type ProcessingRequestStats,
  type TransformationJobSummary,
  type WorkflowExecutionSummary,
} from "./types";
import { withTransientRetries } from "@/shared/transient-http-retry";
import {
  noteForbiddenFailure,
  processingRequestStats,
  processingWindowKey,
} from "./processing-request-stats";

function formatProcessingDataProgress(
  t: (key: string, params?: Record<string, string | number>) => string,
  p: ProcessingDataLoadProgress
): string {
  switch (p.kind) {
    case "functions_list":
      return t("processing.progress.functions.list", {
        count: p.loaded ?? 0,
        pages: p.pages ?? 1,
        pageSize: p.pageSize ?? FUNCTION_LIST_PAGE_SIZE,
      });
    case "functions_runs": {
      const total = p.total ?? 0;
      const current = p.current ?? 0;
      const remaining = Math.max(0, total - current);
      return t("processing.progress.functions.runs", { current, total, remaining });
    }
    case "transformations_list":
      return t("processing.progress.transformations.list");
    case "transformations_jobs": {
      const total = p.total ?? 0;
      const current = p.current ?? 0;
      const remaining = Math.max(0, total - current);
      return t("processing.progress.transformations.jobs", { current, total, remaining });
    }
    case "workflows_executions":
      return t("processing.progress.workflows.executions", { loaded: p.loaded ?? 0 });
    case "extractors_list":
      return t("processing.progress.extractors.list", { loaded: p.loaded ?? 0 });
    case "extractors_runs": {
      const total = p.total ?? 0;
      const current = p.current ?? 0;
      const remaining = Math.max(0, total - current);
      return t("processing.progress.extractors.runs", { current, total, remaining });
    }
    default:
      return "";
  }
}

function processingSeriesLegendLabel(
  t: (key: string, params?: Record<string, string | number>) => string,
  series: ProcessingDiagramSeries
): string {
  switch (series) {
    case "functions":
      return t("processing.legend.functions");
    case "transformations":
      return t("processing.legend.transformations");
    case "workflows":
      return t("processing.legend.workflows");
    case "extractors":
      return t("processing.legend.extractors");
  }
}

function formatProcessingBandCaption(
  t: (key: string, params?: Record<string, string | number>) => string,
  p: ProcessingDataLoadProgress
): string {
  switch (p.kind) {
    case "functions_list":
      return t("processing.progress.band.functions.list", { count: p.loaded ?? 0 });
    case "functions_runs":
      return t("processing.progress.band.functions.runs", {
        current: p.current ?? 0,
        total: p.total ?? 0,
      });
    case "transformations_list":
      return t("processing.progress.band.transformations.list");
    case "transformations_jobs":
      return t("processing.progress.band.transformations.jobs", {
        current: p.current ?? 0,
        total: p.total ?? 0,
      });
    case "workflows_executions":
      return t("processing.progress.band.workflows", { loaded: p.loaded ?? 0 });
    case "extractors_list":
      return t("processing.progress.band.extractors.list", { loaded: p.loaded ?? 0 });
    case "extractors_runs":
      return t("processing.progress.band.extractors.runs", {
        current: p.current ?? 0,
        total: p.total ?? 0,
      });
    default:
      return "";
  }
}

const hoursWindow = 1;
const bucketSeconds = 15;
const PROCESSING_EXTERNAL_ID_FILTER_MIN_CHARS = 3;
const PROCESSING_EXTERNAL_ID_FILTER_DEBOUNCE_MS = 350;

function effectiveProcessingExternalIdNeedle(raw: string): string {
  const q = raw.trim();
  return q.length >= PROCESSING_EXTERNAL_ID_FILTER_MIN_CHARS ? raw : "";
}

export function Processing() {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { t } = useI18n();
  const { isPrivateMode } = usePrivateMode();
  const privateCls = isPrivateMode ? "private-mask" : "";
  const [windowOffsetHours, setWindowOffsetHours] = useState(0);
  const [windowRange, setWindowRange] = useState<{ start: number; end: number } | null>(null);
  type ConcurrencyDiagramPhase =
    | "idle"
    | "functions"
    | "transformations"
    | "workflows"
    | "extractors"
    | "complete";
  const [concurrencyDiagramPhase, setConcurrencyDiagramPhase] =
    useState<ConcurrencyDiagramPhase>("idle");
  const [fetchGeneration, setFetchGeneration] = useState(0);
  const [uncappedSeries, setUncappedSeries] = useState<Set<ProcessingDiagramSeries>>(
    () => new Set()
  );
  const [truncatedReloadQueue, setTruncatedReloadQueue] = useState<
    ProcessingDiagramSeries[] | null
  >(null);
  const executionLimitFor = useCallback(
    (series: ProcessingDiagramSeries) =>
      uncappedSeries.has(series) ? null : DEFAULT_PROCESSING_EXECUTION_CAP,
    [uncappedSeries]
  );
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
  const [showTransformationDebug, setShowTransformationDebug] = useState(false);
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
  const [scheduleRequestStats, setScheduleRequestStats] = useState<ProcessingRequestStats | null>(
    null
  );
  type ScheduleEntryType = "function" | "transformation" | "workflow";
  type ScheduleEntry = { cron: string; name: string; type: ScheduleEntryType; id: string };
  const [scheduleEntries, setScheduleEntries] = useState<ScheduleEntry[]>([]);
  const [heatmapVisibleTypes, setHeatmapVisibleTypes] = useState({
    functions: true,
    transformations: true,
    workflows: true,
  });
  type HeatmapItem = { name: string; type: ScheduleEntryType; id: string };
  type HeatmapCell = { hour: number; minute: number; count: number; names: string[]; items: HeatmapItem[] };
  const [hoverCell, setHoverCell] = useState<HeatmapCell | null>(null);
  const [pinnedHeatmapCell, setPinnedHeatmapCell] = useState<HeatmapCell | null>(null);
  const lastHoverKeyRef = useRef<string | null>(null);
  const [nowUtc, setNowUtc] = useState(() => new Date());
  const [filterExternalId, setFilterExternalId] = useState("");
  const [debouncedFilterExternalId, setDebouncedFilterExternalId] = useState("");
  useEffect(() => {
    const id = setInterval(() => setNowUtc(new Date()), 60_000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (!filterExternalId.trim()) {
      setDebouncedFilterExternalId("");
      return;
    }
    const id = window.setTimeout(() => {
      setDebouncedFilterExternalId(filterExternalId);
    }, PROCESSING_EXTERNAL_ID_FILTER_DEBOUNCE_MS);
    return () => window.clearTimeout(id);
  }, [filterExternalId]);

  const effectiveExternalIdNeedle = useMemo(
    () => effectiveProcessingExternalIdNeedle(debouncedFilterExternalId),
    [debouncedFilterExternalId]
  );

  const filtersExternalIdPendingDebounce =
    filterExternalId.trim() !== "" && filterExternalId !== debouncedFilterExternalId;

  const filterExternalIdTooShort =
    filterExternalId.trim().length > 0 &&
    filterExternalId.trim().length < PROCESSING_EXTERNAL_ID_FILTER_MIN_CHARS;

  const activeSerialSeries = useMemo((): ProcessingDiagramSeries | null => {
    if (truncatedReloadQueue && truncatedReloadQueue.length > 0) {
      return truncatedReloadQueue[0];
    }
    if (concurrencyDiagramPhase === "idle" || concurrencyDiagramPhase === "complete") {
      return null;
    }
    return concurrencyDiagramPhase;
  }, [truncatedReloadQueue, concurrencyDiagramPhase]);

  const fetchConcurrencyDiagram = useMemo(
    () => ({
      functions: activeSerialSeries === "functions",
      transformations: activeSerialSeries === "transformations",
      workflows: activeSerialSeries === "workflows",
      extractors: activeSerialSeries === "extractors",
    }),
    [activeSerialSeries]
  );

  const isTruncatedReload = truncatedReloadQueue != null;

  const windowSessionKey = useMemo(
    () => processingWindowKey(windowRange) ?? "",
    [windowRange?.start, windowRange?.end]
  );

  const {
    status,
    executionsTruncated: functionExecutionsTruncated,
    functionsCatalogMayBeIncomplete,
    functionsCatalogCount,
    loadProgress: functionLoadProgress,
    requestStats: functionRequestStats,
    errorMessage,
    availabilityMessage,
    runs,
    functionNameMap,
    functionMetaMap,
    getRunDuration,
    getRadius,
    getColor,
  } = useFunctionData({
    isSdkLoading,
    sdk,
    windowRange,
    fetchEnabled: fetchConcurrencyDiagram.functions,
    windowSessionKey,
    fetchGeneration,
    refetchExecutionsOnly: isTruncatedReload,
    executionLimit: executionLimitFor("functions"),
  });

  const {
    transformationsStatus,
    executionsTruncated: transformationExecutionsTruncated,
    loadProgress: transformationLoadProgress,
    requestStats: transformationRequestStats,
    transformationsError,
    transformationJobsAll,
    transformationNameMap,
    transformationMetaMap,
    filteredTransformationJobs,
    getTransformationDuration,
    getTransformationRadius,
    getTransformationColor,
  } = useTransformationData({
    isSdkLoading,
    sdk,
    windowRange,
    fetchEnabled: fetchConcurrencyDiagram.transformations,
    windowSessionKey,
    fetchGeneration,
    refetchExecutionsOnly: isTruncatedReload,
    executionLimit: executionLimitFor("transformations"),
  });

  const {
    workflowsStatus,
    executionsTruncated: workflowExecutionsTruncated,
    loadProgress: workflowLoadProgress,
    requestStats: workflowRequestStats,
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
  } = useWorkflowData({
    isSdkLoading,
    sdk,
    windowRange,
    fetchEnabled: fetchConcurrencyDiagram.workflows,
    windowSessionKey,
    fetchGeneration,
    refetchExecutionsOnly: isTruncatedReload,
    executionLimit: executionLimitFor("workflows"),
  });

  const {
    extractorsStatus,
    executionsTruncated: extractorExecutionsTruncated,
    loadProgress: extractorLoadProgress,
    requestStats: extractorRequestStats,
    extractorsError,
    extractorConfigMap,
    filteredExtractorRuns,
    getExtractorRadius,
    getExtractorColor,
  } = useExtractionPipelineData({
    isSdkLoading,
    sdk,
    windowRange,
    fetchEnabled: fetchConcurrencyDiagram.extractors,
    windowSessionKey,
    fetchGeneration,
    refetchExecutionsOnly: isTruncatedReload,
    executionLimit: executionLimitFor("extractors"),
  });

  useEffect(() => {
    const terminal = (s: LoadState) => s === "success" || s === "error";

    if (truncatedReloadQueue && truncatedReloadQueue.length > 0) {
      const current = truncatedReloadQueue[0];
      if (activeSerialSeries !== current) return;
      const currentStatus =
        current === "functions"
          ? status
          : current === "transformations"
            ? transformationsStatus
            : current === "workflows"
              ? workflowsStatus
              : extractorsStatus;
      if (!terminal(currentStatus)) return;
      const rest = truncatedReloadQueue.slice(1);
      if (rest.length === 0) {
        console.log("[Processing] Load all executions: queue finished", { series: current });
        setTruncatedReloadQueue(null);
      } else {
        console.log("[Processing] Load all executions: advancing queue", {
          completed: current,
          next: rest[0],
          remaining: rest,
        });
        setTruncatedReloadQueue(rest);
        setFetchGeneration((g) => g + 1);
      }
      return;
    }

    if (
      concurrencyDiagramPhase === "functions" &&
      activeSerialSeries === "functions" &&
      terminal(status)
    ) {
      setFetchGeneration((g) => g + 1);
      setConcurrencyDiagramPhase("transformations");
      return;
    }
    if (
      concurrencyDiagramPhase === "transformations" &&
      activeSerialSeries === "transformations" &&
      terminal(transformationsStatus)
    ) {
      setFetchGeneration((g) => g + 1);
      setConcurrencyDiagramPhase("workflows");
      return;
    }
    if (
      concurrencyDiagramPhase === "workflows" &&
      activeSerialSeries === "workflows" &&
      terminal(workflowsStatus)
    ) {
      setFetchGeneration((g) => g + 1);
      setConcurrencyDiagramPhase("extractors");
      return;
    }
    if (
      concurrencyDiagramPhase === "extractors" &&
      activeSerialSeries === "extractors" &&
      terminal(extractorsStatus)
    ) {
      setConcurrencyDiagramPhase("complete");
    }
  }, [
    activeSerialSeries,
    concurrencyDiagramPhase,
    truncatedReloadQueue,
    status,
    transformationsStatus,
    workflowsStatus,
    extractorsStatus,
  ]);

  const displayRuns = useMemo(() => {
    const needle = effectiveExternalIdNeedle.trim().toLowerCase();
    if (!needle) return runs;
    return runs.filter((run) => {
      const fid = String(run.functionId ?? "");
      const name = String(functionNameMap[fid] ?? "");
      return fid.toLowerCase().includes(needle) || name.toLowerCase().includes(needle);
    });
  }, [runs, effectiveExternalIdNeedle, functionNameMap]);

  const displayTransformationJobs = useMemo(() => {
    const needle = effectiveExternalIdNeedle.trim().toLowerCase();
    if (!needle) return filteredTransformationJobs;
    return filteredTransformationJobs.filter((job) => {
      const tid = String(job.transformationId ?? "");
      const name = String(transformationNameMap[tid] ?? "");
      return tid.toLowerCase().includes(needle) || name.toLowerCase().includes(needle);
    });
  }, [filteredTransformationJobs, transformationNameMap, effectiveExternalIdNeedle]);

  const displayWorkflowExecutions = useMemo(() => {
    const needle = effectiveExternalIdNeedle.trim().toLowerCase();
    if (!needle) return filteredWorkflowExecutions;
    return filteredWorkflowExecutions.filter((ex) =>
      String(ex.workflowExternalId ?? "").toLowerCase().includes(needle)
    );
  }, [filteredWorkflowExecutions, effectiveExternalIdNeedle]);

  const displayExtractorRuns = useMemo(() => {
    const needle = effectiveExternalIdNeedle.trim().toLowerCase();
    if (!needle) return filteredExtractorRuns;
    return filteredExtractorRuns.filter((run) => {
      const id = String(run.externalId ?? "");
      const name = String(extractorConfigMap[id]?.name ?? "");
      return id.toLowerCase().includes(needle) || name.toLowerCase().includes(needle);
    });
  }, [filteredExtractorRuns, extractorConfigMap, effectiveExternalIdNeedle]);

  const filteredFailureDurationMs = useMemo(() => {
    const failureStatuses = ["failed", "failure", "timeout", "timed_out"];
    return displayRuns.reduce((total, run) => {
      const statusValue = run.status?.toLowerCase() ?? "";
      if (!failureStatuses.some((value) => statusValue.includes(value))) return total;
      const start = toTimestamp(run.startTime ?? run.createdTime);
      const end = toTimestamp(run.endTime ?? run.lastUpdatedTime);
      if (!start || !end || end <= start) return total;
      return total + (end - start);
    }, 0);
  }, [displayRuns]);

  const isProcessingLoading = truncatedReloadQueue
    ? (activeSerialSeries === "functions" &&
        (status === "loading" || functionLoadProgress != null)) ||
      (activeSerialSeries === "transformations" &&
        (transformationsStatus === "loading" || transformationLoadProgress != null)) ||
      (activeSerialSeries === "workflows" &&
        (workflowsStatus === "loading" || workflowLoadProgress != null)) ||
      (activeSerialSeries === "extractors" &&
        (extractorsStatus === "loading" || extractorLoadProgress != null))
    : status === "loading" ||
      transformationsStatus === "loading" ||
      workflowsStatus === "loading" ||
      extractorsStatus === "loading";

  const showExecutionSampleBanner =
    concurrencyDiagramPhase === "complete" &&
    truncatedReloadQueue == null &&
    (functionExecutionsTruncated ||
      transformationExecutionsTruncated ||
      workflowExecutionsTruncated ||
      extractorExecutionsTruncated);

  const showFunctionsCatalogBanner =
    concurrencyDiagramPhase === "complete" && functionsCatalogMayBeIncomplete;

  const truncatedReloadProgress = useMemo(() => {
    if (!truncatedReloadQueue?.length || !activeSerialSeries) return null;
    const progress =
      activeSerialSeries === "functions"
        ? functionLoadProgress
        : activeSerialSeries === "transformations"
          ? transformationLoadProgress
          : activeSerialSeries === "workflows"
            ? workflowLoadProgress
            : extractorLoadProgress;
    const seriesLabel = processingSeriesLegendLabel(t, activeSerialSeries);
    const detail = progress
      ? formatProcessingDataProgress(t, progress)
      : t("processing.executions.reloadingStarting", { series: seriesLabel });
    return t("processing.executions.reloadingActive", { series: seriesLabel, detail });
  }, [
    t,
    truncatedReloadQueue,
    activeSerialSeries,
    functionLoadProgress,
    transformationLoadProgress,
    workflowLoadProgress,
    extractorLoadProgress,
  ]);

  const truncatedReloadQueuedLabels = useMemo(() => {
    if (!truncatedReloadQueue || truncatedReloadQueue.length <= 1) return null;
    const rest = truncatedReloadQueue.slice(1);
    return rest.map((s) => processingSeriesLegendLabel(t, s)).join(", ");
  }, [t, truncatedReloadQueue]);

  const loaderProgressDetails = useMemo(() => {
    const lines: string[] = [];
    const wait = t("processing.bubbles.waiting");
    const activeIdx =
      activeSerialSeries != null
        ? PROCESSING_DIAGRAM_SERIES.indexOf(activeSerialSeries)
        : -1;
    const pushSeries = (
      series: ProcessingDiagramSeries,
      seriesStatus: LoadState,
      progress: ProcessingDataLoadProgress | null | undefined
    ) => {
      const isWaiting = PROCESSING_DIAGRAM_SERIES.indexOf(series) > activeIdx;
      const label = processingSeriesLegendLabel(t, series);
      if (isWaiting) {
        lines.push(`${label}: ${wait}`);
        return;
      }
      if (seriesStatus === "error") return;
      if (activeSerialSeries === series && progress) {
        lines.push(`${label}: ${formatProcessingDataProgress(t, progress)}`);
      } else if (activeSerialSeries === series && seriesStatus === "loading") {
        lines.push(`${label}: ${wait}`);
      }
    };
    pushSeries("functions", status, functionLoadProgress);
    pushSeries("transformations", transformationsStatus, transformationLoadProgress);
    pushSeries("workflows", workflowsStatus, workflowLoadProgress);
    pushSeries("extractors", extractorsStatus, extractorLoadProgress);
    if (lines.length === 0) return null;
    return (
      <>
        <p className="text-xs font-medium text-slate-600">{t("processing.progress.panelTitle")}</p>
        <ul className="mt-2 list-disc space-y-1.5 pl-4 text-xs leading-snug text-slate-800">
          {lines.map((line, i) => (
            <li key={i}>{line}</li>
          ))}
        </ul>
      </>
    );
  }, [
    t,
    activeSerialSeries,
    status,
    functionLoadProgress,
    transformationsStatus,
    transformationLoadProgress,
    workflowsStatus,
    workflowLoadProgress,
    extractorsStatus,
    extractorLoadProgress,
  ]);

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
    const currentHourStart = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        now.getUTCHours(),
        0,
        0,
        0
      )
    );
    if (windowOffsetHours === 0) {
      setWindowRange({ start: currentHourStart.getTime(), end: now.getTime() });
      return;
    }
    const endWindow = new Date(currentHourStart);
    endWindow.setUTCHours(endWindow.getUTCHours() - windowOffsetHours);
    const startWindow = new Date(endWindow);
    startWindow.setUTCHours(startWindow.getUTCHours() - hoursWindow);
    setWindowRange({ start: startWindow.getTime(), end: endWindow.getTime() });
  }, [isSdkLoading, windowOffsetHours]);

  useLayoutEffect(() => {
    if (isSdkLoading || !windowRange) {
      setConcurrencyDiagramPhase("idle");
      return;
    }
    setUncappedSeries(new Set());
    setTruncatedReloadQueue(null);
    setFetchGeneration((g) => g + 1);
    setConcurrencyDiagramPhase("functions");
  }, [isSdkLoading, windowRange?.start, windowRange?.end]);

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
      setScheduleRequestStats(null);
      setScheduleEntries([]);
      try {
        const entries: ScheduleEntry[] = [];
        let failedRequests = 0;
        let totalRequests = 0;
        const permissionsDenied = { current: false };

        totalRequests++;
        try {
          const functionSchedules = (await withTransientRetries(() =>
            sdk.post(`/api/v1/projects/${sdk.project}/functions/schedules/list`, {
              data: { limit: 1000 },
            })
          )) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of functionSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const fnId =
            (item.functionExternalId as string | undefined) ??
            (item.functionId as string | undefined) ??
            "";
          const name =
            (item.name as string | undefined) ?? (fnId || t("processing.heatmap.unknownFunction"));
          entries.push({ cron, name, type: "function", id: fnId });
        }
        } catch (e) {
          failedRequests++;
          noteForbiddenFailure(permissionsDenied, e);
        }

        totalRequests++;
        try {
          const transformationSchedules = (await withTransientRetries(() =>
            sdk.get(`/api/v1/projects/${sdk.project}/transformations/schedules`, {
              params: { limit: "1000" },
            })
          )) as { data?: { items?: Array<Record<string, unknown>> } };
        for (const item of transformationSchedules.data?.items ?? []) {
          const cron = readCron(item);
          if (!cron) continue;
          const txId =
            (item.transformationId as string | undefined) ??
            (item.transformationExternalId as string | undefined) ??
            "";
          const name =
            (item.name as string | undefined) ?? (txId || t("processing.heatmap.unknownTransformation"));
          entries.push({ cron, name, type: "transformation", id: txId });
        }
        } catch (e) {
          failedRequests++;
          noteForbiddenFailure(permissionsDenied, e);
        }

        let triggerCursor: string | undefined;
        do {
          totalRequests++;
          let workflowTriggers: {
            data?: { items?: Array<Record<string, unknown>>; nextCursor?: string };
          };
          try {
            workflowTriggers = (await withTransientRetries(() =>
              sdk.get(`/api/v1/projects/${sdk.project}/workflows/triggers`, {
                params: { limit: "1000", cursor: triggerCursor },
              })
            )) as { data?: { items?: Array<Record<string, unknown>>; nextCursor?: string } };
          } catch (e) {
            failedRequests++;
            noteForbiddenFailure(permissionsDenied, e);
            break;
          }
          for (const item of workflowTriggers.data?.items ?? []) {
            const cron = readCron(item);
            if (!cron) continue;
            const workflowId = item.workflowExternalId as string | undefined;
            const triggerId =
              (item.externalId as string | undefined) ??
              (item.id as string | undefined) ??
              "trigger";
            const name = workflowId ? `${workflowId} · ${triggerId}` : triggerId;
            entries.push({ cron, name, type: "workflow", id: workflowId ?? triggerId });
          }
          triggerCursor = workflowTriggers.data?.nextCursor;
        } while (triggerCursor);

        if (!cancelled) {
          setScheduleEntries(entries);
          setScheduleRequestStats(
            processingRequestStats(failedRequests, totalRequests, permissionsDenied.current)
          );
          if (entries.length === 0 && failedRequests === totalRequests && totalRequests > 0) {
            setScheduleError(
              permissionsDenied.current
                ? t("processing.permissions.heatmapError")
                : t("processing.heatmap.error")
            );
            setScheduleStatus("error");
          } else {
            setScheduleStatus("success");
          }
        }
      } catch (error) {
        if (!cancelled) {
          setScheduleRequestStats(null);
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
    const needle = effectiveExternalIdNeedle.trim().toLowerCase();
    const entriesForHeatmap = needle
      ? scheduleEntries.filter(
          (entry) =>
            String(entry.id).toLowerCase().includes(needle) ||
            String(entry.name).toLowerCase().includes(needle)
        )
      : scheduleEntries;
    const filtered = entriesForHeatmap.filter(
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
      Array.from({ length: 24 }, () => ({
        count: 0,
        names: [] as string[],
        items: [] as HeatmapItem[],
      }))
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
          counts[minute][hour].items.push({
            name: entry.name,
            type: entry.type,
            id: entry.id,
          });
        }
      }
    }
    return counts;
  }, [scheduleEntries, scheduleStatus, heatmapVisibleTypes, effectiveExternalIdNeedle]);

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

    for (const run of displayRuns) {
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
  }, [displayRuns, windowRange]);

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

    for (const job of displayTransformationJobs) {
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
  }, [displayTransformationJobs, windowRange]);

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

    for (const execution of displayWorkflowExecutions) {
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
  }, [displayWorkflowExecutions, windowRange]);

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

    for (const run of displayExtractorRuns) {
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
  }, [displayExtractorRuns, windowRange]);

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

  const partialStatsCombined = useMemo(() => {
    const segments: { label: string; stats: ProcessingRequestStats }[] = [];
    const push = (label: string, s: ProcessingRequestStats | null | undefined) => {
      if (s && s.failed > 0) segments.push({ label, stats: s });
    };
    push(t("processing.legend.functions"), functionRequestStats);
    push(t("processing.legend.transformations"), transformationRequestStats);
    push(t("processing.legend.workflows"), workflowRequestStats);
    push(t("processing.legend.extractors"), extractorRequestStats);
    push(t("processing.partial.schedulesLabel"), scheduleRequestStats);
    const failed = segments.reduce((acc, seg) => acc + seg.stats.failed, 0);
    const total = segments.reduce((acc, seg) => acc + seg.stats.total, 0);
    return { segments, failed, total };
  }, [
    t,
    functionRequestStats,
    transformationRequestStats,
    workflowRequestStats,
    extractorRequestStats,
    scheduleRequestStats,
  ]);

  const showPermissionsDeniedBanner = useMemo(() => {
    const { segments, failed, total } = partialStatsCombined;
    if (segments.length === 0 || total === 0 || failed === 0) return false;
    return (
      segments.every((seg) => seg.stats.permissionsDenied) && failed === total
    );
  }, [partialStatsCombined]);

  const permissionDeniedSegments = useMemo(
    () =>
      partialStatsCombined.segments.filter(
        (seg) => seg.stats.permissionsDenied && seg.stats.failed > 0
      ),
    [partialStatsCombined]
  );

  const showPartialDataBanner =
    partialStatsCombined.segments.length > 0 &&
    partialStatsCombined.total > 0 &&
    !showPermissionsDeniedBanner;

  const diagramConcurrencyUi = useMemo(() => {
    const p = concurrencyDiagramPhase;
    const showLoadProgressHeaders =
      activeSerialSeries != null || (p !== "complete" && p !== "idle");
    const wait = t("processing.bubbles.waiting");
    const err = t("processing.status.error");
    const empty = t("processing.bubbles.empty");

    const activeIdx =
      activeSerialSeries != null
        ? PROCESSING_DIAGRAM_SERIES.indexOf(activeSerialSeries)
        : p === "complete"
          ? PROCESSING_DIAGRAM_SERIES.length
          : p === "idle"
            ? -1
            : PROCESSING_DIAGRAM_SERIES.indexOf(p);

    const seriesWaiting = (series: ProcessingDiagramSeries) =>
      PROCESSING_DIAGRAM_SERIES.indexOf(series) > activeIdx;

    const fnWaiting = seriesWaiting("functions");
    const txWaiting = seriesWaiting("transformations");
    const wfWaiting = seriesWaiting("workflows");
    const exWaiting = seriesWaiting("extractors");

    const bandLabel = (
      series: ProcessingDiagramSeries,
      isWaiting: boolean,
      seriesStatus: LoadState,
      progress: ProcessingDataLoadProgress | null | undefined,
      hasData: boolean
    ) => {
      if (isWaiting) return wait;
      if (seriesStatus === "error") return err;
      if (activeSerialSeries === series && progress) {
        return formatProcessingBandCaption(t, progress);
      }
      if (!isWaiting && seriesStatus === "success" && !hasData) return empty;
      return "";
    };

    const fnHeader =
      showLoadProgressHeaders &&
      (fnWaiting ||
        status === "error" ||
        (activeSerialSeries === "functions" &&
          (status === "loading" || functionLoadProgress != null)));
    const txHeader =
      showLoadProgressHeaders &&
      (txWaiting ||
        transformationsStatus === "error" ||
        (activeSerialSeries === "transformations" &&
          (transformationsStatus === "loading" || transformationLoadProgress != null)));
    const wfHeader =
      showLoadProgressHeaders &&
      (wfWaiting ||
        workflowsStatus === "error" ||
        (activeSerialSeries === "workflows" &&
          (workflowsStatus === "loading" || workflowLoadProgress != null)));
    const exHeader =
      showLoadProgressHeaders &&
      (exWaiting ||
        extractorsStatus === "error" ||
        (activeSerialSeries === "extractors" &&
          (extractorsStatus === "loading" || extractorLoadProgress != null)));

    return {
      band: {
        functions: bandLabel(
          "functions",
          fnWaiting,
          status,
          functionLoadProgress,
          displayRuns.length > 0
        ),
        transformations: bandLabel(
          "transformations",
          txWaiting,
          transformationsStatus,
          transformationLoadProgress,
          displayTransformationJobs.length > 0
        ),
        workflows: bandLabel(
          "workflows",
          wfWaiting,
          workflowsStatus,
          workflowLoadProgress,
          displayWorkflowExecutions.length > 0
        ),
        extractors: bandLabel(
          "extractors",
          exWaiting,
          extractorsStatus,
          extractorLoadProgress,
          displayExtractorRuns.length > 0
        ),
      },
      waiting: { functions: fnWaiting, transformations: txWaiting, workflows: wfWaiting, extractors: exWaiting },
      headerRow: { functions: fnHeader, transformations: txHeader, workflows: wfHeader, extractors: exHeader },
      waitLabel: wait,
    };
  }, [
    t,
    activeSerialSeries,
    truncatedReloadQueue,
    concurrencyDiagramPhase,
    windowRange,
    status,
    functionLoadProgress,
    transformationsStatus,
    transformationLoadProgress,
    displayTransformationJobs.length,
    workflowsStatus,
    workflowLoadProgress,
    displayWorkflowExecutions.length,
    extractorsStatus,
    extractorLoadProgress,
    displayExtractorRuns.length,
    displayRuns.length,
  ]);

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
      const response = await withTransientRetries(() =>
        sdk.get<{
          items?: { message?: string }[];
        }>(`/api/v1/projects/${sdk.project}/functions/${run.functionId}/calls/${run.id}/logs`)
      );
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
      {windowRange ? (
        <div className="rounded-md border border-slate-200 bg-slate-50/80 p-3 text-xs text-slate-600">
          <div className="flex flex-col gap-1.5 sm:flex-row sm:items-end sm:justify-between sm:gap-3">
            <div className="min-w-0 flex-1">
              <label className="mb-1 block font-medium text-slate-700" htmlFor="processing-external-id-filter">
                {t("processing.filter.externalIdLabel")}
              </label>
              <input
                id="processing-external-id-filter"
                type="search"
                value={filterExternalId}
                onChange={(e) => setFilterExternalId(e.target.value)}
                placeholder={t("dataCatalog.filter.placeholder.substringMinChars", {
                  min: PROCESSING_EXTERNAL_ID_FILTER_MIN_CHARS,
                })}
                autoComplete="off"
                className="h-9 w-full max-w-md rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
              />
              <p className="mt-1 text-[10px] leading-snug text-slate-500">
                {t("processing.filter.externalIdLead")}
              </p>
            </div>
            {effectiveExternalIdNeedle.trim() ? (
              <button
                type="button"
                className="shrink-0 self-start rounded-md border border-slate-200 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 sm:self-center"
                onClick={() => {
                  setFilterExternalId("");
                  setDebouncedFilterExternalId("");
                }}
              >
                {t("dataCatalog.filter.clear")}
              </button>
            ) : null}
          </div>
          {filterExternalIdTooShort ? (
            <p className="mt-2 text-[10px] leading-snug text-slate-500">
              {t("dataCatalog.filter.minCharsHint", { min: PROCESSING_EXTERNAL_ID_FILTER_MIN_CHARS })}
            </p>
          ) : null}
          {filtersExternalIdPendingDebounce ? (
            <p className="mt-1 text-[11px] text-slate-500">{t("dataCatalog.filter.debouncePending")}</p>
          ) : null}
        </div>
      ) : null}
      {showPermissionsDeniedBanner ? (
        <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
          <p className="font-medium">{t("processing.permissions.title")}</p>
          <p className="mt-1 text-xs text-red-700">{t("processing.permissions.summary")}</p>
          <ul className="mt-2 list-disc space-y-0.5 pl-4 text-xs text-red-700">
            {permissionDeniedSegments.map((seg, i) => (
              <li key={`${seg.label}-${i}`}>
                {t("processing.permissions.detailLine", { label: seg.label })}
              </li>
            ))}
          </ul>
          <p className="mt-2 text-[11px] text-red-700">{t("processing.permissions.hint")}</p>
        </div>
      ) : null}
      {showPartialDataBanner ? (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-950">
          <p className="font-medium">{t("processing.partial.title")}</p>
          <p className="mt-1 text-xs text-amber-900">
            {t("processing.partial.summary", {
              failed: partialStatsCombined.failed,
              total: partialStatsCombined.total,
              percent:
                partialStatsCombined.total > 0
                  ? Math.round((100 * partialStatsCombined.failed) / partialStatsCombined.total)
                  : 0,
            })}
          </p>
          <ul className="mt-2 list-disc space-y-0.5 pl-4 text-xs text-amber-900">
            {partialStatsCombined.segments.map((seg, i) => (
              <li key={`${seg.label}-${i}`}>
                {seg.stats.permissionsDenied
                  ? t("processing.permissions.detailLine", { label: seg.label })
                  : t("processing.partial.detailLine", {
                      label: seg.label,
                      failed: seg.stats.failed,
                      total: seg.stats.total,
                      percent:
                        seg.stats.total > 0
                          ? Math.round((100 * seg.stats.failed) / seg.stats.total)
                          : 0,
                    })}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
      {showFunctionsCatalogBanner ? (
        <div className="mb-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-950">
          <p className="font-medium">{t("processing.functions.catalog.title")}</p>
          <p className="mt-1 text-xs text-amber-900">
            {t("processing.functions.catalog.body", {
              count: functionsCatalogCount,
              pageSize: FUNCTION_LIST_PAGE_SIZE,
            })}
          </p>
        </div>
      ) : null}
      {showExecutionSampleBanner ? (
        <div className="mb-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-950">
          <p className="font-medium">{t("processing.executions.sampleTitle")}</p>
          <p className="mt-1 text-xs text-amber-900">
            {t("processing.executions.sampleBody", { cap: DEFAULT_PROCESSING_EXECUTION_CAP })}
          </p>
          <ul className="mt-2 list-disc space-y-0.5 pl-4 text-xs text-amber-900">
            {functionExecutionsTruncated ? (
              <li>{t("processing.executions.sampleLineFunctions")}</li>
            ) : null}
            {transformationExecutionsTruncated ? (
              <li>{t("processing.executions.sampleLineTransformations")}</li>
            ) : null}
            {workflowExecutionsTruncated ? (
              <li>{t("processing.executions.sampleLineWorkflows")}</li>
            ) : null}
            {extractorExecutionsTruncated ? (
              <li>{t("processing.executions.sampleLineExtractors")}</li>
            ) : null}
          </ul>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <button
              type="button"
              disabled={truncatedReloadQueue != null}
              className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
              onClick={() => {
                if (truncatedReloadQueue) {
                  console.log("[Processing] Load all executions: ignored (reload already in progress)", {
                    queue: truncatedReloadQueue,
                  });
                  return;
                }
                const truncatedFlags = {
                  functions: functionExecutionsTruncated,
                  transformations: transformationExecutionsTruncated,
                  workflows: workflowExecutionsTruncated,
                  extractors: extractorExecutionsTruncated,
                };
                const queue = PROCESSING_DIAGRAM_SERIES.filter(
                  (series) => truncatedFlags[series]
                );
                if (queue.length === 0) {
                  console.log(
                    "[Processing] Load all executions: nothing to reload (no series marked truncated)",
                    {
                      truncatedFlags,
                      concurrencyDiagramPhase,
                      statuses: {
                        functions: status,
                        transformations: transformationsStatus,
                        workflows: workflowsStatus,
                        extractors: extractorsStatus,
                      },
                      executionCap: DEFAULT_PROCESSING_EXECUTION_CAP,
                    }
                  );
                  return;
                }
                console.log("[Processing] Load all executions: starting uncapped reload", {
                  queue,
                  truncatedFlags,
                  fetchGenerationNext: fetchGeneration + 1,
                  windowSessionKey,
                });
                setUncappedSeries((prev) => new Set([...prev, ...queue]));
                setTruncatedReloadQueue(queue);
                setFetchGeneration((g) => g + 1);
              }}
            >
              {t("processing.executions.loadAll")}
            </button>
          </div>
        </div>
      ) : null}
      {truncatedReloadQueue != null ? (
        <div className="mb-3 rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-sm text-sky-950">
          <p className="font-medium">{t("processing.executions.reloadingTitle")}</p>
          {truncatedReloadProgress ? (
            <p className="mt-1 text-xs text-sky-900">{truncatedReloadProgress}</p>
          ) : null}
          {truncatedReloadQueuedLabels ? (
            <p className="mt-1 text-xs text-sky-800">
              {t("processing.executions.reloadingQueued", { list: truncatedReloadQueuedLabels })}
            </p>
          ) : null}
        </div>
      ) : null}
      <Card>
        <CardHeader>
          <CardTitle>{t("processing.card.concurrency.title")}</CardTitle>
          <CardDescription className="space-y-1">
            <span className="block">
              {t("processing.card.concurrency.description", { bucketSeconds })}
            </span>
            <span className="block text-xs text-slate-500">
              {t("processing.card.concurrency.limits", {
                executionCap: DEFAULT_PROCESSING_EXECUTION_CAP,
                listPageSize: FUNCTION_LIST_PAGE_SIZE,
              })}
            </span>
          </CardDescription>
        </CardHeader>
        <CardContent>
          {diagramConcurrencyUi.headerRow.functions ||
          diagramConcurrencyUi.headerRow.transformations ||
          diagramConcurrencyUi.headerRow.workflows ||
          diagramConcurrencyUi.headerRow.extractors ? (
            <div className="mb-3 space-y-2 text-xs text-slate-600">
              {diagramConcurrencyUi.headerRow.functions ? (
                <div className="flex min-w-0 items-center gap-2">
                  <span className="w-28 shrink-0">{t("processing.legend.functions")}</span>
                  {status === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : diagramConcurrencyUi.waiting.functions ? (
                    <span className="min-w-0 flex-1 text-slate-500">{diagramConcurrencyUi.waitLabel}</span>
                  ) : activeSerialSeries === "functions" &&
                    (status === "loading" || functionLoadProgress != null) ? (
                    <>
                      <span className="h-2 w-40 shrink-0 rounded-sm bg-slate-200/80 animate-pulse" />
                      <span className="min-w-0 flex-1 truncate text-slate-500">
                        {functionLoadProgress
                          ? formatProcessingDataProgress(t, functionLoadProgress)
                          : diagramConcurrencyUi.waitLabel}
                      </span>
                    </>
                  ) : null}
                </div>
              ) : null}
              {diagramConcurrencyUi.headerRow.transformations ? (
                <div className="flex min-w-0 items-center gap-2">
                  <span className="w-28 shrink-0">{t("processing.legend.transformations")}</span>
                  {transformationsStatus === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : diagramConcurrencyUi.waiting.transformations ? (
                    <span className="min-w-0 flex-1 text-slate-500">{diagramConcurrencyUi.waitLabel}</span>
                  ) : activeSerialSeries === "transformations" &&
                    (transformationsStatus === "loading" ||
                      transformationLoadProgress != null) ? (
                    <>
                      <span className="h-2 w-40 shrink-0 rounded-sm bg-slate-200/80 animate-pulse" />
                      <span className="min-w-0 flex-1 truncate text-slate-500">
                        {transformationLoadProgress
                          ? formatProcessingDataProgress(t, transformationLoadProgress)
                          : diagramConcurrencyUi.waitLabel}
                      </span>
                    </>
                  ) : null}
                </div>
              ) : null}
              {diagramConcurrencyUi.headerRow.workflows ? (
                <div className="flex min-w-0 items-center gap-2">
                  <span className="w-28 shrink-0">{t("processing.legend.workflows")}</span>
                  {workflowsStatus === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : diagramConcurrencyUi.waiting.workflows ? (
                    <span className="min-w-0 flex-1 text-slate-500">{diagramConcurrencyUi.waitLabel}</span>
                  ) : activeSerialSeries === "workflows" &&
                    (workflowsStatus === "loading" || workflowLoadProgress != null) ? (
                    <>
                      <span className="h-2 w-40 shrink-0 rounded-sm bg-slate-200/80 animate-pulse" />
                      <span className="min-w-0 flex-1 truncate text-slate-500">
                        {workflowLoadProgress
                          ? formatProcessingDataProgress(t, workflowLoadProgress)
                          : diagramConcurrencyUi.waitLabel}
                      </span>
                    </>
                  ) : null}
                </div>
              ) : null}
              {diagramConcurrencyUi.headerRow.extractors ? (
                <div className="flex min-w-0 items-center gap-2">
                  <span className="w-28 shrink-0">{t("processing.legend.extractors")}</span>
                  {extractorsStatus === "error" ? (
                    <span className="text-red-600">{t("processing.status.error")}</span>
                  ) : diagramConcurrencyUi.waiting.extractors ? (
                    <span className="min-w-0 flex-1 text-slate-500">{diagramConcurrencyUi.waitLabel}</span>
                  ) : activeSerialSeries === "extractors" &&
                    (extractorsStatus === "loading" || extractorLoadProgress != null) ? (
                    <>
                      <span className="h-2 w-40 shrink-0 rounded-sm bg-slate-200/80 animate-pulse" />
                      <span className="min-w-0 flex-1 truncate text-slate-500">
                        {extractorLoadProgress
                          ? formatProcessingDataProgress(t, extractorLoadProgress)
                          : diagramConcurrencyUi.waitLabel}
                      </span>
                    </>
                  ) : null}
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
              <div className="flex flex-wrap items-center gap-3 text-xs text-slate-600">
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
                  {t("processing.legend.functions")} ({t("processing.stats.peak", { peak: maxParallel })})
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
                  {t("processing.legend.transformations")} ({t("processing.stats.peak", { peak: maxTransformParallel })})
                </button>
                <button
                  type="button"
                  className="rounded-md border border-slate-200 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
                  onClick={() => setShowTransformationDebug(true)}
                >
                  {t("processing.debug.transformations.open")}
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
                  {t("processing.legend.workflows")} ({t("processing.stats.peak", { peak: maxWorkflowParallel })})
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
                  {t("processing.legend.extractors")} ({t("processing.stats.peak", { peak: maxExtractorParallel })})
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
                  runs={displayRuns}
                  getRunDuration={getRunDuration}
                  getRadius={getRadius}
                  getColor={getColor}
                  transformationJobs={displayTransformationJobs}
                  getTransformationDuration={getTransformationDuration}
                  getTransformationRadius={getTransformationRadius}
                  getTransformationColor={getTransformationColor}
                  workflowExecutions={displayWorkflowExecutions}
                  getWorkflowDuration={getWorkflowDuration}
                  getWorkflowRadius={getWorkflowRadius}
                  getWorkflowColor={getWorkflowColor}
                  extractorRuns={displayExtractorRuns}
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
                  bandStatusLabels={diagramConcurrencyUi.band}
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
            <div className="text-sm text-slate-600">
              {functionLoadProgress
                ? formatProcessingDataProgress(t, functionLoadProgress)
                : t("processing.loading.stats")}
            </div>
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
                minutes: Math.round(filteredFailureDurationMs / 60000),
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
                            const items = cell?.items ?? [];
                            const hoverKey = `${hour}:${minute}:${count}:${names.length}`;
                            const isPinned =
                              pinnedHeatmapCell?.hour === hour &&
                              pinnedHeatmapCell?.minute === minute;
                            const isNow =
                              nowUtc.getUTCHours() === hour &&
                              nowUtc.getUTCMinutes() === minute;
                            return (
                              <div
                                key={`cell-${minute}-${hour}`}
                                className={`relative h-3 w-full cursor-pointer rounded-sm border ${
                                  isNow
                                    ? "border-slate-800 border-dashed ring-2 ring-inset ring-slate-950/70 z-[1]"
                                    : "border-slate-100"
                                }`}
                                style={{
                                  backgroundColor: getHeatColor(count),
                                  outline: isPinned ? "2px solid #3b82f6" : undefined,
                                  outlineOffset: -1,
                                }}
                                onMouseMove={() => {
                                  if (lastHoverKeyRef.current === hoverKey) return;
                                  lastHoverKeyRef.current = hoverKey;
                                  setHoverCell({ hour, minute, count, names, items });
                                }}
                                onClick={() => {
                                  const isUnpinning =
                                    pinnedHeatmapCell?.hour === hour &&
                                    pinnedHeatmapCell?.minute === minute;
                                  setPinnedHeatmapCell(
                                    isUnpinning ? null : { hour, minute, count, names, items }
                                  );
                                  if (!isUnpinning) {
                                    const currentHourUtc = new Date().getUTCHours();
                                    const offset =
                                      (currentHourUtc - hour - 1 + 24) % 24;
                                    setWindowOffsetHours(offset);
                                  }
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
                      <span className="flex items-center gap-1">
                        <span className="relative z-[1] h-3 w-3 rounded-sm border border-dashed border-slate-800 bg-sky-100 ring-2 ring-inset ring-slate-950/70" />
                        {t("processing.heatmap.legend.now")}
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
                            <ul className={`mt-1 flex-1 min-h-0 space-y-1 overflow-auto pl-1 ${privateCls}`}>
                              {(pinnedHeatmapCell ?? hoverCell)!.items.map((item, index) => (
                                <li key={`${item.name}-${index}`} className="flex items-center gap-1.5">
                                  <ScheduleTypeBadge type={item.type} />
                                  <ScheduleItemLink
                                    item={item}
                                    project={sdk.project}
                                  />
                                </li>
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
        contentClassName={privateCls}
      />
      <TransformationRunModal
        open={!!selectedTransformationJob}
        onClose={() => {
          setSelectedTransformationJob(null);
          setSelectedTransformation(null);
        }}
        project={sdk.project}
        transformationName={
          transformationNameMap[String(selectedTransformationJob?.transformationId ?? "")] ??
          t("processing.unknown.transformation")
        }
        selectedTransformation={selectedTransformation}
        selectedTransformationJob={selectedTransformationJob as Record<string, unknown> | null}
        formatTimeFields={formatTimeFields}
        contentClassName={privateCls}
      />
      <TransformationDebugModal
        open={showTransformationDebug}
        onClose={() => setShowTransformationDebug(false)}
        jobs={transformationJobsAll}
        filteredJobs={displayTransformationJobs}
        selectedWindow={windowRange}
        executionsTruncated={transformationExecutionsTruncated}
        isLoading={transformationsStatus === "loading"}
        loadingLabel={
          transformationsStatus === "loading"
            ? transformationLoadProgress
              ? formatProcessingDataProgress(t, transformationLoadProgress)
              : t("processing.loading.transformations")
            : null
        }
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
        contentClassName={privateCls}
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
        contentClassName={privateCls}
      />
      <Loader
        open={showLoader}
        onClose={() => {
          setShowLoader(false);
          setLoaderDismissed(true);
        }}
        title={t("processing.loader.title")}
        progressDetails={loaderProgressDetails}
      />
      <ProcessingHelpModal open={showHelp} onClose={() => setShowHelp(false)} />
      <ProcessingHeatmapHelpModal
        open={showHeatmapHelp}
        onClose={() => setShowHeatmapHelp(false)}
      />
    </section>
  );
}

const TYPE_BADGE_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  function: { bg: "bg-blue-100", text: "text-blue-700", label: "fn" },
  transformation: { bg: "bg-purple-100", text: "text-purple-700", label: "tx" },
  workflow: { bg: "bg-emerald-100", text: "text-emerald-700", label: "wf" },
};

function ScheduleTypeBadge({ type }: { type: string }) {
  const style = TYPE_BADGE_STYLES[type] ?? { bg: "bg-slate-100", text: "text-slate-600", label: type };
  return (
    <span
      className={`inline-flex shrink-0 items-center rounded px-1 py-0.5 text-[9px] font-semibold uppercase leading-none ${style.bg} ${style.text}`}
    >
      {style.label}
    </span>
  );
}

function ScheduleItemLink({
  item,
  project,
}: {
  item: { name: string; type: string; id: string };
  project: string;
}) {
  let url: string | null = null;
  if (item.type === "function") {
    url = getFunctionsPageUrl(project);
  } else if (item.type === "transformation" && item.id) {
    url = getTransformationPreviewUrl(project, item.id);
  } else if (item.type === "workflow" && item.id) {
    url = getWorkflowEditorUrl(project, item.id);
  }

  if (url) {
    return (
      <a
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        className="truncate text-blue-600 hover:underline"
        title={item.name}
      >
        {item.name}
      </a>
    );
  }
  return <span className="truncate" title={item.name}>{item.name}</span>;
}
