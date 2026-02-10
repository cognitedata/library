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
                <span className="flex items-center gap-2">
                  <span className="h-2 w-4 rounded-sm bg-blue-600" />
                  {t("processing.legend.functions")}
                </span>
                <span className="flex items-center gap-2">
                  <span className="h-2 w-4 rounded-sm bg-orange-500" />
                  {t("processing.legend.transformations")}
                </span>
                <span className="flex items-center gap-2">
                  <span className="h-2 w-4 rounded-sm bg-purple-500" />
                  {t("processing.legend.workflows")}
                </span>
                <span className="flex items-center gap-2">
                  <span className="h-2 w-4 rounded-sm bg-cyan-500" />
                  {t("processing.legend.extractors")}
                </span>
              </div>
              <div className="mt-4 overflow-x-auto">
                <ProcessingChart
                  windowRange={windowRange}
                  parallelSeries={parallelSeries}
                  transformationSeries={transformationSeries}
                  workflowSeries={workflowSeries}
                  extractorSeries={extractorSeries}
                  maxParallel={maxParallel}
                  maxTransformParallel={maxTransformParallel}
                  maxWorkflowParallel={maxWorkflowParallel}
                  maxExtractorParallel={maxExtractorParallel}
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
    </section>
  );
}
