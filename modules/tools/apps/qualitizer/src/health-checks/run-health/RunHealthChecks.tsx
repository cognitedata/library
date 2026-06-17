import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { DatasetFilterProvider, useDatasetFilter } from "@/shared/dataset-filter-context";
import { Loader } from "@/shared/Loader";
import { formatIso } from "@/shared/time-utils";
import {
  TimeRangeProvider,
  useTimeRange,
  type TimeRangePreset,
} from "@/shared/time-range-context";
import type { LoadState } from "../types";
import {
  fetchExtractionPipelineHealth,
  fetchFunctionHealth,
  fetchTransformationHealth,
  fetchWorkflowHealth,
} from "./fetchers";
import { RunHealthHelpModal } from "./RunHealthHelpModal";
import { ResourceHealthPanel } from "./ResourceHealthPanel";
import { classifyHealth, isFailed, uptimePercentage } from "./uptime";
import type { ResourceKind } from "./thresholds";
import type { ResourceReport } from "./types";

type Props = { onBack: () => void };

const THRESHOLD_PCT = 75;
const TOTAL_LOAD_STEPS = 4;

const EMPTY_REPORT = (kindLabel: ResourceReport["kindLabel"]): ResourceReport => ({
  kindLabel,
  resources: [],
  summary: { total: 0, success: 0, warning: 0, critical: 0, noRuns: 0, aggregateUptime: 100 },
  errors: [],
  error: null,
});

export function RunHealthChecks(props: Props) {
  return (
    <TimeRangeProvider>
      <DatasetFilterProvider>
        <RunHealthChecksInner {...props} />
      </DatasetFilterProvider>
    </TimeRangeProvider>
  );
}

function RunHealthChecksInner({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { startMs, endMs, range, setRange } = useTimeRange();
  const { selectedDatasetId, datasets, setSelectedDatasetId, isLoading: isDatasetsLoading } = useDatasetFilter();

  const [status, setStatus] = useState<LoadState>("idle");
  const [loadingKind, setLoadingKind] = useState<string | null>(null);
  const [loadingStep, setLoadingStep] = useState<{ current: number; total: number } | null>(null);
  const [loadingContext, setLoadingContext] = useState<string | null>(null);
  const [reports, setReports] = useState<Record<ResourceKind, ResourceReport>>({
    extractionPipelines: EMPTY_REPORT("Extraction pipeline"),
    workflows: EMPTY_REPORT("Workflow"),
    transformations: EMPTY_REPORT("Transformation"),
    functions: EMPTY_REPORT("Function"),
  });
  const [computedAt, setComputedAt] = useState<number | null>(null);
  const [loadAllKinds, setLoadAllKinds] = useState<Set<ResourceKind>>(new Set());
  const [showHelp, setShowHelp] = useState(false);
  const hasLoadedOnce = useRef(false);
  const previousRangeRef = useRef<{ startMs: number; endMs: number }>({ startMs, endMs });

  const signalRef = useRef<{ cancelled: boolean }>({ cancelled: false });

  const sampleMode = (endMs - startMs) > 3_600_000;

  const visibleReports = useMemo(() => {
    if (selectedDatasetId == null) return reports;

    const filterReport = (report: ResourceReport): ResourceReport => {
      const resources = report.resources.filter(
        (resource) => resource.datasetId == null || resource.datasetId === selectedDatasetId
      );
      const summary = resources.reduce(
        (acc, resource) => {
          const cls = classifyHealth(resource.runsInWindow, resource.uptimePercentage, THRESHOLD_PCT);
          if (cls === "success") acc.success += 1;
          else if (cls === "warning") acc.warning += 1;
          else if (cls === "critical") acc.critical += 1;
          else acc.noRuns += 1;
          acc.totalSuccess += resource.successful;
          acc.totalFailed += resource.failed;
          return acc;
        },
        { success: 0, warning: 0, critical: 0, noRuns: 0, totalSuccess: 0, totalFailed: 0 }
      );
      const aggregateUptime = uptimePercentage(summary.totalSuccess, summary.totalFailed);
      const allowedResourceLabels = new Set(resources.map((resource) => `${report.kindLabel}: ${resource.name}`));
      const errors = report.errors.filter(
        (error) => !error.resource.startsWith(`${report.kindLabel}:`) || allowedResourceLabels.has(error.resource)
      );
      return {
        ...report,
        resources,
        summary: {
          total: resources.length,
          success: summary.success,
          warning: summary.warning,
          critical: summary.critical,
          noRuns: summary.noRuns,
          aggregateUptime,
        },
        errors,
      };
    };

    return {
      extractionPipelines: filterReport(reports.extractionPipelines),
      workflows: filterReport(reports.workflows),
      transformations: filterReport(reports.transformations),
      functions: filterReport(reports.functions),
    };
  }, [reports, selectedDatasetId]);

  const run = useCallback(async (
    context: "initial" | "refresh" | "range" = "refresh",
    clearReports = false
  ) => {
    if (isSdkLoading) return;
    signalRef.current.cancelled = true;
    const signal = { cancelled: false };
    signalRef.current = signal;

    setStatus("loading");
    setLoadingContext(
      context === "initial"
        ? "Loading run health"
        : context === "range"
            ? "Applying time range"
            : "Refreshing run health"
    );
    setLoadingStep({ current: 0, total: TOTAL_LOAD_STEPS });
    if (clearReports) {
      setReports({
        extractionPipelines: EMPTY_REPORT("Extraction pipeline"),
        workflows: EMPTY_REPORT("Workflow"),
        transformations: EMPTY_REPORT("Transformation"),
        functions: EMPTY_REPORT("Function"),
      });
      setComputedAt(null);
      setLoadAllKinds(new Set());
    }
    const opts = { sdk, datasetId: null, startMs, endMs, signal, sampleMode };

    setLoadingKind("Extraction pipelines");
    setLoadingStep({ current: 1, total: TOTAL_LOAD_STEPS });
    const ep = await fetchExtractionPipelineHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, extractionPipelines: ep }));

    setLoadingKind("Workflows");
    setLoadingStep({ current: 2, total: TOTAL_LOAD_STEPS });
    const wf = await fetchWorkflowHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, workflows: wf }));

    setLoadingKind("Transformations");
    setLoadingStep({ current: 3, total: TOTAL_LOAD_STEPS });
    const tr = await fetchTransformationHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, transformations: tr }));

    setLoadingKind("Functions");
    setLoadingStep({ current: 4, total: TOTAL_LOAD_STEPS });
    const fn = await fetchFunctionHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, functions: fn }));

    setComputedAt(Date.now());
    setLoadingKind(null);
    setLoadingStep(null);
    setLoadingContext(null);
    setStatus("success");
    hasLoadedOnce.current = true;
  }, [isSdkLoading, sdk, startMs, endMs, sampleMode]);

  useEffect(() => {
    if (!hasLoadedOnce.current) {
      void run("initial", true);
      previousRangeRef.current = { startMs, endMs };
    } else {
      const rangeChanged =
        previousRangeRef.current.startMs !== startMs ||
        previousRangeRef.current.endMs !== endMs;
      previousRangeRef.current = { startMs, endMs };
      if (rangeChanged) {
        void run("range", true);
      }
    }
    return () => {
      signalRef.current.cancelled = true;
    };
  }, [run, startMs, endMs]);

  const loadAllForKind = useCallback(async (kind: ResourceKind) => {
    if (isSdkLoading) return;
    const signal = { cancelled: false };
    const opts = { sdk, datasetId: null, startMs, endMs, signal, sampleMode: false };

    setLoadAllKinds((prev) => new Set(prev).add(kind));
    const kindLabel =
      kind === "extractionPipelines"
        ? "extraction pipelines"
        : kind === "workflows"
          ? "workflows"
          : kind === "transformations"
            ? "transformations"
            : "functions";
    setLoadingContext(`Loading all ${kindLabel}`);
    setLoadingKind(
      kind === "extractionPipelines"
        ? "Extraction pipelines"
        : kind === "workflows"
          ? "Workflows"
          : kind === "transformations"
            ? "Transformations"
            : "Functions"
    );

    let report: ResourceReport;
    switch (kind) {
      case "extractionPipelines":
        report = await fetchExtractionPipelineHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
        break;
      case "workflows":
        report = await fetchWorkflowHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
        break;
      case "transformations":
        report = await fetchTransformationHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
        break;
      case "functions":
        report = await fetchFunctionHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
        break;
    }
    if (!signal.cancelled) {
      setReports((prev) => ({ ...prev, [kind]: report }));
    }
    setLoadingKind(null);
    setLoadingContext(null);
  }, [isSdkLoading, sdk, startMs, endMs]);

  const aggregatedErrors = useMemo(() => {
    const all = [
      ...visibleReports.extractionPipelines.errors,
      ...visibleReports.workflows.errors,
      ...visibleReports.transformations.errors,
      ...visibleReports.functions.errors,
    ];
    return all
      .filter((e) => isFailed(e.status) || e.status === "error")
      .sort((a, b) => (b.timeMs ?? 0) - (a.timeMs ?? 0))
      .slice(0, 25);
  }, [visibleReports]);

  const failureResourceUrlByLabel = useMemo(() => {
    const map = new Map<string, string>();
    const allReports = [
      visibleReports.extractionPipelines,
      visibleReports.workflows,
      visibleReports.transformations,
      visibleReports.functions,
    ];
    for (const report of allReports) {
      for (const resource of report.resources) {
        if (!resource.fusionUrl) continue;
        map.set(`${report.kindLabel}: ${resource.name}`, resource.fusionUrl);
      }
    }
    return map;
  }, [visibleReports]);

  const presets: TimeRangePreset[] = ["1h", "6h", "12h", "24h", "7d"];

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">Run Health</h2>
          <p className="text-sm text-slate-500">
            Uptime and failure counts for extraction pipelines, workflows, transformations, and functions.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            className="cursor-pointer rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
            onClick={() => setShowHelp(true)}
          >
            What does this mean?
          </button>
          <button
            type="button"
            className="cursor-pointer rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
            onClick={() => void run("refresh", true)}
            disabled={status === "loading"}
          >
            {status === "loading" ? "Refreshing…" : "Refresh"}
          </button>
          <button
            type="button"
            className="cursor-pointer shrink-0 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onBack}
          >
            Back to checks
          </button>
        </div>
      </header>

      <div className="flex flex-wrap items-center gap-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
        <label className="flex items-center gap-2">
          <span className="text-slate-400">Range</span>
          <select
            className="rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700"
            value={range.kind === "custom" ? "custom" : range.kind}
            onChange={(e) => {
              const next = e.target.value as TimeRangePreset | "custom";
              if (next === "custom") return;
              setRange({ kind: next });
            }}
          >
            {presets.map((preset) => (
              <option key={preset} value={preset}>{preset}</option>
            ))}
          </select>
        </label>
        <span className="text-slate-300">|</span>
        <label className="flex items-center gap-2">
          <span className="text-slate-400">Dataset</span>
          <select
            className="rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700"
            value={selectedDatasetId ?? ""}
            onChange={(e) => {
              const value = e.target.value;
              setSelectedDatasetId(value === "" ? null : Number(value));
            }}
            disabled={isDatasetsLoading}
          >
            <option value="">All datasets</option>
            {datasets.map((ds) => (
              <option key={ds.id} value={ds.id}>
                {ds.name ?? ds.externalId ?? String(ds.id)}
              </option>
            ))}
          </select>
        </label>
        {computedAt ? (
          <>
            <span className="text-slate-300">|</span>
            <span>Computed at {formatIso(computedAt)}</span>
          </>
        ) : null}
      </div>
      {status === "loading" ? (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
          <div className="font-medium">
            {loadingContext ?? "Refreshing run health…"}
          </div>
          <div className="mt-1 text-amber-800">
            {loadingStep
              ? `Step ${loadingStep.current} of ${loadingStep.total}${loadingKind ? ` · ${loadingKind}` : ""}`
              : (loadingKind ?? "Preparing request…")}
          </div>
        </div>
      ) : null}

      <ResourceHealthPanel
        thresholdPct={THRESHOLD_PCT}
        report={visibleReports.extractionPipelines}
        loading={status === "loading" && !visibleReports.extractionPipelines.resources.length}
        refreshing={status === "loading" && visibleReports.extractionPipelines.resources.length > 0}
        onLoadAll={visibleReports.extractionPipelines.sampling?.isSampled && !loadAllKinds.has("extractionPipelines")
          ? () => void loadAllForKind("extractionPipelines") : undefined}
      />
      <ResourceHealthPanel
        thresholdPct={THRESHOLD_PCT}
        report={visibleReports.workflows}
        loading={status === "loading" && !visibleReports.workflows.resources.length}
        refreshing={status === "loading" && visibleReports.workflows.resources.length > 0}
        onLoadAll={visibleReports.workflows.sampling?.isSampled && !loadAllKinds.has("workflows")
          ? () => void loadAllForKind("workflows") : undefined}
      />
      <ResourceHealthPanel
        thresholdPct={THRESHOLD_PCT}
        report={visibleReports.transformations}
        loading={status === "loading" && !visibleReports.transformations.resources.length}
        refreshing={status === "loading" && visibleReports.transformations.resources.length > 0}
        onLoadAll={visibleReports.transformations.sampling?.isSampled && !loadAllKinds.has("transformations")
          ? () => void loadAllForKind("transformations") : undefined}
      />
      <ResourceHealthPanel
        thresholdPct={THRESHOLD_PCT}
        report={visibleReports.functions}
        loading={status === "loading" && !visibleReports.functions.resources.length}
        refreshing={status === "loading" && visibleReports.functions.resources.length > 0}
        onLoadAll={visibleReports.functions.sampling?.isSampled && !loadAllKinds.has("functions")
          ? () => void loadAllForKind("functions") : undefined}
      />

      <div className="rounded-md border border-slate-200 bg-white p-4">
        <div className="mb-2 text-sm font-semibold text-slate-900">Recent failures</div>
        {aggregatedErrors.length === 0 ? (
          <div className="text-xs text-slate-500">No failed runs in the current window.</div>
        ) : (
          <ul className="space-y-1 text-xs">
            {aggregatedErrors.map((err, idx) => (
              <li key={idx} className="flex flex-wrap items-start gap-2">
                <span className="rounded-sm bg-red-100 px-2 py-0.5 font-medium text-red-700">
                  {err.status.charAt(0).toUpperCase() + err.status.slice(1).toLowerCase()}
                </span>
                <span className="font-mono text-slate-600">
                  {err.timeMs ? formatIso(err.timeMs) : "—"}
                </span>
                {failureResourceUrlByLabel.get(err.resource) ? (
                  <a
                    href={failureResourceUrlByLabel.get(err.resource)}
                    target="_blank"
                    rel="noreferrer"
                    className="font-medium text-blue-700 underline decoration-dotted hover:text-blue-900"
                  >
                    {err.resource}
                  </a>
                ) : (
                  <span className="font-medium text-slate-800">{err.resource}</span>
                )}
                {err.message ? <span className="text-slate-700">{err.message}</span> : null}
              </li>
            ))}
          </ul>
        )}
      </div>

      <Loader
        open={status === "loading"}
        onClose={() => { /* informational only */ }}
        title={loadingKind ? `Loading ${loadingKind}…` : "Running run-health checks…"}
        progressDetails={
          <div className="space-y-1">
            <div className="font-medium text-slate-800">{loadingContext ?? "Refreshing run health"}</div>
            <div className="text-xs text-slate-600">
              {loadingStep
                ? `Step ${loadingStep.current} of ${loadingStep.total}${loadingKind ? ` · ${loadingKind}` : ""}`
                : (loadingKind ?? "Preparing request…")}
            </div>
          </div>
        }
      />
      <RunHealthHelpModal open={showHelp} onClose={() => setShowHelp(false)} />
    </section>
  );
}
