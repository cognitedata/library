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
import { ResourceHealthPanel } from "./ResourceHealthPanel";
import { isFailed } from "./uptime";
import type { ResourceKind } from "./thresholds";
import type { ResourceReport } from "./types";

type Props = { onBack: () => void };

const THRESHOLD_PCT = 75;

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
  const [reports, setReports] = useState<Record<ResourceKind, ResourceReport>>({
    extractionPipelines: EMPTY_REPORT("Extraction pipeline"),
    workflows: EMPTY_REPORT("Workflow"),
    transformations: EMPTY_REPORT("Transformation"),
    functions: EMPTY_REPORT("Function"),
  });
  const [computedAt, setComputedAt] = useState<number | null>(null);
  const [loadAllKinds, setLoadAllKinds] = useState<Set<ResourceKind>>(new Set());

  const signalRef = useRef<{ cancelled: boolean }>({ cancelled: false });

  const sampleMode = (endMs - startMs) > 3_600_000;

  const run = useCallback(async () => {
    if (isSdkLoading) return;
    signalRef.current.cancelled = true;
    const signal = { cancelled: false };
    signalRef.current = signal;

    setStatus("loading");
    setLoadAllKinds(new Set());
    const opts = { sdk, datasetId: selectedDatasetId, startMs, endMs, signal, sampleMode };

    setLoadingKind("Extraction pipelines");
    const ep = await fetchExtractionPipelineHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, extractionPipelines: ep }));

    setLoadingKind("Workflows");
    const wf = await fetchWorkflowHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, workflows: wf }));

    setLoadingKind("Transformations");
    const tr = await fetchTransformationHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, transformations: tr }));

    setLoadingKind("Functions");
    const fn = await fetchFunctionHealth({ ...opts, thresholdPct: THRESHOLD_PCT });
    if (signal.cancelled) return;
    setReports((prev) => ({ ...prev, functions: fn }));

    setComputedAt(Date.now());
    setLoadingKind(null);
    setStatus("success");
  }, [isSdkLoading, sdk, selectedDatasetId, startMs, endMs, sampleMode]);

  useEffect(() => {
    void run();
    return () => {
      signalRef.current.cancelled = true;
    };
  }, [run]);

  const loadAllForKind = useCallback(async (kind: ResourceKind) => {
    if (isSdkLoading) return;
    const signal = { cancelled: false };
    const opts = { sdk, datasetId: selectedDatasetId, startMs, endMs, signal, sampleMode: false };

    setLoadAllKinds((prev) => new Set(prev).add(kind));

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
  }, [isSdkLoading, sdk, selectedDatasetId, startMs, endMs]);

  const aggregatedErrors = useMemo(() => {
    const all = [
      ...reports.extractionPipelines.errors,
      ...reports.workflows.errors,
      ...reports.transformations.errors,
      ...reports.functions.errors,
    ];
    return all
      .filter((e) => isFailed(e.status) || e.status === "error")
      .sort((a, b) => (b.timeMs ?? 0) - (a.timeMs ?? 0))
      .slice(0, 25);
  }, [reports]);

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
            onClick={() => void run()}
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

      <ResourceHealthPanel
        report={reports.extractionPipelines}
        thresholdPct={THRESHOLD_PCT}
        loading={status === "loading" && !reports.extractionPipelines.resources.length}
        onLoadAll={reports.extractionPipelines.sampling?.isSampled && !loadAllKinds.has("extractionPipelines")
          ? () => void loadAllForKind("extractionPipelines") : undefined}
      />
      <ResourceHealthPanel
        report={reports.workflows}
        thresholdPct={THRESHOLD_PCT}
        loading={status === "loading" && !reports.workflows.resources.length}
        onLoadAll={reports.workflows.sampling?.isSampled && !loadAllKinds.has("workflows")
          ? () => void loadAllForKind("workflows") : undefined}
      />
      <ResourceHealthPanel
        report={reports.transformations}
        thresholdPct={THRESHOLD_PCT}
        loading={status === "loading" && !reports.transformations.resources.length}
        onLoadAll={reports.transformations.sampling?.isSampled && !loadAllKinds.has("transformations")
          ? () => void loadAllForKind("transformations") : undefined}
      />
      <ResourceHealthPanel
        report={reports.functions}
        thresholdPct={THRESHOLD_PCT}
        loading={status === "loading" && !reports.functions.resources.length}
        onLoadAll={reports.functions.sampling?.isSampled && !loadAllKinds.has("functions")
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
                <span className="font-medium text-slate-800">{err.resource}</span>
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
      />
    </section>
  );
}
