import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { useDatasetFilter } from "@/shared/dataset-filter-context";
import { Loader } from "@/shared/Loader";
import { formatIso } from "@/shared/time-utils";
import { useTimeRange, formatRangeLabel } from "@/shared/time-range-context";
import type { LoadState } from "../types";
import {
  fetchExtractionPipelineHealth,
  fetchFunctionHealth,
  fetchTransformationHealth,
  fetchWorkflowHealth,
} from "./fetchers";
import { ResourceHealthPanel } from "./ResourceHealthPanel";
import { isFailed } from "./uptime";
import { DEFAULT_THRESHOLDS, loadThresholds, saveThresholds, type ResourceKind } from "./thresholds";
import type { ResourceReport } from "./types";

type Props = { onBack: () => void };

const EMPTY_REPORT = (kindLabel: ResourceReport["kindLabel"]): ResourceReport => ({
  kindLabel,
  resources: [],
  summary: { total: 0, healthy: 0, unhealthy: 0, noRuns: 0, aggregateUptime: 100 },
  errors: [],
  error: null,
});

export function RunHealthChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { startMs, endMs, range } = useTimeRange();
  const { selectedDatasetId, selectedDataset } = useDatasetFilter();

  const [thresholds, setThresholdsState] = useState(loadThresholds);
  const [showThresholds, setShowThresholds] = useState(false);

  const [status, setStatus] = useState<LoadState>("idle");
  const [reports, setReports] = useState<Record<ResourceKind, ResourceReport>>({
    extractionPipelines: EMPTY_REPORT("Extraction pipeline"),
    workflows: EMPTY_REPORT("Workflow"),
    transformations: EMPTY_REPORT("Transformation"),
    functions: EMPTY_REPORT("Function"),
  });
  const [computedAt, setComputedAt] = useState<number | null>(null);

  const signalRef = useRef<{ cancelled: boolean }>({ cancelled: false });

  const run = useCallback(async () => {
    if (isSdkLoading) return;
    signalRef.current.cancelled = true;
    const signal = { cancelled: false };
    signalRef.current = signal;

    setStatus("loading");
    const opts = { sdk, datasetId: selectedDatasetId, startMs, endMs, signal };

    const [ep, wf, tr, fn] = await Promise.all([
      fetchExtractionPipelineHealth({ ...opts, thresholdPct: thresholds.extractionPipelines }),
      fetchWorkflowHealth({ ...opts, thresholdPct: thresholds.workflows }),
      fetchTransformationHealth({ ...opts, thresholdPct: thresholds.transformations }),
      fetchFunctionHealth({ ...opts, thresholdPct: thresholds.functions }),
    ]);

    if (signal.cancelled) return;

    setReports({
      extractionPipelines: ep,
      workflows: wf,
      transformations: tr,
      functions: fn,
    });
    setComputedAt(Date.now());
    setStatus("success");
  }, [isSdkLoading, sdk, selectedDatasetId, startMs, endMs, thresholds]);

  useEffect(() => {
    void run();
    return () => {
      signalRef.current.cancelled = true;
    };
  }, [run]);

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

  const updateThreshold = (key: ResourceKind, value: number) => {
    const sanitized = Math.max(0, Math.min(100, Math.round(value)));
    const next = { ...thresholds, [key]: sanitized };
    setThresholdsState(next);
    saveThresholds(next);
  };

  const resetThresholds = () => {
    setThresholdsState({ ...DEFAULT_THRESHOLDS });
    saveThresholds({ ...DEFAULT_THRESHOLDS });
  };

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
            onClick={() => setShowThresholds((v) => !v)}
          >
            {showThresholds ? "Hide" : "Thresholds"}
          </button>
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

      <div className="flex flex-wrap items-center gap-2 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
        <span>Range: <span className="font-mono text-slate-800">{formatRangeLabel(range)}</span></span>
        <span>·</span>
        <span>
          Dataset:{" "}
          <span className="font-mono text-slate-800">
            {selectedDataset ? selectedDataset.name ?? selectedDataset.externalId ?? selectedDataset.id : "All"}
          </span>
        </span>
        {computedAt ? (
          <>
            <span>·</span>
            <span>Computed at {formatIso(computedAt)}</span>
          </>
        ) : null}
      </div>

      {showThresholds ? (
        <div className="rounded-md border border-slate-200 bg-white p-4">
          <div className="mb-2 text-sm font-semibold text-slate-900">Uptime thresholds</div>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            {(Object.keys(DEFAULT_THRESHOLDS) as ResourceKind[]).map((key) => (
              <label key={key} className="flex flex-col gap-1 text-xs text-slate-600">
                <span className="font-medium text-slate-800">{labelForKind(key)}</span>
                <input
                  type="number"
                  min={0}
                  max={100}
                  value={thresholds[key]}
                  onChange={(e) => updateThreshold(key, Number(e.target.value))}
                  className="w-24 rounded-md border border-slate-200 px-2 py-1 text-sm"
                />
              </label>
            ))}
          </div>
          <button
            type="button"
            className="mt-3 cursor-pointer rounded-md border border-slate-200 bg-white px-3 py-1 text-xs text-slate-700 hover:bg-slate-50"
            onClick={resetThresholds}
          >
            Reset to defaults
          </button>
        </div>
      ) : null}

      <ResourceHealthPanel
        report={reports.extractionPipelines}
        thresholdPct={thresholds.extractionPipelines}
        loading={status === "loading"}
      />
      <ResourceHealthPanel
        report={reports.workflows}
        thresholdPct={thresholds.workflows}
        loading={status === "loading"}
      />
      <ResourceHealthPanel
        report={reports.transformations}
        thresholdPct={thresholds.transformations}
        loading={status === "loading"}
      />
      <ResourceHealthPanel
        report={reports.functions}
        thresholdPct={thresholds.functions}
        loading={status === "loading"}
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
                  {err.status}
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
        title="Running run-health checks…"
      />
    </section>
  );
}

function labelForKind(kind: ResourceKind): string {
  switch (kind) {
    case "extractionPipelines":
      return "Extraction pipelines";
    case "workflows":
      return "Workflows";
    case "transformations":
      return "Transformations";
    case "functions":
      return "Functions";
  }
}
