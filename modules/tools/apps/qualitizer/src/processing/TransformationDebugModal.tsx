import { useMemo } from "react";
import { line, curveMonotoneX } from "d3-shape";
import { formatIso, formatUtcRangeCompact } from "@/shared/time-utils";
import type { TransformationJobSummary } from "./types";
import { useI18n } from "@/shared/i18n";

type TransformationDebugModalProps = {
  open: boolean;
  onClose: () => void;
  jobs: TransformationJobSummary[];
  filteredJobs: TransformationJobSummary[];
  selectedWindow: { start: number; end: number } | null;
  executionsTruncated: boolean;
  isLoading?: boolean;
  loadingLabel?: string | null;
};

type HourBucket = {
  hourStart: number;
  count: number;
};

type ChartPoint = {
  time: number;
  value: number;
  startHour: number;
  endHour: number;
};

function hourFloorUtc(ms: number): number {
  const d = new Date(ms);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), 0, 0, 0);
}

export function TransformationDebugModal({
  open,
  onClose,
  jobs,
  filteredJobs,
  selectedWindow,
  executionsTruncated,
  isLoading = false,
  loadingLabel = null,
}: TransformationDebugModalProps) {
  const { t } = useI18n();
  const selectedHourStart = selectedWindow ? hourFloorUtc(selectedWindow.start) : null;

  const diagnostics = useMemo(() => {
    const withStart = jobs
      .map((job) => ({ job, startedTime: job.startedTime }))
      .filter(
        (entry): entry is { job: TransformationJobSummary; startedTime: number } =>
          typeof entry.startedTime === "number" && Number.isFinite(entry.startedTime)
      );
    const withoutStartCount = jobs.length - withStart.length;
    const uniqueTransformationIds = new Set(
      jobs.map((job) => String(job.transformationId ?? "")).filter((id) => id !== "")
    );
    const statusCounts = new Map<string, number>();
    for (const job of jobs) {
      const key = String(job.status ?? "unknown").toLowerCase() || "unknown";
      statusCounts.set(key, (statusCounts.get(key) ?? 0) + 1);
    }
    const sortedStatuses = Array.from(statusCounts.entries()).sort((a, b) => b[1] - a[1]);

    if (withStart.length === 0) {
      return {
        buckets: [] as HourBucket[],
        withStartCount: 0,
        withoutStartCount,
        uniqueTransformationCount: uniqueTransformationIds.size,
        sortedStatuses,
        rangeStart: null as number | null,
        rangeEnd: null as number | null,
      };
    }

    const hourCounts = new Map<number, number>();
    let minHour = Number.POSITIVE_INFINITY;
    let maxHour = Number.NEGATIVE_INFINITY;
    for (const entry of withStart) {
      const hour = hourFloorUtc(entry.startedTime);
      hourCounts.set(hour, (hourCounts.get(hour) ?? 0) + 1);
      if (hour < minHour) minHour = hour;
      if (hour > maxHour) maxHour = hour;
    }
    const buckets: HourBucket[] = [];
    for (let cursor = minHour; cursor <= maxHour; cursor += 60 * 60 * 1000) {
      buckets.push({ hourStart: cursor, count: hourCounts.get(cursor) ?? 0 });
    }
    return {
      buckets,
      withStartCount: withStart.length,
      withoutStartCount,
      uniqueTransformationCount: uniqueTransformationIds.size,
      sortedStatuses,
      rangeStart: minHour,
      rangeEnd: maxHour + 60 * 60 * 1000,
    };
  }, [jobs]);

  const graph = useMemo(() => {
    const buckets = diagnostics.buckets;
    if (buckets.length === 0) {
      return null;
    }
    const width = 1000;
    const height = 420;
    const marginLeft = 54;
    const marginRight = 20;
    const marginTop = 20;
    const marginBottom = 58;
    const plotWidth = width - marginLeft - marginRight;
    const plotHeight = height - marginTop - marginBottom;
    const rangeStart = buckets[0].hourStart;
    const rangeEnd = buckets[buckets.length - 1].hourStart + 60 * 60 * 1000;
    const totalHours = Math.max(1, (rangeEnd - rangeStart) / (60 * 60 * 1000));

    const targetPoints = 180;
    const binSize = Math.max(1, Math.ceil(buckets.length / targetPoints));
    const points: ChartPoint[] = [];
    for (let i = 0; i < buckets.length; i += binSize) {
      const slice = buckets.slice(i, i + binSize);
      const startHour = slice[0].hourStart;
      const endHour = slice[slice.length - 1].hourStart + 60 * 60 * 1000;
      const sum = slice.reduce((acc, bucket) => acc + bucket.count, 0);
      const value = sum / slice.length;
      points.push({
        time: startHour + (endHour - startHour) / 2,
        value,
        startHour,
        endHour,
      });
    }

    const xForTime = (time: number) =>
      marginLeft + ((time - rangeStart) / (rangeEnd - rangeStart || 1)) * plotWidth;
    const maxValue = Math.max(...points.map((point) => point.value), 1);
    const yForValue = (value: number) => marginTop + (1 - value / maxValue) * plotHeight;
    const selectedX =
      selectedHourStart == null || selectedHourStart < rangeStart || selectedHourStart >= rangeEnd
        ? null
        : xForTime(selectedHourStart + 30 * 60 * 1000);

    const linePath =
      line<ChartPoint>()
        .x((point) => xForTime(point.time))
        .y((point) => yForValue(point.value))
        .curve(curveMonotoneX)(points) ?? "";

    const areaPath = `${linePath} L ${xForTime(points[points.length - 1].time)} ${marginTop + plotHeight} L ${xForTime(points[0].time)} ${marginTop + plotHeight} Z`;

    const xTicks = 6;
    const yTicks = 4;

    return {
      width,
      height,
      marginLeft,
      marginTop,
      plotWidth,
      plotHeight,
      rangeStart,
      rangeEnd,
      totalHours,
      points,
      maxValue,
      selectedX,
      xForTime,
      yForValue,
      linePath,
      areaPath,
      xTicks,
      yTicks,
    };
  }, [diagnostics.buckets, selectedHourStart]);

  if (!open) return null;

  const topStatuses = diagnostics.sortedStatuses.slice(0, 6);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-6xl max-h-[88vh] overflow-y-auto rounded-lg bg-white p-6 shadow-xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">
              {t("processing.debug.transformations.title")}
            </h3>
            <p className="text-sm text-slate-500">{t("processing.debug.transformations.subtitle")}</p>
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_360px]">
          <div className="rounded-md border border-slate-200 bg-white p-3">
            {isLoading ? (
              <div className="mb-2 rounded-md border border-sky-200 bg-sky-50 px-2 py-1 text-xs text-sky-700">
                {loadingLabel ?? "Loading transformation jobs..."}
              </div>
            ) : null}
            <div className="mb-2 flex flex-wrap items-center gap-3 text-xs text-slate-600">
              {diagnostics.rangeStart != null && diagnostics.rangeEnd != null ? (
                <span className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1">
                  {t("processing.debug.transformations.range")}{" "}
                  {formatUtcRangeCompact(diagnostics.rangeStart, diagnostics.rangeEnd)}
                </span>
              ) : null}
              {selectedWindow ? (
                <span className="rounded-md border border-blue-200 bg-blue-50 px-2 py-1 text-blue-700">
                  {t("processing.debug.transformations.focusHour")}{" "}
                  {formatUtcRangeCompact(selectedWindow.start, selectedWindow.end)}
                </span>
              ) : null}
            </div>
            {graph ? (
              <div className="w-full">
                <svg
                  className="h-[420px] w-full"
                  viewBox={`0 0 ${graph.width} ${graph.height}`}
                  role="img"
                  aria-label="Transformation starts by hour"
                >
                  {graph.selectedX != null ? (
                    <rect
                      x={graph.selectedX - 4}
                      y={graph.marginTop}
                      width={8}
                      height={graph.plotHeight}
                      fill="#dbeafe"
                      stroke="#2563eb"
                      strokeWidth={1}
                      rx={2}
                    />
                  ) : null}
                  {Array.from({ length: graph.yTicks + 1 }, (_, tick) => {
                    const value = (graph.maxValue * (graph.yTicks - tick)) / graph.yTicks;
                    const y = graph.yForValue(value);
                    return (
                      <g key={`y-${tick}`}>
                        <line
                          x1={graph.marginLeft}
                          x2={graph.marginLeft + graph.plotWidth}
                          y1={y}
                          y2={y}
                          stroke="#e2e8f0"
                        />
                        <text x={graph.marginLeft - 8} y={y + 3} textAnchor="end" fontSize={10} fill="#64748b">
                          {value.toFixed(value % 1 === 0 ? 0 : 1)}
                        </text>
                      </g>
                    );
                  })}
                  {Array.from({ length: graph.xTicks + 1 }, (_, tick) => {
                    const time =
                      graph.rangeStart +
                      ((graph.rangeEnd - graph.rangeStart) * tick) / graph.xTicks;
                    const x = graph.xForTime(time);
                    const label = new Date(time).toISOString().slice(5, 13);
                    return (
                      <g key={`x-${tick}`}>
                        <line
                          x1={x}
                          x2={x}
                          y1={graph.marginTop}
                          y2={graph.marginTop + graph.plotHeight}
                          stroke="#f1f5f9"
                        />
                        <text x={x} y={graph.marginTop + graph.plotHeight + 16} textAnchor="middle" fontSize={10} fill="#64748b">
                          {label}
                        </text>
                      </g>
                    );
                  })}

                  <path d={graph.areaPath} fill="#fdba74" opacity={0.2} />
                  <path d={graph.linePath} fill="none" stroke="#ea580c" strokeWidth={2.5} />

                  {graph.points.map((point) => (
                    <circle
                      key={`${point.startHour}-${point.endHour}`}
                      cx={graph.xForTime(point.time)}
                      cy={graph.yForValue(point.value)}
                      r={2.5}
                      fill="#c2410c"
                    >
                      <title>{`${formatIso(point.startHour)} → ${formatIso(point.endHour)} · ${point.value.toFixed(2)} avg starts/hour`}</title>
                    </circle>
                  ))}

                  <line
                    x1={graph.marginLeft}
                    x2={graph.marginLeft + graph.plotWidth}
                    y1={graph.marginTop + graph.plotHeight}
                    y2={graph.marginTop + graph.plotHeight}
                    stroke="#94a3b8"
                  />
                  <line
                    x1={graph.marginLeft}
                    x2={graph.marginLeft}
                    y1={graph.marginTop}
                    y2={graph.marginTop + graph.plotHeight}
                    stroke="#94a3b8"
                  />
                </svg>
              </div>
            ) : (
              <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-600">
                {t("processing.debug.transformations.emptyGraph")}
              </div>
            )}
          </div>

          <div className="space-y-3">
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
              <DiagnosticCard
                label={t("processing.debug.transformations.totalJobs")}
                value={String(jobs.length)}
              />
              <DiagnosticCard
                label={t("processing.debug.transformations.jobsInWindow")}
                value={String(filteredJobs.length)}
              />
              <DiagnosticCard
                label={t("processing.debug.transformations.uniqueTransformations")}
                value={String(diagnostics.uniqueTransformationCount)}
              />
              <DiagnosticCard
                label={t("processing.debug.transformations.missingStartedTime")}
                value={String(diagnostics.withoutStartCount)}
              />
            </div>

            <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
              <div className="text-sm font-medium text-slate-900">
                {t("processing.debug.transformations.dataCoverage")}
              </div>
              <ul className="mt-2 space-y-1">
                <li>
                  {t("processing.debug.transformations.rowsWithStartTime")}: {diagnostics.withStartCount}
                </li>
                <li>
                  {t("processing.debug.transformations.rowsWithoutStartTime")}: {diagnostics.withoutStartCount}
                </li>
                <li>
                  {t("processing.debug.transformations.rangeStart")}:{" "}
                  {diagnostics.rangeStart != null ? formatIso(diagnostics.rangeStart) : "n/a"}
                </li>
                <li>
                  {t("processing.debug.transformations.rangeEnd")}:{" "}
                  {diagnostics.rangeEnd != null ? formatIso(diagnostics.rangeEnd) : "n/a"}
                </li>
                <li>
                  {t("processing.debug.transformations.executionCapApplied")}:{" "}
                  {executionsTruncated
                    ? t("processing.debug.transformations.yesPotentiallyTruncated")
                    : t("processing.debug.transformations.no")}
                </li>
              </ul>
            </div>

            <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
              <div className="text-sm font-medium text-slate-900">
                {t("processing.debug.transformations.statusBreakdown")}
              </div>
              {topStatuses.length > 0 ? (
                <ul className="mt-2 space-y-1">
                  {topStatuses.map(([status, count]) => (
                    <li key={status}>
                      <span className="font-medium">{status}</span>: {count}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="mt-2 text-slate-500">{t("processing.debug.transformations.noStatuses")}</p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function DiagnosticCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
      <div className="text-[11px] text-slate-500">{label}</div>
      <div className="mt-1 text-xl font-semibold text-slate-900">{value}</div>
    </div>
  );
}
