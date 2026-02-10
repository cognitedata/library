import { useEffect, useRef } from "react";
import { select } from "d3-selection";
import { formatDuration, formatIso, formatZoned } from "@/shared/time-utils";
import type {
  ExtPipeConfigSummary,
  ExtPipeRunSummary,
  FunctionRunSummary,
  TransformationJobSummary,
  WorkflowExecutionSummary,
} from "./types";
import { useI18n } from "@/shared/i18n";

type ProcessingChartProps = {
  windowRange: { start: number; end: number } | null;
  parallelSeries: Array<{ time: number; count: number }>;
  transformationSeries: Array<{ time: number; count: number }>;
  workflowSeries: Array<{ time: number; count: number }>;
  extractorSeries: Array<{ time: number; count: number }>;
  maxParallel: number;
  maxTransformParallel: number;
  maxWorkflowParallel: number;
  maxExtractorParallel: number;
  runs: FunctionRunSummary[];
  getRunDuration: (run: FunctionRunSummary) => number | null;
  getRadius: (run: FunctionRunSummary) => number;
  getColor: (run: FunctionRunSummary) => string;
  transformationJobs: TransformationJobSummary[];
  getTransformationDuration: (job: TransformationJobSummary) => number | null;
  getTransformationRadius: (job: TransformationJobSummary) => number;
  getTransformationColor: (job: TransformationJobSummary) => string;
  workflowExecutions: WorkflowExecutionSummary[];
  getWorkflowDuration: (execution: WorkflowExecutionSummary) => number | null;
  getWorkflowRadius: (execution: WorkflowExecutionSummary) => number;
  getWorkflowColor: (execution: WorkflowExecutionSummary) => string;
  extractorRuns: Array<ExtPipeRunSummary & { externalId: string }>;
  extractorConfigMap: Record<string, ExtPipeConfigSummary>;
  getExtractorRadius: (run: ExtPipeRunSummary) => number;
  getExtractorColor: (run: ExtPipeRunSummary) => string;
  onRunClick: (run: FunctionRunSummary) => void;
  onTransformationClick: (job: TransformationJobSummary) => void;
  onWorkflowClick: (execution: WorkflowExecutionSummary) => void;
  onExtractorClick: (run: ExtPipeRunSummary & { externalId: string }) => void;
  functionNameMap: Record<string, string>;
};

export function ProcessingChart({
  windowRange,
  parallelSeries,
  transformationSeries,
  workflowSeries,
  extractorSeries,
  maxParallel,
  maxTransformParallel,
  maxWorkflowParallel,
  maxExtractorParallel,
  runs,
  getRunDuration,
  getRadius,
  getColor,
  transformationJobs,
  getTransformationDuration,
  getTransformationRadius,
  getTransformationColor,
  workflowExecutions,
  getWorkflowDuration,
  getWorkflowRadius,
  getWorkflowColor,
  extractorRuns,
  extractorConfigMap,
  getExtractorRadius,
  getExtractorColor,
  onRunClick,
  onTransformationClick,
  onWorkflowClick,
  onExtractorClick,
  functionNameMap,
}: ProcessingChartProps) {
  const { t } = useI18n();
  const svgRef = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!svgRef.current || !windowRange) return;
    const width = 1000;
    const height = 720;
    const padding = 24;
    const lineHeight = 140;
    const dotRadius = 5;
    const dotPadding = 2;
    const lineMax = Math.max(
      maxParallel,
      maxTransformParallel,
      maxWorkflowParallel,
      maxExtractorParallel,
      1
    );

    const startTime = windowRange.start;
    const endTime = windowRange.end;
    const timeSpan = endTime - startTime || 1;
    const toX = (time: number) =>
      padding + ((time - startTime) / timeSpan) * (width - padding * 2);

    const root = select(svgRef.current);
    root.selectAll("*").remove();
    root.attr("width", width).attr("height", height).attr("viewBox", `0 0 ${width} ${height}`);

    root
      .append("rect")
      .attr("x", 0)
      .attr("y", 0)
      .attr("width", width)
      .attr("height", height)
      .attr("fill", "white")
      .attr("stroke", "#e2e8f0");

    const axisGroup = root.append("g");
    const axisColor = "#94a3b8";
    const textColor = "#64748b";
    const axisBottom = height - padding;
    const functionBandTop = padding;
    const functionBandBottom = functionBandTop + lineHeight;
    const bandGap = 12;
    const yAxisTop = functionBandTop;
    const yAxisBottom = functionBandBottom;
    const axisLeft = padding;

    axisGroup
      .append("line")
      .attr("x1", axisLeft)
      .attr("x2", width - padding)
      .attr("y1", axisBottom)
      .attr("y2", axisBottom)
      .attr("stroke", axisColor);

    axisGroup
      .append("line")
      .attr("x1", axisLeft)
      .attr("x2", axisLeft)
      .attr("y1", yAxisTop)
      .attr("y2", yAxisBottom)
      .attr("stroke", axisColor);

    const tickCount = 5;
    for (let i = 0; i <= tickCount; i += 1) {
      const t = startTime + (timeSpan * i) / tickCount;
      const x = toX(t);
      axisGroup
        .append("line")
        .attr("x1", x)
        .attr("x2", x)
        .attr("y1", axisBottom)
        .attr("y2", axisBottom + 6)
        .attr("stroke", axisColor);
      axisGroup
        .append("text")
        .attr("x", x)
        .attr("y", axisBottom + 18)
        .attr("text-anchor", "middle")
        .attr("fill", textColor)
        .attr("font-size", 10)
        .text(formatZoned(t, "UTC"));
    }

    const yMax = lineMax;
    const yTickCount = Math.max(1, Math.min(tickCount, yMax));
    for (let i = 0; i <= yTickCount; i += 1) {
      const value = Math.round((yMax * i) / yTickCount);
      const y = yAxisBottom - ((yAxisBottom - yAxisTop) * i) / yTickCount;
      axisGroup
        .append("line")
        .attr("x1", axisLeft - 6)
        .attr("x2", axisLeft)
        .attr("y1", y)
        .attr("y2", y)
        .attr("stroke", axisColor);
      axisGroup
        .append("text")
        .attr("x", axisLeft - 10)
        .attr("y", y + 3)
        .attr("text-anchor", "end")
        .attr("fill", textColor)
        .attr("font-size", 10)
        .text(String(value));
    }

    const lineGroup = root.append("g").attr("transform", `translate(0, ${functionBandTop})`);
    if (parallelSeries.length > 0) {
      const linePath = parallelSeries
        .map((point, index) => {
          const x = toX(point.time);
          const y = lineHeight - (point.count / lineMax) * lineHeight;
          return `${index === 0 ? "M" : "L"}${x},${y}`;
        })
        .join(" ");
      lineGroup
        .append("path")
        .attr("d", linePath)
        .attr("fill", "none")
        .attr("stroke", "#2563eb")
        .attr("stroke-width", 2);
    }
    if (transformationSeries.length > 0) {
      const linePath = transformationSeries
        .map((point, index) => {
          const x = toX(point.time);
          const y = lineHeight - (point.count / lineMax) * lineHeight;
          return `${index === 0 ? "M" : "L"}${x},${y}`;
        })
        .join(" ");
      lineGroup
        .append("path")
        .attr("d", linePath)
        .attr("fill", "none")
        .attr("stroke", "#f97316")
        .attr("stroke-width", 2);
    }
    if (workflowSeries.length > 0) {
      const linePath = workflowSeries
        .map((point, index) => {
          const x = toX(point.time);
          const y = lineHeight - (point.count / lineMax) * lineHeight;
          return `${index === 0 ? "M" : "L"}${x},${y}`;
        })
        .join(" ");
      lineGroup
        .append("path")
        .attr("d", linePath)
        .attr("fill", "none")
        .attr("stroke", "#a855f7")
        .attr("stroke-width", 2);
    }
    if (extractorSeries.length > 0) {
      const linePath = extractorSeries
        .map((point, index) => {
          const x = toX(point.time);
          const y = lineHeight - (point.count / lineMax) * lineHeight;
          return `${index === 0 ? "M" : "L"}${x},${y}`;
        })
        .join(" ");
      lineGroup
        .append("path")
        .attr("d", linePath)
        .attr("fill", "none")
        .attr("stroke", "#06b6d4")
        .attr("stroke-width", 2);
    }

    const dotAreaTop = functionBandBottom + bandGap;
    const dotAreaBottom = height - padding;
    const dotAreaHeight = dotAreaBottom - dotAreaTop;
    const bandHeight = dotAreaHeight / 4;
    const functionBaseline = dotAreaTop + bandHeight * 0.65;
    const transformationBaseline = dotAreaTop + bandHeight * 1.65;
    const workflowBaseline = dotAreaTop + bandHeight * 2.65;
    const extractorBaseline = dotAreaTop + bandHeight * 3.65;
    const dividerGroup = root.append("g");
    dividerGroup
      .append("rect")
      .attr("x", axisLeft)
      .attr("y", dotAreaTop)
      .attr("width", width - padding - axisLeft)
      .attr("height", bandHeight)
      .attr("fill", "#f8fafc");
    dividerGroup
      .append("rect")
      .attr("x", axisLeft)
      .attr("y", dotAreaTop + bandHeight)
      .attr("width", width - padding - axisLeft)
      .attr("height", bandHeight)
      .attr("fill", "#f1f5f9");
    dividerGroup
      .append("rect")
      .attr("x", axisLeft)
      .attr("y", dotAreaTop + bandHeight * 2)
      .attr("width", width - padding - axisLeft)
      .attr("height", bandHeight)
      .attr("fill", "#f8fafc");
    dividerGroup
      .append("rect")
      .attr("x", axisLeft)
      .attr("y", dotAreaTop + bandHeight * 3)
      .attr("width", width - padding - axisLeft)
      .attr("height", bandHeight)
      .attr("fill", "#f1f5f9");
    dividerGroup
      .append("line")
      .attr("x1", axisLeft)
      .attr("x2", width - padding)
      .attr("y1", dotAreaTop + bandHeight)
      .attr("y2", dotAreaTop + bandHeight)
      .attr("stroke", "#cbd5f5");
    dividerGroup
      .append("line")
      .attr("x1", axisLeft)
      .attr("x2", width - padding)
      .attr("y1", dotAreaTop + bandHeight * 2)
      .attr("y2", dotAreaTop + bandHeight * 2)
      .attr("stroke", "#cbd5f5");
    dividerGroup
      .append("line")
      .attr("x1", axisLeft)
      .attr("x2", width - padding)
      .attr("y1", dotAreaTop + bandHeight * 3)
      .attr("y2", dotAreaTop + bandHeight * 3)
      .attr("stroke", "#cbd5f5");
    dividerGroup
      .append("text")
      .attr("x", axisLeft + 6)
      .attr("y", dotAreaTop + 14)
      .attr("fill", "#475569")
      .attr("font-size", 11)
      .text(t("processing.legend.functions"));
    dividerGroup
      .append("text")
      .attr("x", axisLeft + 6)
      .attr("y", dotAreaTop + bandHeight + 14)
      .attr("fill", "#475569")
      .attr("font-size", 11)
      .text(t("processing.legend.transformations"));
    dividerGroup
      .append("text")
      .attr("x", axisLeft + 6)
      .attr("y", dotAreaTop + bandHeight * 2 + 14)
      .attr("fill", "#475569")
      .attr("font-size", 11)
      .text(t("processing.legend.workflows"));
    dividerGroup
      .append("text")
      .attr("x", axisLeft + 6)
      .attr("y", dotAreaTop + bandHeight * 3 + 14)
      .attr("fill", "#475569")
      .attr("font-size", 11)
      .text(t("processing.legend.extractors"));

    const dotGroup = root.append("g");
    const sortedRuns = [...runs].sort((a, b) => {
      const aStart = a.startTime ?? a.createdTime ?? 0;
      const bStart = b.startTime ?? b.createdTime ?? 0;
      return aStart - bStart;
    });

    const placed: Array<{ x: number; y: number }> = [];
    const transformPlaced: Array<{ x: number; y: number }> = [];
    const workflowPlaced: Array<{ x: number; y: number }> = [];
    const extractorPlaced: Array<{ x: number; y: number }> = [];
    const step = dotRadius * 2 + dotPadding;
    const functionMinY = dotAreaTop + dotRadius;
    const transformationMinY = dotAreaTop + bandHeight + dotRadius;
    const workflowMinY = dotAreaTop + bandHeight * 2 + dotRadius;
    const extractorMinY = dotAreaTop + bandHeight * 3 + dotRadius;

    const dots = sortedRuns
      .map((run) => {
        const start = run.startTime ?? run.createdTime;
        if (start == null) return null;
        const x = toX(start);
        let y = functionBaseline;
        while (
          placed.some(
            (point) => Math.abs(point.x - x) < dotRadius * 2 && Math.abs(point.y - y) < dotRadius * 2
          ) &&
          y - step > functionMinY
        ) {
          y -= step;
        }
        placed.push({ x, y });
        return { x, y, run };
      })
      .filter((value): value is { x: number; y: number; run: FunctionRunSummary } => !!value);

    const transformDots = [...transformationJobs]
      .sort((a, b) => (a.startedTime ?? 0) - (b.startedTime ?? 0))
      .map((job) => {
        const start = job.startedTime;
        if (start == null) return null;
        const x = toX(start);
        let y = transformationBaseline;
        while (
          transformPlaced.some(
            (point) => Math.abs(point.x - x) < dotRadius * 2 && Math.abs(point.y - y) < dotRadius * 2
          ) &&
          y - step > transformationMinY
        ) {
          y -= step;
        }
        transformPlaced.push({ x, y });
        return { x, y, job };
      })
      .filter(
        (value): value is { x: number; y: number; job: TransformationJobSummary } => !!value
      );

    dotGroup
      .selectAll("circle")
      .data(dots)
      .enter()
      .append("circle")
      .attr("cx", (d) => d.x)
      .attr("cy", (d) => d.y)
      .attr("r", (d) => getRadius(d.run))
      .attr("fill", (d) => getColor(d.run))
      .attr("stroke", "#0f172a")
      .attr("stroke-width", 0.5)
      .on("click", (_event, d) => onRunClick(d.run))
      .append("title")
      .text(
        (d) => {
          const functionId = d.run.functionId ?? t("processing.unknown");
          const name = functionNameMap[functionId] ?? functionId;
          return `${name} · ${d.run.status ?? t("processing.unknown")} · ${formatDuration(
            getRunDuration(d.run)
          )}`;
        }
      );

    dotGroup
      .selectAll("circle.transform")
      .data(transformDots)
      .enter()
      .append("circle")
      .attr("class", "transform")
      .attr("cx", (d) => d.x)
      .attr("cy", (d) => d.y)
      .attr("r", (d) => getTransformationRadius(d.job))
      .attr("fill", (d) => getTransformationColor(d.job))
      .attr("stroke", "#0f172a")
      .attr("stroke-width", 0.5)
      .on("click", (_event, d) => onTransformationClick(d.job))
      .append("title")
      .text((d) => {
        return `${d.job.status ?? t("processing.unknown")} · ${formatDuration(
          getTransformationDuration(d.job)
        )}`;
      });

    const workflowDots = [...workflowExecutions]
      .sort((a, b) => (a.startTime ?? a.createdTime) - (b.startTime ?? b.createdTime))
      .map((execution) => {
        const start = execution.startTime ?? execution.createdTime;
        if (start == null) return null;
        const x = toX(start);
        let y = workflowBaseline;
        while (
          workflowPlaced.some(
            (point) => Math.abs(point.x - x) < dotRadius * 2 && Math.abs(point.y - y) < dotRadius * 2
          ) &&
          y - step > workflowMinY
        ) {
          y -= step;
        }
        workflowPlaced.push({ x, y });
        return { x, y, execution };
      })
      .filter(
        (value): value is { x: number; y: number; execution: WorkflowExecutionSummary } => !!value
      );

    dotGroup
      .selectAll("circle.workflow")
      .data(workflowDots)
      .enter()
      .append("circle")
      .attr("class", "workflow")
      .attr("cx", (d) => d.x)
      .attr("cy", (d) => d.y)
      .attr("r", (d) => getWorkflowRadius(d.execution))
      .attr("fill", (d) => getWorkflowColor(d.execution))
      .attr("stroke", "#0f172a")
      .attr("stroke-width", 0.5)
      .on("click", (_event, d) => onWorkflowClick(d.execution))
      .append("title")
      .text((d) => {
        return `${d.execution.status} · ${formatDuration(getWorkflowDuration(d.execution))}`;
      });

    const extractorDots = [...extractorRuns]
      .sort((a, b) => a.createdTime - b.createdTime)
      .map((run) => {
        const x = toX(run.createdTime);
        let y = extractorBaseline;
        while (
          extractorPlaced.some(
            (point) => Math.abs(point.x - x) < dotRadius * 2 && Math.abs(point.y - y) < dotRadius * 2
          ) &&
          y - step > extractorMinY
        ) {
          y -= step;
        }
        extractorPlaced.push({ x, y });
        return { x, y, run };
      });

    dotGroup
      .selectAll("circle.extractor")
      .data(extractorDots)
      .enter()
      .append("circle")
      .attr("class", "extractor")
      .attr("cx", (d) => d.x)
      .attr("cy", (d) => d.y)
      .attr("r", (d) => getExtractorRadius(d.run))
      .attr("fill", (d) => getExtractorColor(d.run))
      .attr("stroke", "#0f172a")
      .attr("stroke-width", 0.5)
      .on("click", (_event, d) => onExtractorClick(d.run))
      .append("title")
      .text((d) => {
        const name = extractorConfigMap[d.run.externalId]?.name ?? d.run.externalId;
        const endTime = d.run.endTime;
        const timeLabel =
          endTime && endTime !== d.run.createdTime
            ? `${formatIso(d.run.createdTime)} → ${formatIso(endTime)}`
            : formatIso(d.run.createdTime);
        return `${name} · ${d.run.status} · ${timeLabel}`;
      });
  }, [
    windowRange,
    parallelSeries,
    transformationSeries,
    workflowSeries,
    extractorSeries,
    maxParallel,
    maxTransformParallel,
    maxWorkflowParallel,
    maxExtractorParallel,
    runs,
    getRunDuration,
    getRadius,
    getColor,
    transformationJobs,
    getTransformationDuration,
    getTransformationRadius,
    getTransformationColor,
    workflowExecutions,
    getWorkflowDuration,
    getWorkflowRadius,
    getWorkflowColor,
    extractorRuns,
    extractorConfigMap,
    getExtractorRadius,
    getExtractorColor,
    onRunClick,
    onTransformationClick,
    onWorkflowClick,
    onExtractorClick,
    functionNameMap,
    t,
  ]);

  return <svg ref={svgRef} />;
}
