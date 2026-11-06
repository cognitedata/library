import { useId, useState, type MouseEvent } from 'react';

import type { SnapshotPointDto } from '../../types';

import { formatCount } from './contextualisationCoverageLabels';
import { placementForDimensionTrendTooltip } from './dimensionTrendTooltipLayout';
import {
  contextualisedCountAtPercent,
  formatAxisDate,
  formatDimensionTrendPointChange,
  formatTooltipDate,
  pickAxisLabelIndexes,
} from './trendChartFormat';

type DimensionTrendAreaChartProps = {
  /** Stable id for tests and aria (dimension id or configured-metric key). */
  chartId: string;
  points: SnapshotPointDto[];
  totalIncludingExcluded: number;
  /** When totals vary by run (configured metrics), supply counts per point index. */
  populationAtPoint?: (index: number) => { contextualised: number; total: number };
  t: (key: string) => string;
};

const CHART_WIDTH = 280;
const CHART_HEIGHT = 80;

/** Plot aspect ratio matches the SVG viewBox so the chart scales with card width without distortion. */
export const DIMENSION_TREND_CHART_ASPECT_CLASS = 'aspect-[7/2]';

/** Empty-state frame height matches the previous fixed chart slot. */
export const DIMENSION_TREND_CHART_EMPTY_CLASS = 'flex h-24 w-full flex-col items-center justify-center';
const CHART_LEFT_MARGIN = 26;
const CHART_TOP_MARGIN = 4;
const CHART_BOTTOM_MARGIN = 2;
const GRID_PERCENTS = [0, 50, 100];
const MAX_X_AXIS_LABELS = 4;

const PLOT_WIDTH = CHART_WIDTH - CHART_LEFT_MARGIN;
const PLOT_HEIGHT = CHART_HEIGHT - CHART_TOP_MARGIN - CHART_BOTTOM_MARGIN;

/** Matches sky coverage badge (`text-decorative-foreground-sky` / `bg-decorative-background-sky`). */
const SERIES_CLASS = 'text-decorative-foreground-sky';

export function DimensionTrendAreaChart({
  chartId,
  points,
  totalIncludingExcluded,
  populationAtPoint,
  t,
}: DimensionTrendAreaChartProps) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const gradientId = useId().replace(/:/g, '');

  if (points.length === 0) {
    return (
      <div className={DIMENSION_TREND_CHART_EMPTY_CLASS} data-testid={`dimension-trend-chart-${chartId}`}>
        <p className="text-center text-xs text-muted-foreground">{t('overview.trend.noHistory')}</p>
      </div>
    );
  }

  const xFor = (index: number): number =>
    CHART_LEFT_MARGIN + (index / Math.max(points.length - 1, 1)) * PLOT_WIDTH;
  const yFor = (percent: number): number =>
    CHART_TOP_MARGIN + PLOT_HEIGHT - (Math.max(0, Math.min(100, percent)) / 100) * PLOT_HEIGHT;

  const baselineY = CHART_TOP_MARGIN + PLOT_HEIGHT;
  const lineCoords = points.map((point, index) => `${xFor(index)},${yFor(point.valuePercent)}`);
  const areaPath = [
    `M ${xFor(0)},${baselineY}`,
    ...points.map((point, index) => `L ${xFor(index)},${yFor(point.valuePercent)}`),
    `L ${xFor(points.length - 1)},${baselineY}`,
    'Z',
  ].join(' ');

  const hovered = hoveredIndex === null ? null : points[hoveredIndex];
  const axisLabelIndexes = pickAxisLabelIndexes(points.length, MAX_X_AXIS_LABELS);

  return (
    <div className="flex w-full min-w-0 flex-col" data-testid={`dimension-trend-chart-${chartId}`}>
      <div className={`relative w-full min-w-0 ${DIMENSION_TREND_CHART_ASPECT_CLASS}`}>
        <svg
          viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
          preserveAspectRatio="xMinYMid meet"
          className="absolute inset-0 block h-full w-full text-muted-foreground"
          role="img"
          aria-label={t('overview.trend.chartLabel')}
        >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1" className={SERIES_CLASS}>
            <stop offset="0%" stopColor="currentColor" stopOpacity={0.45} />
            <stop offset="100%" stopColor="currentColor" stopOpacity={0.08} />
          </linearGradient>
        </defs>

        {GRID_PERCENTS.map((percent) => (
          <g key={percent} pointerEvents="none">
            <line
              x1={CHART_LEFT_MARGIN}
              x2={CHART_WIDTH}
              y1={yFor(percent)}
              y2={yFor(percent)}
              stroke="currentColor"
              strokeOpacity={0.15}
              strokeWidth={1}
            />
            <text
              x={CHART_LEFT_MARGIN - 4}
              y={yFor(percent)}
              textAnchor="end"
              dominantBaseline="middle"
              className="fill-muted-foreground text-[8px]"
            >
              {percent}%
            </text>
          </g>
        ))}

        <path d={areaPath} fill={`url(#${gradientId})`} className={SERIES_CLASS} pointerEvents="none" />

        <polyline
          fill="none"
          stroke="currentColor"
          strokeWidth={1.5}
          strokeLinecap="round"
          strokeLinejoin="round"
          points={lineCoords.join(' ')}
          className={SERIES_CLASS}
          pointerEvents="none"
        />

        {hovered !== null ? (
          <line
            x1={xFor(hoveredIndex ?? 0)}
            x2={xFor(hoveredIndex ?? 0)}
            y1={CHART_TOP_MARGIN}
            y2={baselineY}
            stroke="currentColor"
            strokeDasharray="2,2"
            className="text-muted-foreground"
            pointerEvents="none"
          />
        ) : null}

        {points.map((point, index) => (
          <circle
            key={`${point.capturedAt}-${index}`}
            data-testid={`dimension-trend-point-${chartId}-${index}`}
            cx={xFor(index)}
            cy={yFor(point.valuePercent)}
            r={index === hoveredIndex ? 3 : 0}
            fill="currentColor"
            className={SERIES_CLASS}
            pointerEvents="none"
          />
        ))}

        <rect
          x={CHART_LEFT_MARGIN}
          y={CHART_TOP_MARGIN}
          width={PLOT_WIDTH}
          height={PLOT_HEIGHT}
          fill="transparent"
          data-testid={`dimension-trend-plot-${chartId}`}
          onMouseMove={(event) => {
            setHoveredIndex(hoveredIndexFromPointer(event, points.length));
          }}
          onMouseLeave={() => setHoveredIndex(null)}
        />
      </svg>

      {hovered !== null ? (
        <div
          data-testid={`dimension-trend-tooltip-${chartId}`}
          className="pointer-events-none absolute z-10 max-w-[calc(100%-0.5rem)] rounded-md border border-transparent bg-decorative-background-sky p-2 text-xs shadow-sm"
          style={placementForDimensionTrendTooltip(
            xFor(hoveredIndex ?? 0),
            yFor(hovered.valuePercent)
          )}
        >
          <p className="font-medium text-foreground">{formatTooltipDate(hovered.capturedAt)}</p>
          <p className="tabular-nums text-foreground">
            {(() => {
              const index = hoveredIndex ?? 0;
              const population =
                populationAtPoint?.(index) ?? {
                  contextualised: contextualisedCountAtPercent(
                    totalIncludingExcluded,
                    hovered.valuePercent
                  ),
                  total: totalIncludingExcluded,
                };
              return interpolate(t('overview.dimensionCard.trendTooltipValue'), {
                contextualised: formatCount(population.contextualised),
                total: formatCount(population.total),
                percent: String(hovered.valuePercent),
              });
            })()}
          </p>
          <p className="tabular-nums text-muted-foreground">
            {formatDimensionTrendPointChange(points, hoveredIndex ?? 0, t)}
          </p>
        </div>
      ) : null}
      </div>

      <div className="mt-1 flex w-full shrink-0 justify-between pl-6 text-[10px] text-muted-foreground">
        {axisLabelIndexes.map((index) => (
          <span key={points[index].capturedAt}>{formatAxisDate(points[index].capturedAt)}</span>
        ))}
      </div>
    </div>
  );
}

function hoveredIndexFromPointer(
  event: MouseEvent<SVGRectElement>,
  pointCount: number
): number {
  const svg = event.currentTarget.ownerSVGElement;
  if (svg === null || pointCount <= 1) {
    return 0;
  }

  const ctm = svg.getScreenCTM();
  if (ctm === null) {
    return 0;
  }

  const point = new DOMPoint(event.clientX, event.clientY).matrixTransform(ctm.inverse());
  const ratio = (point.x - CHART_LEFT_MARGIN) / PLOT_WIDTH;
  const index = Math.round(ratio * (pointCount - 1));
  return Math.max(0, Math.min(pointCount - 1, index));
}

function interpolate(template: string, vars: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => vars[name] ?? '');
}
