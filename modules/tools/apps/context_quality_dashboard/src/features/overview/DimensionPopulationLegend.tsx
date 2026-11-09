import { formatCount } from './contextualisationCoverageLabels';
import {
  buildInScopeBreakdownSegments,
  type BreakdownSegmentId,
} from './coverageBreakdownSegments';

const LABEL_KEYS: Record<Exclude<BreakdownSegmentId, 'excluded'>, string> = {
  contextualised: 'overview.coverage.contextualised',
  remaining: 'overview.coverage.remaining',
};

type DimensionPopulationLegendProps = {
  contextualised: number;
  remaining: number;
  total: number;
  /** When true, always list contextualised and remaining even when a count is zero. */
  includeZeroSegments?: boolean;
  showPercent?: boolean;
  density?: 'default' | 'minimal';
  /** Names of the two parts when they are not "Contextualised" and "Remaining" (a metric about something else). */
  labelKeys?: Record<Exclude<BreakdownSegmentId, 'excluded'>, string>;
  t: (key: string) => string;
};

export function DimensionPopulationLegend({
  contextualised,
  remaining,
  total,
  includeZeroSegments = false,
  showPercent = true,
  density = 'default',
  labelKeys = LABEL_KEYS,
  t,
}: DimensionPopulationLegendProps) {
  if (total <= 0) {
    return null;
  }

  const segments = buildInScopeBreakdownSegments(
    { contextualised, remaining },
    { includeZeroSegments }
  );

  return (
    <ul
      className={`flex w-full min-w-0 flex-col ${density === 'minimal' ? 'gap-0.5' : 'gap-1.5'}`}
      data-testid="dimension-population-legend"
    >
      {segments.map((segment) => {
        const percent = Math.round((segment.value / total) * 100);
        return (
          <li
            key={segment.id}
            className={`flex w-full min-w-0 items-center gap-1.5 text-muted-foreground ${
              density === 'minimal' ? 'text-[11px] leading-tight' : 'text-xs'
            }`}
            data-testid={`dimension-population-legend-${segment.id}`}
          >
            <span
              className={`shrink-0 rounded-sm ${segment.legendDotClassName} ${
                density === 'minimal' ? 'size-1.5' : 'size-2'
              }`}
              aria-hidden="true"
            />
            <span className="min-w-0 flex-1 truncate">{segment.id === 'excluded' ? '' : t(labelKeys[segment.id])}</span>
            <span className="shrink-0 tabular-nums text-foreground">{formatCount(segment.value)}</span>
            {showPercent ? (
              <span className="w-9 shrink-0 text-right tabular-nums">{percent}%</span>
            ) : null}
          </li>
        );
      })}
    </ul>
  );
}
