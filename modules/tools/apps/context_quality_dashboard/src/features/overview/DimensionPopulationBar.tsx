import { buildInScopeBreakdownSegments } from './coverageBreakdownSegments';

type DimensionPopulationBarProps = {
  contextualised: number;
  remaining: number;
  ariaLabel: string;
  includeZeroSegments?: boolean;
  size?: 'sm' | 'md';
};

export function DimensionPopulationBar({
  contextualised,
  remaining,
  ariaLabel,
  includeZeroSegments = false,
  size = 'md',
}: DimensionPopulationBarProps) {
  const total = contextualised + remaining;
  if (total <= 0) {
    return null;
  }

  const segments = buildInScopeBreakdownSegments(
    { contextualised, remaining },
    { includeZeroSegments }
  );

  return (
    <div
      className={`flex w-full min-w-0 overflow-hidden rounded-sm bg-muted/80 ${size === 'sm' ? 'h-1.5' : 'h-3'}`}
      role="img"
      aria-label={ariaLabel}
      data-testid="dimension-population-bar"
    >
      {segments.map((segment) => {
        const widthPercent = (segment.value / total) * 100;
        if (widthPercent <= 0) {
          return null;
        }
        return (
          <div
            key={segment.id}
            className={`h-full ${segment.legendDotClassName}`}
            style={{ width: `${widthPercent}%` }}
            data-testid={`dimension-population-segment-${segment.id}`}
          />
        );
      })}
    </div>
  );
}
