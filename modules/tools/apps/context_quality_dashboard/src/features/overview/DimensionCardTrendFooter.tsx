import { Skeleton } from '@cognite/aura/components/skeleton';

import type { SnapshotPointDto } from '../../types';

import { DimensionTrendAreaChart } from './DimensionTrendAreaChart';

type DimensionCardTrendFooterProps = {
  chartId: string;
  points: SnapshotPointDto[];
  totalIncludingExcluded: number;
  populationAtPoint?: (index: number) => { contextualised: number; total: number };
  isLoading: boolean;
  t: (key: string) => string;
  /** Stacked under coverage (dimension cards) or beside it (configured metric cards). */
  layout?: 'stacked' | 'aside';
};

export function DimensionCardTrendFooter({
  chartId,
  points,
  totalIncludingExcluded,
  populationAtPoint,
  isLoading,
  t,
  layout = 'stacked',
}: DimensionCardTrendFooterProps) {
  const isAside = layout === 'aside';
  return (
    <div
      className={`w-full min-w-0 ${isAside ? 'flex h-full min-h-0 flex-col justify-center' : 'border-t border-border pt-6'}`}
      data-testid="dimension-trend-footer"
    >
      <div className="w-full min-w-0">
        {isLoading ? (
          <Skeleton className="aspect-[7/2] w-full" data-testid="dimension-trend-loading" />
        ) : (
          <DimensionTrendAreaChart
            chartId={chartId}
            points={points}
            totalIncludingExcluded={totalIncludingExcluded}
            populationAtPoint={populationAtPoint}
            t={t}
          />
        )}
      </div>
    </div>
  );
}
