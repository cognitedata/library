import { useQueries } from '@tanstack/react-query';
import { useMemo } from 'react';

import { useQualityDashboardService } from '../../services/useServices';
import type { DimensionId, SliceFilters, SnapshotPointDto, TrendWindow } from '../../types';
import { sliceFiltersQueryKey } from '../../types/sliceFiltersQueryKey';

import type { DimensionPresentation } from './overviewPresentation';
import { primaryTrendMetricForDimension } from './trendMetricPresentation';

export type DimensionTrendSeries = {
  points: SnapshotPointDto[];
  isLoading: boolean;
};

export function useDimensionOverviewTrends(
  dimensions: readonly DimensionPresentation[],
  filters: SliceFilters,
  trendWindow: TrendWindow
): ReadonlyMap<DimensionId, DimensionTrendSeries> {
  const qualityDashboardService = useQualityDashboardService();

  const enabledDimensions = useMemo(
    () => dimensions.filter((dimension) => dimension.enabled),
    [dimensions]
  );

  const queries = useQueries({
    queries: enabledDimensions.map((dimension) => {
      const metricId = primaryTrendMetricForDimension(dimension.dimensionId);
      return {
        queryKey: [
          'dimension-card-snapshots',
          dimension.dimensionId,
          metricId,
          trendWindow,
          ...sliceFiltersQueryKey(filters),
        ],
        queryFn: () => qualityDashboardService.getSnapshots(metricId, trendWindow, filters),
      };
    }),
  });

  return useMemo(() => {
    const map = new Map<DimensionId, DimensionTrendSeries>();
    enabledDimensions.forEach((dimension, index) => {
      map.set(dimension.dimensionId, {
        points: queries[index]?.data ?? [],
        isLoading: queries[index]?.isLoading ?? false,
      });
    });
    return map;
  }, [enabledDimensions, queries]);
}
