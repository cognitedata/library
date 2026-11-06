import { useQueries } from '@tanstack/react-query';
import { useMemo } from 'react';

import { useQualityDashboardService } from '../../services/useServices';
import type { ConfiguredMetricKey, ConfiguredMetricTrendPointDto, SliceFilters, TrendWindow } from '../../types';
import { sliceFiltersQueryKey } from '../../types/sliceFiltersQueryKey';

import { configuredMetricKeyId } from './configuredMetricKey';

export type ConfiguredMetricTrendSeries = {
  points: ConfiguredMetricTrendPointDto[];
  isLoading: boolean;
};

export function useConfiguredMetricTrends(
  keys: readonly ConfiguredMetricKey[],
  filters: SliceFilters,
  trendWindow: TrendWindow
): ReadonlyMap<string, ConfiguredMetricTrendSeries> {
  const qualityDashboardService = useQualityDashboardService();

  const queries = useQueries({
    queries: keys.map((key) => ({
      queryKey: [
        'configured-metric-trends',
        key.modelId,
        key.view,
        key.property,
        trendWindow,
        ...sliceFiltersQueryKey(filters),
      ],
      queryFn: () => qualityDashboardService.getConfiguredMetricTrends(key, trendWindow, filters),
    })),
  });

  return useMemo(() => {
    const map = new Map<string, ConfiguredMetricTrendSeries>();
    keys.forEach((key, index) => {
      map.set(configuredMetricKeyId(key), {
        points: queries[index]?.data ?? [],
        isLoading: queries[index]?.isLoading ?? false,
      });
    });
    return map;
  }, [keys, queries]);
}

/** Trend charts need at least two runs with valid coverage in the selected window. */
export function hasConfiguredMetricTrendHistory(
  points: readonly ConfiguredMetricTrendPointDto[]
): boolean {
  return points.length >= 2;
}
