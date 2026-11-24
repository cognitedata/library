import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';

import { useQualityDashboardService } from '../../services/useServices';
import { sliceFiltersQueryKey } from '../../types/sliceFiltersQueryKey';
import { findConfiguredGroup } from '../dimension/findConfiguredGroup';

import { useScopedFilters } from './useScopedFilters';

/** The configured view group the run button of a configured dimensions page would run, once its metrics are read. */
export function useConfiguredRunGroup(enabled: boolean, configuredGroupId: string | undefined) {
  const qualityDashboardService = useQualityDashboardService();
  const filters = useScopedFilters();
  const configuredMetricsQuery = useQuery({
    queryKey: ['configuredMetrics', ...sliceFiltersQueryKey(filters)],
    queryFn: () => qualityDashboardService.getConfiguredMetrics(filters),
    enabled,
  });
  const models = configuredMetricsQuery.data?.models;

  return useMemo(() => {
    if (!enabled || configuredGroupId === undefined) {
      return undefined;
    }
    return findConfiguredGroup(models ?? [], configuredGroupId);
  }, [models, enabled, configuredGroupId]);
}
