import { useQuery } from '@tanstack/react-query';

import { useQualityDashboardService } from '../../services/useServices';
import type { ConfiguredMetricKey, SliceFilters } from '../../types';
import { sliceFiltersQueryKey } from '../../types/sliceFiltersQueryKey';

import { configuredMetricKeyId } from './configuredMetricKey';

export function useConfiguredDrilldownViewModel(metricKey: ConfiguredMetricKey, filters: SliceFilters) {
  const service = useQualityDashboardService();
  return useQuery({
    queryKey: ['configuredMetrics', 'drilldown', configuredMetricKeyId(metricKey), ...sliceFiltersQueryKey(filters)],
    queryFn: () => service.getConfiguredDrilldown(metricKey, filters),
  });
}
