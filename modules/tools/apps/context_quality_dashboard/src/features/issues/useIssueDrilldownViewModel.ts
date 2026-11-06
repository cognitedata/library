import { useMutation, useQuery } from '@tanstack/react-query';
import { useCallback } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { useLanguage } from '../../i18n/useLanguage';
import { useBackend, useQualityDashboardService } from '../../services/useServices';
import type { DimensionId, MetricId } from '../../types';
import type { DrilldownPanelViewModel } from '../dimension/drilldownPanelViewModel';
import { isMetricId } from '../dimension/metricIds';
import { DEFAULT_DIMENSION_ID } from '../dimension/useDimensionViewModel';
import { useQualityOperations } from '../shell/useQualityOperations';
import { useScopedFilters } from '../shell/useScopedFilters';

export function useIssueDrilldownViewModel(): DrilldownPanelViewModel {
  const { state, dispatch } = useAppState();
  const { t } = useLanguage();
  const qualityDashboardService = useQualityDashboardService();
  const backend = useBackend();
  const qualityOperations = useQualityOperations();
  const filters = useScopedFilters();

  const selectedMetricId = isMetricId(state.metricId) ? state.metricId : undefined;
  const dimensionId: DimensionId = state.dimensionId ?? DEFAULT_DIMENSION_ID;
  const drilldownOpen = state.page === 'issues' && selectedMetricId !== undefined;

  const dimensionQuery = useQuery({
    queryKey: ['dimension', dimensionId, filters],
    queryFn: () => qualityDashboardService.getDimension(dimensionId, filters),
    enabled: drilldownOpen,
  });

  const drilldownQuery = useQuery({
    queryKey: ['drilldown', selectedMetricId, filters],
    queryFn: () => {
      if (selectedMetricId === undefined) {
        throw new Error('No metric selected');
      }
      return qualityDashboardService.getDrilldown(selectedMetricId, filters);
    },
    enabled: drilldownOpen && dimensionQuery.data?.enabled === true,
  });

  const notifyMutation = useMutation({
    mutationFn: (input: { metricId: MetricId; ownerId: string; entityIds: string[] }) =>
      qualityDashboardService.notifyOwner(input),
  });

  const clearMetric = useCallback(() => {
    dispatch({ type: 'setMetricId', metricId: undefined });
  }, [dispatch]);

  const rerunMetric = useCallback(() => {
    if (selectedMetricId === undefined || dimensionQuery.data?.enabled === false) {
      return;
    }
    qualityOperations.rerunMetric(selectedMetricId);
  }, [dimensionQuery.data?.enabled, qualityOperations, selectedMetricId]);

  const notifyOwner = useCallback(
    (ownerId: string, entityIds: string[]) => {
      if (selectedMetricId === undefined || dimensionQuery.data?.enabled === false) {
        return;
      }
      notifyMutation.mutate({ metricId: selectedMetricId, ownerId, entityIds });
    },
    [dimensionQuery.data?.enabled, notifyMutation, selectedMetricId]
  );

  const isRerunningMetric =
    qualityOperations.rerunScope !== undefined &&
    'metricId' in qualityOperations.rerunScope &&
    selectedMetricId !== undefined &&
    qualityOperations.rerunScope.metricId === selectedMetricId;

  return {
    drilldown: drilldownQuery.data,
    selectedMetricId: drilldownOpen ? selectedMetricId : undefined,
    isDrilldownLoading: drilldownOpen && (dimensionQuery.isLoading || drilldownQuery.isLoading),
    isDrilldownError: drilldownQuery.isError,
    clearMetric,
    canRerun: backend.capabilities.rerun,
    isRerunningMetric,
    rerunMetric,
    canNotifyOwner: backend.capabilities.notifyOwner,
    isNotifying: notifyMutation.isPending,
    notificationSent: notifyMutation.isSuccess,
    notifyError: notifyMutation.isError,
    notifyOwner,
    t,
  };
}
