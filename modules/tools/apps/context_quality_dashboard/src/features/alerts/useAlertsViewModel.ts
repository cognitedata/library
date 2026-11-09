import { useQuery } from '@tanstack/react-query';
import { useCallback, useMemo } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { useLanguage } from '../../i18n/useLanguage';
import { useCatalogService, useQualityDashboardService } from '../../services/useServices';
import type { AlertDto, AlertsDto, DimensionId, MetricId, SliceFilters } from '../../types';
import { useScopedFilters } from '../shell/useScopedFilters';

export type AlertsViewModel = {
  filters: SliceFilters;
  alertsData: AlertsDto | undefined;
  alerts: AlertDto[];
  summary: AlertsDto['summary'] | undefined;
  facilityLabels: Record<string, string>;
  ownerLabels: Record<string, string>;
  isLoading: boolean;
  isError: boolean;
  isEmpty: boolean;
  navigateToDimension: (dimensionId: DimensionId, metricId?: MetricId) => void;
  t: (key: string) => string;
};

export function useAlertsViewModel(): AlertsViewModel {
  const { state, dispatch } = useAppState();
  const { t } = useLanguage();
  const qualityDashboardService = useQualityDashboardService();
  const catalogService = useCatalogService();

  const filters = useScopedFilters();

  const alertsQuery = useQuery({
    queryKey: ['alerts', filters],
    queryFn: () => qualityDashboardService.getAlerts(filters),
    enabled: state.page === 'alerts',
  });

  const facilitiesQuery = useQuery({
    queryKey: ['catalog', 'facilities'],
    queryFn: () => catalogService.getFacilities(),
    enabled: state.page === 'alerts',
  });

  const ownersQuery = useQuery({
    queryKey: ['catalog', 'owners'],
    queryFn: () => catalogService.getDataOwners(),
    enabled: state.page === 'alerts',
  });

  const facilityLabels = useMemo(() => {
    const labels: Record<string, string> = {};
    for (const facility of facilitiesQuery.data ?? []) {
      labels[facility.id] = facility.name;
    }
    return labels;
  }, [facilitiesQuery.data]);

  const ownerLabels = useMemo(() => {
    const labels: Record<string, string> = {};
    for (const owner of ownersQuery.data ?? []) {
      labels[owner.id] = owner.name;
    }
    return labels;
  }, [ownersQuery.data]);

  const navigateToDimension = useCallback(
    (dimensionId: DimensionId, metricId?: MetricId) => {
      dispatch({ type: 'setPage', page: 'dimensions' });
      dispatch({ type: 'setDimensionId', dimensionId });
      dispatch({ type: 'setMetricId', metricId });
    },
    [dispatch]
  );

  const alerts = alertsQuery.data?.alerts ?? [];
  const isEmpty = alertsQuery.isSuccess && alerts.length === 0;

  return {
    filters,
    alertsData: alertsQuery.data,
    alerts,
    summary: alertsQuery.data?.summary,
    facilityLabels,
    ownerLabels,
    isLoading: alertsQuery.isLoading,
    isError: alertsQuery.isError,
    isEmpty,
    navigateToDimension,
    t,
  };
}
