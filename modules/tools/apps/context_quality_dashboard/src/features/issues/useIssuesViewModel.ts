import { useQueries, useQuery } from '@tanstack/react-query';
import { useCallback, useMemo } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { useLanguage } from '../../i18n/useLanguage';
import { useCatalogService, useQualityDashboardService } from '../../services/useServices';
import type { DimensionId, IssueDto, IssuesDto, MetricId, SliceFilters } from '../../types';
import { sliceFiltersQueryKey } from '../../types/sliceFiltersQueryKey';
import { useScopedFilters } from '../shell/useScopedFilters';

export type IssuesViewModel = {
  filters: SliceFilters;
  issues: IssueDto[];
  /** Set when the list holds only part of the issues. */
  partial: IssuesDto['partial'] | undefined;
  facilityLabels: Record<string, string>;
  ownerLabels: Record<string, string>;
  /** Nothing has arrived yet. Once the first metric has, the page shows what it has and `isLoadingMore` is set. */
  isLoading: boolean;
  isLoadingMore: boolean;
  /** Every metric failed. */
  isError: boolean;
  /** How many metrics could not be loaded while others could: their issues are missing. */
  failedCount: number;
  isEmpty: boolean;
  openIssueDrilldown: (dimensionId: DimensionId, metricId: MetricId) => void;
  t: (key: string) => string;
};

export function useIssuesViewModel(): IssuesViewModel {
  const { state, dispatch } = useAppState();
  const { t } = useLanguage();
  const qualityDashboardService = useQualityDashboardService();
  const catalogService = useCatalogService();

  const filters = useScopedFilters();

  // One query per metric, so each metric's issues show as soon as they arrive instead of after the slowest one
  const metricIds = useMemo(() => qualityDashboardService.getIssueMetricIds(), [qualityDashboardService]);
  const issueQueries = useQueries({
    queries: metricIds.map((metricId) => ({
      queryKey: ['issues', metricId, ...sliceFiltersQueryKey(filters)],
      queryFn: () => qualityDashboardService.getIssuesOfMetric(metricId, filters),
      enabled: state.page === 'issues',
    })),
  });

  const facilitiesQuery = useQuery({
    queryKey: ['catalog', 'facilities'],
    queryFn: () => catalogService.getFacilities(),
    enabled: state.page === 'issues',
  });

  const ownersQuery = useQuery({
    queryKey: ['catalog', 'owners'],
    queryFn: () => catalogService.getDataOwners(),
    enabled: state.page === 'issues',
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

  const openIssueDrilldown = useCallback(
    (dimensionId: DimensionId, metricId: MetricId) => {
      dispatch({ type: 'setDimensionId', dimensionId });
      dispatch({ type: 'setMetricId', metricId });
    },
    [dispatch]
  );

  // in the metrics' order, whatever order they arrived in, so the table does not reshuffle as results come in
  const arrived = issueQueries.map((query) => query.data);
  const issues = arrived.flatMap((part) => part?.issues ?? []);
  const cutMetricIds = metricIds.filter((_, index) => arrived[index]?.cut === true);
  const limit = Math.max(0, ...arrived.map((part) => (part?.cut === true ? part.limit : 0)));
  const partial: IssuesDto['partial'] =
    cutMetricIds.length > 0 ? { limitPerMetric: limit, metricCount: metricIds.length, cutMetricIds } : null;
  const loading = issueQueries.filter((query) => query.isLoading).length;
  const failedCount = issueQueries.filter((query) => query.isError).length;
  const anyArrived = issueQueries.some((query) => query.isSuccess);
  const isEmpty = loading === 0 && failedCount === 0 && anyArrived && issues.length === 0;

  return {
    filters,
    issues,
    partial,
    facilityLabels,
    ownerLabels,
    isLoading: !anyArrived && failedCount < metricIds.length,
    isLoadingMore: anyArrived && loading > 0,
    isError: metricIds.length > 0 && failedCount === metricIds.length,
    failedCount: anyArrived ? failedCount : 0,
    isEmpty,
    openIssueDrilldown,
    t,
  };
}
