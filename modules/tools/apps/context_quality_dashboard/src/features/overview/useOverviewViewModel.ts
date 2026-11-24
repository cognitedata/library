import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useMemo } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { useLanguage } from '../../i18n/useLanguage';
import { useQualityDashboardService } from '../../services/useServices';
import type {
  ConfiguredDataModelDto,
  DimensionId,
  HeadlineMetricDto,
  OverviewDto,
  RunStatusDto,
  SliceFilters,
  SnapshotPointDto,
  TrendWindow,
} from '../../types';
import { sliceFiltersQueryKey } from '../../types/sliceFiltersQueryKey';
import { invalidateDashboardQueries } from '../shell/invalidateDashboardQueries';
import { useScopedFilters } from '../shell/useScopedFilters';

import {
  buildOverviewPresentation,
  type OverviewPresentation,
} from './overviewPresentation';

export type HeadlineTrendViewModel = {
  metricId: HeadlineMetricDto['metricId'];
  labelKey: string;
  points: SnapshotPointDto[];
  isLoading: boolean;
};

export type OverviewViewModel = {
  filters: SliceFilters;
  trendWindow: TrendWindow;
  overview: OverviewDto | undefined;
  /** Metrics the config file named; empty when the run had none. */
  configuredModels: readonly ConfiguredDataModelDto[];
  /** Whether the latest run's configured metrics have been read: until then, nothing is said about having none. */
  isConfiguredLoaded: boolean;
  presentation: OverviewPresentation | undefined;
  runStatus: RunStatusDto | undefined;
  isLoading: boolean;
  isSliceLoading: boolean;
  isError: boolean;
  isEmpty: boolean;
  isRefreshing: boolean;
  refresh: () => void;
  navigateToDimension: (dimensionId: DimensionId) => void;
  navigateToDimensions: () => void;
  navigateToIssues: () => void;
  navigateToConfiguration: () => void;
  setTrendWindow: (trendWindow: TrendWindow) => void;
  t: (key: string) => string;
};

export function useOverviewViewModel(): OverviewViewModel {
  const { state, dispatch } = useAppState();
  const { t } = useLanguage();
  const qualityDashboardService = useQualityDashboardService();
  const queryClient = useQueryClient();

  const filters = useScopedFilters();
  const trendWindow = state.trendWindow;

  const overviewQuery = useQuery({
    queryKey: ['overview', ...sliceFiltersQueryKey(filters)],
    queryFn: () => qualityDashboardService.getOverview(filters),
  });

  const configuredQuery = useQuery({
    queryKey: ['configuredMetrics', ...sliceFiltersQueryKey(filters)],
    queryFn: () => qualityDashboardService.getConfiguredMetrics(filters),
  });

  const runStatusQuery = useQuery({
    queryKey: ['runStatus'],
    queryFn: () => qualityDashboardService.getRunStatus(),
  });

  const refreshMutation = useMutation({
    mutationFn: () => qualityDashboardService.refresh(),
    onSuccess: async () => {
      await invalidateDashboardQueries(queryClient);
    },
  });

  const presentation = useMemo(() => {
    if (overviewQuery.data === undefined) {
      return undefined;
    }
    return buildOverviewPresentation({ overview: overviewQuery.data });
  }, [overviewQuery.data]);

  const navigateToDimension = useCallback(
    (dimensionId: DimensionId) => {
      dispatch({ type: 'setPage', page: 'dimensions' });
      dispatch({ type: 'setDimensionId', dimensionId });
    },
    [dispatch]
  );

  const navigateToDimensions = useCallback(() => {
    dispatch({ type: 'setPage', page: 'dimensions' });
  }, [dispatch]);

  const navigateToIssues = useCallback(() => {
    dispatch({ type: 'setPage', page: 'issues' });
  }, [dispatch]);

  const navigateToConfiguration = useCallback(() => {
    dispatch({ type: 'setPage', page: 'configuration' });
  }, [dispatch]);

  const setTrendWindow = useCallback(
    (window: TrendWindow) => {
      dispatch({ type: 'setTrendWindow', trendWindow: window });
    },
    [dispatch]
  );

  const refresh = useCallback(() => {
    refreshMutation.mutate();
  }, [refreshMutation]);

  const isEmpty =
    overviewQuery.isSuccess &&
    (overviewQuery.data.scope.entityCount === 0 ||
      overviewQuery.data.overallAggregatePercent === null);

  return {
    filters,
    trendWindow,
    overview: overviewQuery.data,
    configuredModels: configuredQuery.data?.models ?? [],
    isConfiguredLoaded: configuredQuery.isSuccess,
    presentation,
    runStatus: runStatusQuery.data,
    isLoading: overviewQuery.isLoading,
    isSliceLoading: overviewQuery.isPending,
    isError: overviewQuery.isError,
    isEmpty,
    isRefreshing: refreshMutation.isPending || runStatusQuery.data?.inProgress === true,
    refresh,
    navigateToDimension,
    navigateToDimensions,
    navigateToIssues,
    navigateToConfiguration,
    setTrendWindow,
    t,
  };
}
