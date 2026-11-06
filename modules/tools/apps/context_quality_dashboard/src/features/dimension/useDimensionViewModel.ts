import { useMutation, useQueries, useQuery } from '@tanstack/react-query';
import { useCallback, useEffect, useMemo } from 'react';

import type { DimensionsScopeKind } from '../../app/state/types';
import { useAppState } from '../../app/state/useAppState';
import { useLanguage } from '../../i18n/useLanguage';
import { useBackend, useQualityDashboardService } from '../../services/useServices';
import type {
  ConfiguredDataModelDto,
  DimensionDto,
  DimensionId,
  DrilldownDto,
  DrilldownSummaryDto,
  MetricId,
  RunStatusDto,
  SliceFilters,
  SnapshotPointDto,
  TrendWindow,
} from '../../types';
import { sliceFiltersQueryKey } from '../../types/sliceFiltersQueryKey';
import { useQualityOperations } from '../shell/useQualityOperations';
import { useScopedFilters } from '../shell/useScopedFilters';

import { ALL_DIMENSION_IDS } from './dimensionIds';
import type { DimensionsScope } from './dimensionScope';
import { findConfiguredGroup } from './findConfiguredGroup';
import { isMetricId } from './metricIds';

export const DEFAULT_DIMENSION_ID: DimensionId = 'asset-hierarchy';

export type DimensionTrendViewModel = {
  metricId: MetricId;
  subKey: string | null;
  labelKey: string;
  points: SnapshotPointDto[];
  isLoading: boolean;
};

export type DimensionViewModel = {
  dimensionId: DimensionId;
  filters: SliceFilters;
  trendWindow: TrendWindow;
  dimension: DimensionDto | undefined;
  /** The lifecycle lists that have items. Empty until they arrive. */
  lifecycleWarnings: DrilldownSummaryDto[];
  drilldown: DrilldownDto | undefined;
  selectedMetricId: MetricId | undefined;
  trendMetrics: DimensionTrendViewModel[];
  runStatus: RunStatusDto | undefined;
  isLoading: boolean;
  isDrilldownLoading: boolean;
  isDrilldownError: boolean;
  isError: boolean;
  isEmpty: boolean;
  isDisabled: boolean;
  isNotifying: boolean;
  /** False when the connected backend cannot start a metrics run. */
  canRerun: boolean;
  /** False when the connected backend cannot notify owners. */
  canNotifyOwner: boolean;
  notificationSent: boolean;
  notifyError: boolean;
  selectDimension: (dimensionId: DimensionId) => void;
  selectMetric: (metricId: MetricId) => void;
  clearMetric: () => void;
  notifyOwner: (ownerId: string, entityIds: string[]) => void;
  rerunDimension: () => void;
  rerunMetric: () => void;
  isRerunningDimension: boolean;
  isRerunningMetric: boolean;
  allDimensionIds: readonly DimensionId[];
  dimensionsScope: DimensionsScopeKind;
  configuredModels: readonly ConfiguredDataModelDto[];
  isConfiguredMetricsLoading: boolean;
  isConfiguredMetricsError: boolean;
  configuredGroupId: string | undefined;
  selectDimensionsScope: (scope: DimensionsScope) => void;
  scopeDisplayValue: string;
  t: (key: string) => string;
};

export function useDimensionViewModel(): DimensionViewModel {
  const { state, dispatch } = useAppState();
  const { t } = useLanguage();
  const qualityDashboardService = useQualityDashboardService();
  const backend = useBackend();
  const qualityOperations = useQualityOperations();

  const filters = useScopedFilters();
  const trendWindow = state.trendWindow;
  const dimensionsScope: DimensionsScopeKind = state.dimensionsScope ?? 'builtin';
  const dimensionId = state.dimensionId ?? DEFAULT_DIMENSION_ID;
  const selectedMetricId = isMetricId(state.metricId) ? state.metricId : undefined;
  const isBuiltinScope = dimensionsScope === 'builtin';

  useEffect(() => {
    if (state.page !== 'dimensions') {
      return;
    }
    if (state.dimensionsScope === 'configured') {
      return;
    }
    if (state.dimensionId === undefined) {
      dispatch({ type: 'setDimensionId', dimensionId: DEFAULT_DIMENSION_ID });
    }
  }, [dispatch, state.dimensionId, state.dimensionsScope, state.page]);

  const configuredMetricsQuery = useQuery({
    queryKey: ['configuredMetrics', ...sliceFiltersQueryKey(filters)],
    queryFn: () => qualityDashboardService.getConfiguredMetrics(filters),
    enabled: state.page === 'dimensions',
  });

  const configuredModels = useMemo(
    () => configuredMetricsQuery.data?.models ?? [],
    [configuredMetricsQuery.data?.models]
  );

  const configuredGroup = useMemo(() => {
    if (state.configuredGroupId === undefined) {
      return undefined;
    }
    return findConfiguredGroup(configuredModels, state.configuredGroupId);
  }, [configuredModels, state.configuredGroupId]);

  const scopeDisplayValue = useMemo(() => {
    if (dimensionsScope === 'configured') {
      return configuredGroup?.viewLabel ?? state.configuredGroupId ?? t('dimension.label');
    }
    return t(`dimension.${dimensionId}`);
  }, [configuredGroup?.viewLabel, dimensionId, dimensionsScope, state.configuredGroupId, t]);

  const dimensionQuery = useQuery({
    queryKey: ['dimension', dimensionId, filters],
    queryFn: () => qualityDashboardService.getDimension(dimensionId, filters),
    enabled: state.page === 'dimensions' && isBuiltinScope,
  });

  // The lifecycle lists load apart from the dimension, so the page does not wait for them
  const lifecycleQuery = useQuery({
    queryKey: ['lifecycleWarnings', dimensionId, filters],
    queryFn: () => qualityDashboardService.getLifecycleWarnings(dimensionId, filters),
    enabled: state.page === 'dimensions' && isBuiltinScope && dimensionQuery.data?.enabled === true,
  });

  const drilldownQuery = useQuery({
    queryKey: ['drilldown', selectedMetricId, filters],
    queryFn: () => qualityDashboardService.getDrilldown(selectedMetricId as MetricId, filters),
    enabled:
      isBuiltinScope &&
      selectedMetricId !== undefined &&
      dimensionQuery.data?.enabled === true,
  });

  const runStatusQuery = useQuery({
    queryKey: ['runStatus'],
    queryFn: () => qualityDashboardService.getRunStatus(),
  });

  const trendMetricIds = useMemo(() => {
    const rows = dimensionQuery.data?.aggregateRows ?? [];
    const ids: { metricId: MetricId; subKey: string | null }[] = [];
    const seen = new Set<string>();
    const add = (metricId: MetricId, subKey: string | null | undefined) => {
      const id = `${metricId}/${subKey ?? ''}`;
      if (!seen.has(id)) {
        seen.add(id);
        ids.push({ metricId, subKey: subKey ?? null });
      }
    };
    for (const row of rows) {
      if (row.direction === 'critical') {
        add(row.metricId, row.subKey);
      }
    }
    for (const row of rows) {
      if (row.metricId === 'orphanRate') {
        add(row.metricId, row.subKey);
      }
    }
    return ids.slice(0, 8);
  }, [dimensionQuery.data?.aggregateRows]);

  const trendQueries = useQueries({
    queries: trendMetricIds.map(({ metricId, subKey }) => ({
      queryKey: ['snapshots', metricId, subKey, trendWindow, filters],
      queryFn: () =>
        subKey === null
          ? qualityDashboardService.getSnapshots(metricId, trendWindow, filters)
          : qualityDashboardService.getSnapshots(metricId, trendWindow, filters, subKey),
      enabled: isBuiltinScope && dimensionQuery.isSuccess && dimensionQuery.data.enabled,
    })),
  });

  const trendMetrics = useMemo(
    () =>
      trendMetricIds.map(({ metricId, subKey }, index) => ({
        metricId,
        subKey,
        labelKey: subKey === null ? `metric.${metricId}` : `metric.${metricId}.${subKey}`,
        points: trendQueries[index]?.data ?? [],
        isLoading: trendQueries[index]?.isLoading ?? false,
      })),
    [trendMetricIds, trendQueries]
  );

  const notifyMutation = useMutation({
    mutationFn: (input: { metricId: MetricId; ownerId: string; entityIds: string[] }) =>
      qualityDashboardService.notifyOwner(input),
  });

  const selectDimension = useCallback(
    (nextDimensionId: DimensionId) => {
      dispatch({
        type: 'setDimensionsScope',
        scope: 'builtin',
        dimensionId: nextDimensionId,
      });
    },
    [dispatch]
  );

  const selectDimensionsScope = useCallback(
    (scope: DimensionsScope) => {
      if (scope.kind === 'builtin') {
        dispatch({
          type: 'setDimensionsScope',
          scope: 'builtin',
          dimensionId: scope.dimensionId,
        });
        return;
      }
      dispatch({
        type: 'setDimensionsScope',
        scope: 'configured',
        configuredModelId: scope.modelId,
        configuredGroupId: scope.groupId,
      });
    },
    [dispatch]
  );

  const selectMetric = useCallback(
    (metricId: MetricId) => {
      if (!isBuiltinScope || dimensionQuery.data?.enabled === false) {
        return;
      }
      dispatch({ type: 'setMetricId', metricId });
    },
    [dimensionQuery.data?.enabled, dispatch, isBuiltinScope]
  );

  const clearMetric = useCallback(() => {
    dispatch({ type: 'setMetricId', metricId: undefined });
  }, [dispatch]);

  const notifyOwner = useCallback(
    (ownerId: string, entityIds: string[]) => {
      if (selectedMetricId === undefined || dimensionQuery.data?.enabled === false) {
        return;
      }
      notifyMutation.mutate({ metricId: selectedMetricId, ownerId, entityIds });
    },
    [dimensionQuery.data?.enabled, notifyMutation, selectedMetricId]
  );

  const rerunDimension = useCallback(() => {
    if (dimensionQuery.data?.enabled === false) {
      return;
    }
    qualityOperations.rerunDimension(dimensionId);
  }, [dimensionId, dimensionQuery.data?.enabled, qualityOperations]);

  const rerunMetric = useCallback(() => {
    if (selectedMetricId === undefined || dimensionQuery.data?.enabled === false) {
      return;
    }
    qualityOperations.rerunMetric(selectedMetricId);
  }, [dimensionQuery.data?.enabled, qualityOperations, selectedMetricId]);

  const isRerunningDimension =
    qualityOperations.rerunScope !== undefined &&
    'dimensionId' in qualityOperations.rerunScope &&
    qualityOperations.rerunScope.dimensionId === dimensionId;

  const isRerunningMetric =
    qualityOperations.rerunScope !== undefined &&
    'metricId' in qualityOperations.rerunScope &&
    selectedMetricId !== undefined &&
    qualityOperations.rerunScope.metricId === selectedMetricId;

  const isDisabled = dimensionQuery.data?.enabled === false;
  const isEmpty =
    dimensionQuery.isSuccess &&
    dimensionQuery.data.enabled &&
    // a dimension whose rows say nothing was eligible is Not applicable; one with no rows in the scope is empty
    !(dimensionQuery.data.status === 'not-applicable' && dimensionQuery.data.aggregateRows.length > 0) &&
    (dimensionQuery.data.entityCount === 0 || dimensionQuery.data.overallPercent === null);

  return {
    dimensionId,
    filters,
    trendWindow,
    dimension: dimensionQuery.data,
    lifecycleWarnings: lifecycleQuery.data ?? [],
    drilldown: drilldownQuery.data,
    selectedMetricId,
    trendMetrics,
    runStatus: runStatusQuery.data,
    isLoading: dimensionQuery.isLoading,
    isDrilldownLoading: drilldownQuery.isLoading,
    isDrilldownError: drilldownQuery.isError,
    isError: dimensionQuery.isError,
    isEmpty,
    isDisabled,
    isNotifying: notifyMutation.isPending,
    canRerun: backend.capabilities.rerun,
    canNotifyOwner: backend.capabilities.notifyOwner,
    notificationSent: notifyMutation.isSuccess,
    notifyError: notifyMutation.isError,
    selectDimension,
    selectMetric,
    clearMetric,
    notifyOwner,
    rerunDimension,
    rerunMetric,
    isRerunningDimension,
    isRerunningMetric,
    allDimensionIds: ALL_DIMENSION_IDS,
    dimensionsScope,
    configuredModels,
    isConfiguredMetricsLoading: configuredMetricsQuery.isLoading,
    isConfiguredMetricsError: configuredMetricsQuery.isError,
    configuredGroupId: state.configuredGroupId,
    selectDimensionsScope,
    scopeDisplayValue,
    t,
  };
}
