import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useMemo, useState, type ReactNode } from 'react';

import { useQualityDashboardService } from '../../services/useServices';
import type { DimensionId, MetricId } from '../../types';

import { invalidateDashboardQueries } from './invalidateDashboardQueries';
import {
  QualityOperationsContext,
  type QualityOperations,
} from './qualityOperationsReactContext';

type QualityOperationsProviderProps = {
  children: ReactNode;
};

export function QualityOperationsProvider({ children }: QualityOperationsProviderProps) {
  const qualityDashboardService = useQualityDashboardService();
  const queryClient = useQueryClient();
  const [operationsTerminalDismissed, setOperationsTerminalDismissed] = useState(false);
  const [refreshLatchedSuccess, setRefreshLatchedSuccess] = useState(false);
  const [rerunLatchedSuccess, setRerunLatchedSuccess] = useState(false);
  const [refreshLatchedFailure, setRefreshLatchedFailure] = useState(false);
  const [rerunLatchedFailure, setRerunLatchedFailure] = useState(false);

  const clearOpsTerminalLatches = useCallback(() => {
    setRefreshLatchedSuccess(false);
    setRerunLatchedSuccess(false);
    setRefreshLatchedFailure(false);
    setRerunLatchedFailure(false);
  }, []);

  const beginOperationsRun = useCallback(() => {
    setOperationsTerminalDismissed(false);
    clearOpsTerminalLatches();
  }, [clearOpsTerminalLatches]);

  const runStatusQuery = useQuery({
    queryKey: ['runStatus'],
    queryFn: () => qualityDashboardService.getRunStatus(),
  });

  const { refetch: refetchRunStatus } = runStatusQuery;
  const retryRunStatus = useCallback(() => {
    void refetchRunStatus();
  }, [refetchRunStatus]);

  const refreshMutation = useMutation({
    mutationFn: () => qualityDashboardService.refresh(),
    onMutate: beginOperationsRun,
    onSuccess: async () => {
      setRefreshLatchedSuccess(true);
      await invalidateDashboardQueries(queryClient);
    },
    onError: () => {
      setRefreshLatchedFailure(true);
    },
  });

  const rerunMutation = useMutation({
    mutationFn: (scope: { dimensionId: DimensionId } | { metricId: MetricId }) =>
      qualityDashboardService.rerun(scope),
    onMutate: beginOperationsRun,
    onSuccess: async () => {
      setRerunLatchedSuccess(true);
      await invalidateDashboardQueries(queryClient);
    },
    onError: () => {
      setRerunLatchedFailure(true);
    },
  });

  const refresh = useCallback(() => {
    refreshMutation.mutate();
  }, [refreshMutation]);

  const rerunDimension = useCallback(
    (dimensionId: DimensionId) => {
      rerunMutation.mutate({ dimensionId });
    },
    [rerunMutation]
  );

  const rerunMetric = useCallback(
    (metricId: MetricId) => {
      rerunMutation.mutate({ metricId });
    },
    [rerunMutation]
  );

  const dismissOperationsTerminal = useCallback(() => {
    setOperationsTerminalDismissed(true);
    clearOpsTerminalLatches();
    refreshMutation.reset();
    rerunMutation.reset();
  }, [clearOpsTerminalLatches, refreshMutation, rerunMutation]);

  const isInProgress =
    refreshMutation.isPending ||
    rerunMutation.isPending ||
    runStatusQuery.data?.inProgress === true;

  const value = useMemo(
    (): QualityOperations => ({
      runStatus: runStatusQuery.data,
      runStatusError: runStatusQuery.error,
      retryRunStatus,
      isRefreshing: refreshMutation.isPending,
      isRerunning: rerunMutation.isPending,
      isInProgress,
      refreshComplete: refreshMutation.isSuccess,
      rerunComplete: rerunMutation.isSuccess,
      rerunScope: rerunMutation.isPending ? rerunMutation.variables : undefined,
      refreshLatchedSuccess,
      rerunLatchedSuccess,
      refreshLatchedFailure,
      rerunLatchedFailure,
      operationsTerminalDismissed,
      dismissOperationsTerminal,
      refresh,
      rerunDimension,
      rerunMetric,
    }),
    [
      dismissOperationsTerminal,
      isInProgress,
      operationsTerminalDismissed,
      refresh,
      refreshLatchedFailure,
      refreshLatchedSuccess,
      refreshMutation.isPending,
      refreshMutation.isSuccess,
      rerunDimension,
      rerunLatchedFailure,
      rerunLatchedSuccess,
      rerunMetric,
      rerunMutation.isPending,
      rerunMutation.isSuccess,
      rerunMutation.variables,
      retryRunStatus,
      runStatusQuery.data,
      runStatusQuery.error,
    ]
  );

  return (
    <QualityOperationsContext.Provider value={value}>{children}</QualityOperationsContext.Provider>
  );
}
