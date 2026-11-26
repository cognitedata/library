import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useMemo, useState, type ReactNode } from 'react';

import { useQualityDashboardService } from '../../services/useServices';
import type { DimensionId, MetricId, RunStatusDto } from '../../types';

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

  // A query that has no data yet (the "no completed run" error) goes back to pending, with its error cleared, every
  // time it refetches, for example when the browser tab regains focus. Keep the last answer meanwhile, so the app
  // does not forget that it already knows there is no run and swap the first-run screen for a loading state.
  const [lastRunStatus, setLastRunStatus] = useState<{ data: RunStatusDto | undefined; error: unknown } | null>(null);
  if (
    !runStatusQuery.isPending &&
    (lastRunStatus === null ||
      lastRunStatus.data !== runStatusQuery.data ||
      lastRunStatus.error !== runStatusQuery.error)
  ) {
    setLastRunStatus({ data: runStatusQuery.data, error: runStatusQuery.error });
  }
  const runStatus = runStatusQuery.isPending ? lastRunStatus?.data : runStatusQuery.data;
  const runStatusError = runStatusQuery.isPending ? lastRunStatus?.error : runStatusQuery.error;
  const isRunStatusLoading = runStatusQuery.isPending && lastRunStatus === null;

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
    runStatus?.inProgress === true;

  const value = useMemo(
    (): QualityOperations => ({
      runStatus,
      runStatusError,
      isRunStatusLoading,
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
      runStatus,
      runStatusError,
      isRunStatusLoading,
    ]
  );

  return (
    <QualityOperationsContext.Provider value={value}>{children}</QualityOperationsContext.Provider>
  );
}
