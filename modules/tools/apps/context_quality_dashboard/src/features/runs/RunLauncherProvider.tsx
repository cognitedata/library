import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useContext, useEffect, useMemo, useRef, type ReactNode } from 'react';

import { isBackendError } from '../../services/cdf/backendErrors';
import { useQualityDashboardService, useRunService } from '../../services/useServices';
import type { RunConfigModel } from '../configuration/runConfigFile';
import { DEFAULT_POLL_MS, RunConfigFileContext } from '../configuration/runConfigFileContext';
import { invalidateDashboardQueries } from '../shell/invalidateDashboardQueries';

import { RunLauncherContext, type RunLauncher, type RunPhase } from './runLauncherContext';
import { toRunInput, type RunRequest } from './runRequest';

/** How often the dashboard looks for a run it did not start. */
export const RUNNING_CHECK_MS = 20_000;

const ERROR_KEYS: Readonly<Record<string, string>> = {
  'permission-denied': 'configuration.run.error.permission-denied',
  unauthenticated: 'configuration.run.error.unauthenticated',
  network: 'configuration.run.error.network',
};

type StartArgs = { request: RunRequest; models: readonly RunConfigModel[] | null };

type RunLauncherProviderProps = {
  children: ReactNode;
  /** How often a run in progress is looked for; tests make it short. */
  runningCheckMs?: number;
};

/**
 * Starts runs and follows the one started here, and tells whether any other run is going on, so that two runs are never
 * started together: the workflow runs one at a time and a second one would only wait behind the first.
 */
export function RunLauncherProvider({ children, runningCheckMs = RUNNING_CHECK_MS }: RunLauncherProviderProps) {
  const runService = useRunService();
  const dashboard = useQualityDashboardService();
  const queryClient = useQueryClient();
  const { pollMs = DEFAULT_POLL_MS } = useContext(RunConfigFileContext);

  const start = useMutation({
    mutationFn: async ({ request, models }: StartArgs) => {
      if (runService === null) {
        throw new Error('No workflow is configured.');
      }
      const started = await runService.start(toRunInput(request, models));
      return { ...started, request };
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['runInProgress'] });
    },
  });
  const executionId = start.data?.executionId;

  const status = useQuery({
    queryKey: ['runExecution', executionId],
    queryFn: () => {
      if (runService === null || executionId === undefined) {
        throw new Error('No run to follow.');
      }
      return runService.getStatus(executionId);
    },
    enabled: executionId !== undefined && runService !== null,
    retry: false,
    refetchInterval: (query) => (query.state.data === undefined || query.state.data === 'running' ? pollMs : false),
  });

  const running = useQuery({
    queryKey: ['runInProgress'],
    queryFn: () => {
      if (runService === null) {
        throw new Error('No workflow is configured.');
      }
      return runService.findRunning();
    },
    enabled: runService !== null,
    retry: false,
    refetchInterval: runningCheckMs,
  });

  // When the run has finished, the new run is read: the cached runs are dropped and every query reloaded.
  const reloaded = useRef<string | undefined>(undefined);
  useEffect(() => {
    if (status.data === 'completed' && executionId !== undefined && reloaded.current !== executionId) {
      reloaded.current = executionId;
      void dashboard
        .refresh()
        .then(() => invalidateDashboardQueries(queryClient))
        .then(() => queryClient.invalidateQueries({ queryKey: ['runInProgress'] }));
    }
  }, [status.data, executionId, dashboard, queryClient]);

  const phase = ((): RunPhase => {
    if (start.isPending) {
      return 'starting';
    }
    if (start.isError) {
      return 'not-started';
    }
    if (executionId === undefined) {
      return 'idle';
    }
    if (status.data === 'completed' || status.data === 'failed') {
      return status.data;
    }
    return status.isError ? 'failed' : 'running';
  })();

  const { mutate } = start;
  const startRun = useCallback(
    (request: RunRequest, models: readonly RunConfigModel[] | null) => {
      mutate({ request, models });
    },
    [mutate]
  );

  const startError = useMemo(() => {
    if (!start.isError) {
      return null;
    }
    const error: unknown = start.error;
    return {
      key: (isBackendError(error) ? ERROR_KEYS[error.kind] : undefined) ?? 'configuration.run.error.other',
      message: error instanceof Error ? error.message : String(error),
    };
  }, [start.isError, start.error]);

  const ownRunGoingOn = phase === 'starting' || phase === 'running';
  const otherRunInProgress = running.data === true && !ownRunGoingOn;
  const request = start.isPending ? (start.variables?.request ?? null) : (start.data?.request ?? null);

  const value = useMemo(
    (): RunLauncher => ({
      phase,
      request,
      startError,
      otherRunInProgress,
      busy: ownRunGoingOn || otherRunInProgress,
      start: startRun,
    }),
    [phase, request, startError, otherRunInProgress, ownRunGoingOn, startRun]
  );

  if (runService === null) {
    return <>{children}</>;
  }
  return <RunLauncherContext.Provider value={value}>{children}</RunLauncherContext.Provider>;
}
