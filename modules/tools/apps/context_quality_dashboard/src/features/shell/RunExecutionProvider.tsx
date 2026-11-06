import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import { isBackendError } from '../../services/cdf/backendErrors';
import { useQualityDashboardService, useRunService } from '../../services/useServices';
import { type RunConfigModel } from '../configuration/runConfigFile';
import { DEFAULT_POLL_MS, RunConfigFileContext } from '../configuration/runConfigFileContext';
import type { RunPhase } from '../configuration/useRunConfigFileViewModel';
import { toRunInput, type RunRequest } from '../runs/runRequest';

import { invalidateDashboardQueries } from './invalidateDashboardQueries';
import {
  RunExecutionContext,
  type RunExecutionContextValue,
  type RunExecutionStartError,
} from './runExecutionReactContext';

const ERROR_KEYS: Readonly<Record<string, string>> = {
  'permission-denied': 'configuration.run.error.permission-denied',
  unauthenticated: 'configuration.run.error.unauthenticated',
  network: 'configuration.run.error.network',
};

type RunExecutionProviderProps = {
  children: ReactNode;
  pollMs?: number;
};

export function RunExecutionProvider({ children, pollMs: pollMsProp }: RunExecutionProviderProps) {
  const fileContext = useContext(RunConfigFileContext);
  const pollMs = pollMsProp ?? fileContext.pollMs ?? DEFAULT_POLL_MS;
  const runService = useRunService();
  const qualityDashboardService = useQualityDashboardService();
  const queryClient = useQueryClient();
  const [configTerminalDismissed, setConfigTerminalDismissed] = useState(false);

  const start = useMutation({
    mutationFn: async (models: readonly RunConfigModel[]) => {
      if (runService === null) {
        throw new Error('No workflow is configured.');
      }
      const request: RunRequest = models.length === 0 ? { kind: 'dimensions' } : { kind: 'everything' };
      return runService.start(toRunInput(request, models.length === 0 ? null : models));
    },
    onMutate: () => {
      setConfigTerminalDismissed(false);
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

  const reloaded = useRef<string | undefined>(undefined);
  useEffect(() => {
    if (status.data === 'completed' && executionId !== undefined && reloaded.current !== executionId) {
      reloaded.current = executionId;
      void qualityDashboardService.refresh().then(() => invalidateDashboardQueries(queryClient));
    }
  }, [status.data, executionId, qualityDashboardService, queryClient]);

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

  const startError = useMemo((): RunExecutionStartError | null => {
    if (!start.isError) {
      return null;
    }
    const error: unknown = start.error;
    return {
      key: (isBackendError(error) ? ERROR_KEYS[error.kind] : undefined) ?? 'configuration.run.error.other',
      message: error instanceof Error ? error.message : String(error),
    };
  }, [start.error, start.isError]);

  const startRun = useCallback(
    (models: readonly RunConfigModel[]) => {
      setConfigTerminalDismissed(false);
      start.mutate(models);
    },
    [start]
  );

  const dismissConfigTerminal = useCallback(() => {
    setConfigTerminalDismissed(true);
    if (phase === 'completed' || phase === 'failed' || phase === 'not-started') {
      start.reset();
    }
  }, [phase, start]);

  const value = useMemo(
    (): RunExecutionContextValue => ({
      phase,
      startError,
      configTerminalDismissed,
      startRun,
      dismissConfigTerminal,
    }),
    [configTerminalDismissed, dismissConfigTerminal, phase, startError, startRun]
  );

  return <RunExecutionContext.Provider value={value}>{children}</RunExecutionContext.Provider>;
}
