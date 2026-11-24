import { createContext } from 'react';

import type { DimensionId, MetricId, RunStatusDto } from '../../types';

export type QualityOperations = {
  runStatus: RunStatusDto | undefined;
  /** Set when the backend cannot be read at all; every page depends on the same latest run. */
  runStatusError: unknown;
  retryRunStatus: () => void;
  isRefreshing: boolean;
  isRerunning: boolean;
  isInProgress: boolean;
  refreshComplete: boolean;
  rerunComplete: boolean;
  rerunScope: { dimensionId: DimensionId } | { metricId: MetricId } | undefined;
  refreshLatchedSuccess: boolean;
  rerunLatchedSuccess: boolean;
  refreshLatchedFailure: boolean;
  rerunLatchedFailure: boolean;
  operationsTerminalDismissed: boolean;
  dismissOperationsTerminal: () => void;
  refresh: () => void;
  rerunDimension: (dimensionId: DimensionId) => void;
  rerunMetric: (metricId: MetricId) => void;
};

export const QualityOperationsContext = createContext<QualityOperations | null>(null);
