import { createContext, useContext } from 'react';

import type { DimensionId } from '../../types';

export type RerunTab = 'dimensions' | 'metrics';

/** What is picked for the next run, across both tabs, so that switching tabs loses nothing. */
export type RerunSelection = {
  tab: RerunTab;
  dimensions: ReadonlySet<DimensionId>;
  metrics: ReadonlySet<string>;
  /** Which data model to show in the metrics tab; `all` shows every model. */
  metricsModelFilter: string;
  setTab: (tab: RerunTab) => void;
  setDimensions: (next: ReadonlySet<DimensionId>) => void;
  setMetrics: (next: ReadonlySet<string>) => void;
  setMetricsModelFilter: (model: string) => void;
};

export const METRICS_MODEL_FILTER_ALL = 'all';

export const RerunSelectionContext = createContext<RerunSelection | null>(null);

export function useRerunSelection(): RerunSelection {
  const value = useContext(RerunSelectionContext);
  if (value === null) {
    throw new Error('useRerunSelection needs a RerunSelectionProvider.');
  }
  return value;
}
