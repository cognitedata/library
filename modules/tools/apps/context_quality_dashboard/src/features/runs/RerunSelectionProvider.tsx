import { useMemo, useState, type ReactNode } from 'react';

import type { DimensionId } from '../../types';

import { RerunSelectionContext, METRICS_MODEL_FILTER_ALL, type RerunSelection, type RerunTab } from './rerunSelectionContext';

/** Holds the picks once for every component of the Rerun configuration section. */
export function RerunSelectionProvider({ children }: { children: ReactNode }) {
  const [tab, setTab] = useState<RerunTab>('dimensions');
  const [dimensions, setDimensions] = useState<ReadonlySet<DimensionId>>(new Set());
  const [metrics, setMetrics] = useState<ReadonlySet<string>>(new Set());
  const [metricsModelFilter, setMetricsModelFilter] = useState<string>(METRICS_MODEL_FILTER_ALL);
  const value = useMemo(
    (): RerunSelection => ({
      tab,
      dimensions,
      metrics,
      metricsModelFilter,
      setTab,
      setDimensions,
      setMetrics,
      setMetricsModelFilter,
    }),
    [tab, dimensions, metrics, metricsModelFilter]
  );
  return <RerunSelectionContext.Provider value={value}>{children}</RerunSelectionContext.Provider>;
}
