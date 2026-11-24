import { ALL_DIMENSION_IDS } from '../../domain/dimensions';
import type { DimensionId } from '../../types';

/** What a run measured: the dimensions it covers, and whether it measured the metrics of a config file. */
export type RunCoverage = {
  dimensions: ReadonlySet<DimensionId>;
  configured: boolean;
};

const EVERYTHING: readonly DimensionId[] = ALL_DIMENSION_IDS;

function isDimensionId(value: string): value is DimensionId {
  return EVERYTHING.some((id) => id === value);
}

/**
 * Reads the `scope` a run marker carries: `all`, `all+configured`, `files,time-series`, `configured`... A marker without
 * one (a run written before scopes existed) is a run of every dimension. Text that names no known dimension covers
 * nothing, so an unknown scope is never mistaken for a full run.
 */
export function parseRunCoverage(scope: string | null): RunCoverage {
  if (scope === null || scope.trim() === '') {
    return { dimensions: new Set(EVERYTHING), configured: false };
  }
  const [base = '', ...flags] = scope.trim().split('+');
  const configured = flags.includes('configured') || base === 'configured';
  if (base === 'all') {
    return { dimensions: new Set(EVERYTHING), configured };
  }
  const named = base.split(',').map((part) => part.trim());
  return { dimensions: new Set(named.filter(isDimensionId)), configured };
}

export function coversDimension(coverage: RunCoverage, dimensionId: DimensionId): boolean {
  return coverage.dimensions.has(dimensionId);
}
