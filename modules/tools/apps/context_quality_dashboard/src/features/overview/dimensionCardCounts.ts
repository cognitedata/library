import type { DimensionId } from '../../types';

import type { DimensionPresentation } from './overviewPresentation';

/** The numbers on a dimension card: how many items there are, how many are contextualised, how many are not. */
export type DimensionCardCounts = {
  dimensionId: DimensionId;
  nameKey: string;
  contextualisedCount: number;
  inScopeCount: number;
  contextualisedPercent: number | null;
  unresolvedCount: number;
};

/**
 * The counts come from what the headline metric measured. Without them (the demo data does not say), the population is
 * worked back from the percentage and the number of issues, which is only as exact as the rounded percentage.
 */
function countsOf(dimension: DimensionPresentation): Omit<DimensionCardCounts, 'dimensionId' | 'nameKey'> {
  if (dimension.eligibleCount !== undefined && dimension.passingCount !== undefined) {
    const inScopeCount = dimension.eligibleCount;
    const contextualisedCount = Math.min(dimension.passingCount, inScopeCount);
    return {
      inScopeCount,
      contextualisedCount,
      unresolvedCount: inScopeCount - contextualisedCount,
      contextualisedPercent: inScopeCount === 0 ? null : Math.round((contextualisedCount / inScopeCount) * 100),
    };
  }
  const unresolvedCount = dimension.issueCount ?? 0;
  const unresolvedShare = dimension.overallPercent === null ? 0 : 1 - dimension.overallPercent / 100;
  const inScopeCount = unresolvedShare > 0 ? Math.round(unresolvedCount / unresolvedShare) : unresolvedCount;
  return {
    inScopeCount,
    contextualisedCount: Math.max(inScopeCount - unresolvedCount, 0),
    unresolvedCount,
    contextualisedPercent: dimension.overallPercent,
  };
}

export function buildDimensionCardCounts(dimensions: readonly DimensionPresentation[]): DimensionCardCounts[] {
  return dimensions.map((dimension) => ({
    dimensionId: dimension.dimensionId,
    nameKey: dimension.nameKey,
    ...countsOf(dimension),
  }));
}
