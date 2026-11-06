import { computeValuePercent } from '../../domain/metricMath';
import type { MockDataset } from '../../mock/dataset';
import type { DimensionId, ScoreBasis, SliceFilters, TierScore, UntieredScoreDto } from '../../types';
import { TIER_TARGETS } from '../../types';

/**
 * The untiered entities of a dimension in the active slice. Untiered entities have no division, discipline or owner, so, as
 * in CDF mode, any of those filters leaves them out; a facility filter keeps those of that facility.
 */
export function untieredPool(
  dataset: MockDataset,
  dimensionId: DimensionId,
  filters: SliceFilters
): UntieredScoreDto | null {
  if (filters.divisionId !== undefined || filters.disciplineId !== undefined || filters.ownerId !== undefined) {
    return null;
  }
  const pools = dataset.states.untieredPools.filter(
    (pool) =>
      pool.dimensionId === dimensionId && (filters.facilityId === undefined || pool.facilityId === filters.facilityId)
  );
  const eligibleCount = pools.reduce((sum, pool) => sum + pool.eligibleCount, 0);
  const passingCount = pools.reduce((sum, pool) => sum + pool.passingCount, 0);
  return eligibleCount === 0
    ? null
    : { scorePercent: computeValuePercent(passingCount, eligibleCount), eligibleCount, passingCount };
}

export type MockBreakdown = {
  overallPercent: number | null;
  statusScores: TierScore[];
  untiered: UntieredScoreDto | null;
  scoreBasis: ScoreBasis;
};

function pooledStatusScores(pooled: number | null): TierScore[] {
  return [
    {
      tier: 1,
      scorePercent: pooled,
      targetPercent: TIER_TARGETS.find((entry) => entry.tier === 1)?.targetPercent ?? 0,
      scored: pooled !== null,
    },
  ];
}

/** The same rules as CDF mode: tiered, pooled when tiering is off, or pooled as a fallback when no scored tier has data. */
export function buildBreakdown(input: {
  tierScores: readonly TierScore[];
  overallPercent: number | null;
  untiered: UntieredScoreDto | null;
  allTiers: { eligible: number; passing: number };
  tieringEnabled: boolean;
}): MockBreakdown {
  const { tierScores, overallPercent, untiered, allTiers, tieringEnabled } = input;
  if (!tieringEnabled) {
    const eligible = allTiers.eligible + (untiered?.eligibleCount ?? 0);
    const passing = allTiers.passing + (untiered?.passingCount ?? 0);
    const pooled = eligible === 0 ? overallPercent : computeValuePercent(passing, eligible);
    return { overallPercent: pooled, statusScores: pooledStatusScores(pooled), untiered: null, scoreBasis: 'pooled' };
  }
  const anyScored = tierScores.some((entry) => entry.scored && entry.scorePercent !== null);
  if (anyScored) {
    return { overallPercent, statusScores: [...tierScores], untiered, scoreBasis: 'tiered' };
  }
  const eligible = allTiers.eligible + (untiered?.eligibleCount ?? 0);
  const passing = allTiers.passing + (untiered?.passingCount ?? 0);
  const pooled = computeValuePercent(passing, eligible);
  return { overallPercent: pooled, statusScores: pooledStatusScores(pooled), untiered, scoreBasis: 'pooled-fallback' };
}
