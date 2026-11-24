import type { DimensionCardDto, DimensionId, DimensionQualityStatus, Tier, TierScore } from '../types';

export const ALL_DIMENSION_IDS: readonly DimensionId[] = [
  'asset-hierarchy',
  'equipment-asset',
  'time-series',
  'maintenance',
  'file-annotation',
  'model-3d',
  'files',
];

export const OPTIONAL_DIMENSION_IDS: readonly DimensionId[] = [
  'maintenance',
  'file-annotation',
  'model-3d',
];

export function isOptionalDimension(dimensionId: DimensionId): boolean {
  return OPTIONAL_DIMENSION_IDS.includes(dimensionId);
}

export function computeDimensionStatus(
  tierScores: readonly TierScore[]
): Exclude<DimensionQualityStatus, 'disabled'> {
  const tier1 = tierScores.find((entry) => entry.tier === 1);
  if (tier1 === undefined || !tier1.scored || tier1.scorePercent === null) {
    return 'good';
  }
  if (tier1.scorePercent < tier1.targetPercent - 15) {
    return 'critical';
  }
  if (tier1.scorePercent < tier1.targetPercent) {
    return 'weak';
  }
  return 'good';
}

export function findWeakestTier(tierScores: readonly TierScore[]): Tier | null {
  const scored = tierScores.filter(
    (entry) => entry.scored && entry.scorePercent !== null
  );
  if (scored.length === 0) {
    return null;
  }

  let weakest: TierScore | null = null;
  for (const entry of scored) {
    if (
      weakest === null ||
      (entry.scorePercent ?? 0) < (weakest.scorePercent ?? 0)
    ) {
      weakest = entry;
    }
  }
  return weakest?.tier ?? null;
}

export function computePortfolioAggregatePercent(
  dimensionCards: readonly DimensionCardDto[]
): number | null {
  const scored = dimensionCards.filter(
    (card) => card.enabled && card.overallPercent !== null
  );
  if (scored.length === 0) {
    return null;
  }

  const total = scored.reduce((sum, card) => sum + (card.overallPercent ?? 0), 0);
  return Math.round(total / scored.length);
}
