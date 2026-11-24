export type Tier = 1 | 2 | 3;

export type TierTarget = {
  tier: Tier;
  targetPercent: number;
  /** Tier 3 is tracked for visibility only — no pass/fail scoring. */
  scored: boolean;
};

export const TIER_TARGETS: readonly TierTarget[] = [
  { tier: 1, targetPercent: 95, scored: true },
  { tier: 2, targetPercent: 60, scored: true },
  { tier: 3, targetPercent: 0, scored: false },
];

export function isScoredTier(tier: Tier): boolean {
  return tier === 1 || tier === 2;
}
