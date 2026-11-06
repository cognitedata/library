/** Percent of eligible entities that pass, rounded; null when nothing is eligible. */
export function computeValuePercent(passingCount: number, eligibleCount: number): number | null {
  if (eligibleCount === 0) {
    return null;
  }
  return Math.round((passingCount / eligibleCount) * 100);
}
