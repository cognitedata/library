import { describe, expect, it } from 'vitest';

import { buildDimensionCardCounts } from './dimensionCardCounts';
import type { DimensionPresentation } from './overviewPresentation';

function dimension(extra: Partial<DimensionPresentation>): DimensionPresentation {
  return {
    dimensionId: 'asset-hierarchy',
    nameKey: 'dimension.asset-hierarchy',
    enabled: true,
    optional: false,
    overallPercent: 94,
    issueCount: 72,
    ...extra,
  };
}

describe(buildDimensionCardCounts.name, () => {
  it('uses what the headline metric measured, not a population worked back from the rounded percentage', () => {
    const [counts] = buildDimensionCardCounts([dimension({ eligibleCount: 1119, passingCount: 1047 })]);

    // 72 issues at "94%" would give 1,200 if the population were estimated; the real figure is 1,119
    expect(counts).toMatchObject({
      inScopeCount: 1119,
      contextualisedCount: 1047,
      unresolvedCount: 72,
      contextualisedPercent: 94,
    });
  });

  it('has no percentage when nothing was eligible', () => {
    const [counts] = buildDimensionCardCounts([dimension({ eligibleCount: 0, passingCount: 0, overallPercent: null })]);

    expect(counts?.contextualisedPercent).toBeNull();
    expect(counts?.inScopeCount).toBe(0);
  });

  it('never lets passing items exceed the items measured', () => {
    const [counts] = buildDimensionCardCounts([dimension({ eligibleCount: 10, passingCount: 12 })]);

    expect(counts).toMatchObject({ contextualisedCount: 10, unresolvedCount: 0 });
  });

  it('works the population back from the percentage and the issues only when the counts are not given', () => {
    const [counts] = buildDimensionCardCounts([dimension({ overallPercent: 90, issueCount: 10 })]);

    expect(counts).toMatchObject({ inScopeCount: 100, contextualisedCount: 90, unresolvedCount: 10, contextualisedPercent: 90 });
  });
});
