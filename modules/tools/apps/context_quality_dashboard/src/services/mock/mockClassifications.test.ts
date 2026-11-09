import { describe, expect, it } from 'vitest';

import { buildMockClassificationGroups, buildMockClassificationRows } from './mockClassifications';

describe(buildMockClassificationRows.name, () => {
  it('allocates the real in-scope total across classifications, with no leftover or overflow', () => {
    const rows = buildMockClassificationRows(105);

    const total = rows.reduce((sum, row) => sum + row.totalCount, 0);
    expect(total).toBe(105);
  });

  it('never reports more contextualised items than the row total', () => {
    const rows = buildMockClassificationRows(105);

    for (const row of rows) {
      expect(row.contextualisedCount).toBeLessThanOrEqual(row.totalCount);
    }
  });

  it('returns five distinct classifications', () => {
    const rows = buildMockClassificationRows(105);

    expect(rows).toHaveLength(5);
    expect(new Set(rows.map((row) => row.id)).size).toBe(5);
  });

  it('handles a zero in-scope total without negative or NaN counts', () => {
    const rows = buildMockClassificationRows(0);

    for (const row of rows) {
      expect(row.totalCount).toBe(0);
      expect(row.contextualisedCount).toBe(0);
    }
  });

  it('scales up cleanly for a larger in-scope total', () => {
    const rows = buildMockClassificationRows(8000);

    const total = rows.reduce((sum, row) => sum + row.totalCount, 0);
    expect(total).toBe(8000);
  });
});

describe(buildMockClassificationGroups.name, () => {
  it('returns a single asset class group', () => {
    const groups = buildMockClassificationGroups(105);

    expect(groups.map((group) => group.kind)).toEqual(['assetClass']);
  });
});
