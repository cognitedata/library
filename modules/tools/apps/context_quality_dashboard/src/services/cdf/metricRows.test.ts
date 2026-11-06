import { describe, expect, it } from 'vitest';

import {
  matchesRowSlice,
  parseAggregateRow,
  parseDrilldownRow,
  parseExtraLinkRow,
  parseRunRow,
  sumRows,
} from './metricRows';
import { parseRunCoverage } from './runScope';
import { aggregateRow, extraLinkRow, notApplicableRow } from './testSupport';

describe(parseAggregateRow.name, () => {
  it('parses counts, optional slice fields and the tier', () => {
    const row = parseAggregateRow(
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', facilityId: 'north', tier: 2, eligible: 10, passing: 7 })
    );

    expect(row).toMatchObject({
      metricId: 'orphanRate',
      facilityId: 'north',
      divisionId: null,
      tier: 2,
      eligibleCount: 10,
      passingCount: 7,
      issueCount: 3,
    });
  });

  it('rejects a row that has no metric id', () => {
    expect(parseAggregateRow({ runId: 'r', runTimestamp: 't', dimensionId: 'd', reportingUnitId: 'u' })).toBeNull();
  });

  it('treats an unknown tier value as no tier', () => {
    const row = parseAggregateRow({ ...aggregateRow({ metricId: 'm', dimensionId: 'd', eligible: 1, passing: 1 }), tier: 9 });

    expect(row?.tier).toBeNull();
  });
});

describe(parseDrilldownRow.name, () => {
  it('falls back to the entity id for the label and leaves a missing tier unassigned', () => {
    const row = parseDrilldownRow({ metricId: 'orphansByTier', dimensionId: 'asset-hierarchy', entityId: 'a-1' });

    expect(row).toMatchObject({ entityLabel: 'a-1', tier: null, detail: '' });
  });

  it('never turns an absent or invalid tier into tier 3', () => {
    const base = { metricId: 'orphansByTier', dimensionId: 'asset-hierarchy', entityId: 'a-1' };

    expect(parseDrilldownRow(base)?.tier).toBeNull();
    expect(parseDrilldownRow({ ...base, tier: 9 })?.tier).toBeNull();
    expect(parseDrilldownRow({ ...base, tier: '2' })?.tier).toBeNull();
    expect(parseDrilldownRow({ ...base, tier: 3 })?.tier).toBe(3);
  });
});

describe('drilldown classifications', () => {
  const base = { metricId: 'orphansByTier', dimensionId: 'asset-hierarchy', entityId: 'a-1' };

  it('reads the classification labels stored as JSON', () => {
    const row = parseDrilldownRow({ ...base, cellsJson: '{"assetType":"Pump","assetClass":"Rotating equipment"}' });

    expect(row?.classes).toEqual({ assetType: 'Pump', assetClass: 'Rotating equipment' });
  });

  it('treats a missing, malformed or non-object value as no classification', () => {
    expect(parseDrilldownRow(base)?.classes).toEqual({});
    expect(parseDrilldownRow({ ...base, cellsJson: '{oops' })?.classes).toEqual({});
    expect(parseDrilldownRow({ ...base, cellsJson: '["Pump"]' })?.classes).toEqual({});
  });

  it('drops values that are not strings', () => {
    const row = parseDrilldownRow({ ...base, cellsJson: '{"assetType":"Pump","bad":3}' });

    expect(row?.classes).toEqual({ assetType: 'Pump' });
  });
});

describe(matchesRowSlice.name, () => {
  const row = { facilityId: 'north', divisionId: null, disciplineId: 'mechanical', ownerId: 'sap-steward' };

  it('matches when no filter is active', () => {
    expect(matchesRowSlice(row, {})).toBe(true);
  });

  it('requires equality on every active filter', () => {
    expect(matchesRowSlice(row, { facilityId: 'north', disciplineId: 'mechanical' })).toBe(true);
    expect(matchesRowSlice(row, { facilityId: 'south' })).toBe(false);
  });

  it('matches a row in one of the facilities of a location, and no other', () => {
    expect(matchesRowSlice(row, { facilityIds: ['north', 'south'] })).toBe(true);
    expect(matchesRowSlice(row, { facilityIds: ['south'] })).toBe(false);
    expect(matchesRowSlice(row, { facilityIds: [] })).toBe(false);
    expect(matchesRowSlice({ ...row, facilityId: null }, { facilityIds: ['north'] })).toBe(false);
  });

  it('requires both a chosen facility and the facilities of a chosen location', () => {
    expect(matchesRowSlice(row, { facilityId: 'north', facilityIds: ['north', 'south'] })).toBe(true);
    expect(matchesRowSlice(row, { facilityId: 'north', facilityIds: ['south'] })).toBe(false);
  });

  it('never matches an active filter when the row has no value for it', () => {
    expect(matchesRowSlice(row, { divisionId: 'prod-a' })).toBe(false);
  });
});

describe(sumRows.name, () => {
  it('adds eligible, passing, issue and value sums', () => {
    const rows = [
      { metricId: 'm', dimensionId: 'd', eligible: 10, passing: 6, valueSum: 1.5 },
      { metricId: 'm', dimensionId: 'd', eligible: 5, passing: 5, valueSum: 2 },
    ]
      .map((input) => parseAggregateRow(aggregateRow(input)))
      .flatMap((row) => (row === null ? [] : [row]));

    expect(sumRows(rows)).toEqual({ eligible: 15, passing: 11, issue: 4, valueSum: 3.5 });
  });

  it('returns zeros for no rows', () => {
    expect(sumRows([])).toEqual({ eligible: 0, passing: 0, issue: 0, valueSum: 0 });
  });
});

describe('notApplicable rows', () => {
  it('reads the flag, and treats an absent or non-true flag as a measured row', () => {
    const base = { metricId: 'monitoringCoverage', dimensionId: 'time-series' };

    expect(parseAggregateRow(notApplicableRow(base.metricId, base.dimensionId))).toMatchObject({
      notApplicable: true,
      eligibleCount: 0,
      passingCount: 0,
      tier: null,
    });
    expect(parseAggregateRow(aggregateRow({ ...base, eligible: 4, passing: 0 }))).toMatchObject({ notApplicable: false, eligibleCount: 4, passingCount: 0 });
    expect(parseAggregateRow({ ...aggregateRow({ ...base, eligible: 4, passing: 0 }), notApplicable: 'true' })?.notApplicable).toBe(false);
  });
});

describe('untiered aggregate rows', () => {
  it('keeps a row with no tier as untiered', () => {
    expect(parseAggregateRow(aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: null, eligible: 5, passing: 4 }))?.tier).toBeNull();
  });
});

describe(parseRunRow.name, () => {
  const base = { runId: 'r', runTimestamp: '2026-10-12T06:00:00.000Z' };

  it('has no warnings for a legacy marker without the property', () => {
    expect(parseRunRow(base)).toEqual({ ...base, warnings: [], coverage: parseRunCoverage(null) });
  });

  it('reads what the run measured from its scope', () => {
    const row = parseRunRow({ ...base, scope: 'files,time-series+configured' });

    expect([...(row?.coverage.dimensions ?? [])].sort()).toEqual(['files', 'time-series']);
    expect(row?.coverage.configured).toBe(true);
  });

  it('reads the warnings JSON text', () => {
    const row = parseRunRow({ ...base, warnings: '[{"code":"mapping-empty","message":"m","subject":"tierByTypeCode"}]' });

    expect(row?.warnings).toEqual([expect.objectContaining({ code: 'mapping-empty', subject: 'tierByTypeCode' })]);
  });

  it('does not fail on warnings that are not JSON', () => {
    expect(parseRunRow({ ...base, warnings: 'free text' })?.warnings).toEqual([expect.objectContaining({ code: 'free-text' })]);
  });
});

describe(parseExtraLinkRow.name, () => {
  it('reads the model, view and property the row names, whatever they are', () => {
    const row = parseExtraLinkRow(
      extraLinkRow({ model: 'my_space:MyModel:v2', view: 'MyView', property: 'owner', eligible: 10, passing: 4 })
    );

    expect(row).toMatchObject({
      model: 'my_space:MyModel:v2',
      modelLabel: 'MyModel',
      view: 'MyView',
      property: 'owner',
      facilityId: 'north',
      eligibleCount: 10,
      passingCount: 4,
      notApplicable: false,
    });
  });

  it('reads a not applicable row', () => {
    const row = parseExtraLinkRow(extraLinkRow({ view: 'V', property: 'p', eligible: 0, passing: 0, notApplicable: true }));

    expect(row?.notApplicable).toBe(true);
  });

  it('rejects a row without a property or without a model and view', () => {
    const good = extraLinkRow({ view: 'V', property: 'p', eligible: 1, passing: 1 });

    expect(parseExtraLinkRow({ ...good, metricId: undefined })).toBeNull();
    expect(parseExtraLinkRow({ ...good, dimensionId: 'no-view-here' })).toBeNull();
    expect(parseExtraLinkRow({ ...good, dimensionId: undefined })).toBeNull();
  });
});
