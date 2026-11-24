import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import type { PropertyBag } from './recordsClient';
import {
  FakeRecordsClient,
  RUN_NEW,
  aggregateRow,
  drilldownRow,
  notApplicableRow,
  runRow,
  untieredDrilldownRow,
} from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');
const TS = '2026-10-12T06:00:00.000Z';

function serviceFor(rows: readonly PropertyBag[], tiering = true, runExtra: Partial<Record<string, string>> = {}) {
  const all = [runRow(RUN_NEW, TS, 'complete', runExtra), ...rows];
  const reader = new QualityStreamReader(new FakeRecordsClient(all), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW, getTieringEnabled: () => tiering });
}

const hierarchy = (extra: Partial<Parameters<typeof aggregateRow>[0]>) =>
  aggregateRow({ metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', facilityId: 'north', eligible: 0, passing: 0, ...extra });

describe('metrics with nothing eligible', () => {
  const onlyNotApplicable = [
    notApplicableRow('associationRate', 'equipment-asset', { facilityId: 'north' }),
    notApplicableRow('serialMfrCompleteness', 'equipment-asset', { facilityId: 'north', subKey: 'serialNumber' }),
  ];

  it('gives a dimension that has only not-applicable rows no score and no healthy status', async () => {
    const overview = await serviceFor(onlyNotApplicable).getOverview({});

    const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'equipment-asset');
    expect(card).toMatchObject({ enabled: true, status: 'not-applicable', overallPercent: null, issueCount: null, tierScores: [], untiered: null });
    expect(overview.overallAggregatePercent).toBeNull();
    expect(overview.scope.entityCount).toBe(0);
  });

  it('keeps a not-applicable dimension out of the portfolio score', async () => {
    const overview = await serviceFor([
      ...onlyNotApplicable,
      aggregateRow({ metricId: 'tsToAssetRate', dimensionId: 'time-series', facilityId: 'north', tier: 1, eligible: 10, passing: 9 }),
    ]).getOverview({});

    expect(overview.overallAggregatePercent).toBe(90);
    expect(overview.dimensionCards.find((c) => c.dimensionId === 'equipment-asset')?.status).toBe('not-applicable');
  });

  it('shows the dimension page as not applicable, with each metric flagged and no value', async () => {
    const dimension = await serviceFor(onlyNotApplicable).getDimension('equipment-asset', {});

    expect(dimension).toMatchObject({ enabled: true, status: 'not-applicable', entityCount: 0, overallPercent: null, tierScores: [] });
    expect(dimension.aggregateRows).toEqual([
      expect.objectContaining({ metricId: 'associationRate', notApplicable: true, valuePercent: null, status: 'not-applicable', eligibleCount: 0, issueCount: 0 }),
      expect.objectContaining({ metricId: 'serialMfrCompleteness', notApplicable: true, valuePercent: null, status: 'not-applicable' }),
    ]);
  });

  it('keeps a measured zero as a valid 0% result, not as not applicable', async () => {
    const dimension = await serviceFor([
      aggregateRow({ metricId: 'associationRate', dimensionId: 'equipment-asset', facilityId: 'north', tier: 1, eligible: 12, passing: 0 }),
    ]).getDimension('equipment-asset', {});

    expect(dimension.aggregateRows[0]).toMatchObject({ notApplicable: false, valuePercent: 0, eligibleCount: 12, passingCount: 0, status: 'critical' });
    expect(dimension.status).toBe('critical');
    expect(dimension.overallPercent).toBe(0);
  });

  it('treats a metric as measured when another unit measured it, and ignores the not-applicable row beside it', async () => {
    const dimension = await serviceFor([
      aggregateRow({ metricId: 'associationRate', dimensionId: 'equipment-asset', facilityId: 'north', tier: 1, eligible: 10, passing: 10 }),
      notApplicableRow('associationRate', 'equipment-asset', { facilityId: 'south' }),
    ]).getDimension('equipment-asset', {});

    expect(dimension.aggregateRows[0]).toMatchObject({ notApplicable: false, valuePercent: 100, eligibleCount: 10 });
    expect(dimension.status).toBe('good');
  });

  it('treats an empty row without the flag the same way, so nothing eligible never reads as healthy', async () => {
    const dimension = await serviceFor([aggregateRow({ metricId: 'associationRate', dimensionId: 'equipment-asset', tier: null, eligible: 0, passing: 0 })]).getDimension('equipment-asset', {});

    expect(dimension.status).toBe('not-applicable');
    expect(dimension.aggregateRows[0]).toMatchObject({ notApplicable: true, valuePercent: null });
  });

  it('says why when the run warnings do: an empty mapping, everything excluded, or nothing eligible', async () => {
    const warnings = JSON.stringify([
      { code: 'metric-not-applicable', message: 'm', subject: 'monitoringCoverage', reason: 'mapping-empty:tierByTypeCode', units: 1 },
      { code: 'metric-not-applicable', message: 'm', subject: 'associationRate', reason: 'all-excluded', units: 1 },
      { code: 'metric-not-applicable', message: 'm', subject: 'serialMfrCompleteness/serialNumber', reason: 'no-eligible-entities', units: 1 },
    ]);
    const rows = [
      ...onlyNotApplicable,
      notApplicableRow('monitoringCoverage', 'equipment-asset', { facilityId: 'north' }),
    ];

    const dimension = await serviceFor(rows, true, { warnings }).getDimension('equipment-asset', {});

    const reason = (metricId: string) => dimension.aggregateRows.find((row) => row.metricId === metricId)?.notApplicableReason;
    expect(reason('monitoringCoverage')).toEqual({ kind: 'mapping-empty', mapping: 'tierByTypeCode' });
    expect(reason('associationRate')).toEqual({ kind: 'all-excluded', mapping: null });
    expect(reason('serialMfrCompleteness')).toEqual({ kind: 'no-eligible-entities', mapping: null });
  });

  it('gives no reason for an older run whose marker has no warnings, and does not invent one', async () => {
    const dimension = await serviceFor(onlyNotApplicable).getDimension('equipment-asset', {});

    expect(dimension.aggregateRows.every((row) => row.notApplicable && row.notApplicableReason === null)).toBe(true);
  });

  it('gives a measured metric no reason even if a warning names it for another unit', async () => {
    const warnings = JSON.stringify([{ code: 'metric-not-applicable', message: 'm', subject: 'associationRate', reason: 'all-excluded', units: 1 }]);
    const dimension = await serviceFor(
      [aggregateRow({ metricId: 'associationRate', dimensionId: 'equipment-asset', facilityId: 'north', tier: 1, eligible: 10, passing: 5 })],
      true,
      { warnings }
    ).getDimension('equipment-asset', {});

    expect(dimension.aggregateRows[0]).toMatchObject({ notApplicable: false, notApplicableReason: null });
  });

  it('does not put a trend point on a not-applicable run', async () => {
    const points = await serviceFor(onlyNotApplicable).getSnapshots('associationRate', '30d', {});

    expect(points).toEqual([]);
  });

  it('does not fabricate a metric that has no row at all', async () => {
    const dimension = await serviceFor([aggregateRow({ metricId: 'tsToAssetRate', dimensionId: 'time-series', facilityId: 'north', tier: 1, eligible: 5, passing: 5 })]).getDimension('time-series', {});

    expect(dimension.aggregateRows.map((row) => row.metricId)).toEqual(['tsToAssetRate']);
  });
});

describe('untiered rows', () => {
  const tiered = [
    hierarchy({ tier: 1, eligible: 100, passing: 90 }),
    hierarchy({ tier: 2, eligible: 50, passing: 40 }),
  ];
  const untiered = hierarchy({ tier: null, eligible: 20, passing: 10 });

  it('shows untiered rows beside the tiers instead of dropping them', async () => {
    const overview = await serviceFor([...tiered, untiered]).getOverview({});

    const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');
    expect(card?.tierScores.map((tier) => tier.scorePercent)).toEqual([90, 80, null]);
    expect(card?.untiered).toEqual({ scorePercent: 50, eligibleCount: 20, passingCount: 10 });
    expect(card?.scoreBasis).toBe('tiered');
    expect(card?.overallPercent).toBe(85);
    expect(overview.untiered).toEqual({ scorePercent: 50, eligibleCount: 20, passingCount: 10 });
  });

  it('counts the untiered entities in the entity count and the dimension page', async () => {
    const svc = serviceFor([...tiered, untiered]);

    expect((await svc.getOverview({})).scope.entityCount).toBe(170);
    expect(await svc.getDimension('asset-hierarchy', {})).toMatchObject({
      entityCount: 170,
      untiered: { scorePercent: 50, eligibleCount: 20, passingCount: 10 },
      scoreBasis: 'tiered',
    });
  });

  it('pools all entities and says so when no tier 1 or 2 entity was measured (entirely untiered data)', async () => {
    const overview = await serviceFor([hierarchy({ tier: null, eligible: 40, passing: 20 })]).getOverview({});

    const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');
    expect(card).toMatchObject({ scoreBasis: 'pooled-fallback', overallPercent: 50, status: 'critical', weakestTier: null });
    expect(card?.tierScores.every((tier) => tier.scorePercent === null)).toBe(true);
    expect(card?.untiered).toEqual({ scorePercent: 50, eligibleCount: 40, passingCount: 20 });
    expect(overview.tierScores.every((tier) => tier.scorePercent === null)).toBe(true);
  });

  it('judges the pooled fallback against the same target as the tiering-off pool, and a healthy pool is good', async () => {
    const overview = await serviceFor([hierarchy({ tier: null, eligible: 40, passing: 40 })]).getOverview({});

    expect(overview.dimensionCards.find((c) => c.dimensionId === 'asset-hierarchy')).toMatchObject({ status: 'good', overallPercent: 100 });
  });

  it('is not changed for legacy data where every row has a tier', async () => {
    const card = (await serviceFor(tiered).getOverview({})).dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');

    expect(card).toMatchObject({ scoreBasis: 'tiered', untiered: null, overallPercent: 85 });
  });

  it('scores tier 3 rows only as tier 3, never as untiered', async () => {
    const card = (await serviceFor([...tiered, hierarchy({ tier: 3, eligible: 10, passing: 10 })]).getOverview({})).dimensionCards.find(
      (entry) => entry.dimensionId === 'asset-hierarchy'
    );

    expect(card?.tierScores.map((tier) => tier.scorePercent)).toEqual([90, 80, 100]);
    expect(card?.untiered).toBeNull();
  });

  it('pools untiered rows into the single score when tiering is off, with no separate untiered figure', async () => {
    const card = (await serviceFor([...tiered, untiered], false).getOverview({})).dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');

    // (90 + 40 + 10) / (100 + 50 + 20)
    expect(card).toMatchObject({ scoreBasis: 'pooled', overallPercent: 82, untiered: null, tierScores: [] });
  });

  it('keeps untiered rows in a slice filter by their own facility', async () => {
    const svc = serviceFor([...tiered, hierarchy({ tier: null, facilityId: 'south', eligible: 5, passing: 5 })]);

    const south = (await svc.getOverview({ facilityId: 'south' })).dimensionCards.find((c) => c.dimensionId === 'asset-hierarchy');

    expect(south).toMatchObject({ scoreBasis: 'pooled-fallback', overallPercent: 100 });
  });

  it('never reports an untiered drill-down row or issue as tier 3', async () => {
    const svc = serviceFor([
      drilldownRow('orphansByTier', 'asset-hierarchy', 'tiered-1', { tier: 1 }),
      untieredDrilldownRow('orphansByTier', 'asset-hierarchy', 'loose-1'),
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: 1, eligible: 1, passing: 0 }),
    ]);

    const drilldown = await svc.getDrilldown('orphansByTier', {});
    const issues = await svc.getIssues({});

    expect(drilldown.rows.map((row) => [row.id, row.tier, row.cells.tier])).toEqual([
      ['orphansByTier:tiered-1', 1, '1'],
      ['orphansByTier:loose-1', null, ''],
    ]);
    expect(issues.issues.find((issue) => issue.entityId === 'loose-1')?.tier).toBeNull();
    expect(issues.summary).toEqual({ total: 2, critical: 1, important: 0, standard: 0, unassigned: 1 });
  });

  it('does not count an untiered issue as a standard (tier 3) issue even when real tier 3 issues exist', async () => {
    const svc = serviceFor([
      drilldownRow('orphansByTier', 'asset-hierarchy', 'three', { tier: 3 }),
      untieredDrilldownRow('orphansByTier', 'asset-hierarchy', 'none'),
    ]);

    expect((await svc.getIssues({})).summary).toMatchObject({ standard: 1, unassigned: 1 });
  });
});

describe('run warnings', () => {
  it('returns the latest run warnings with the run time', async () => {
    const warnings = JSON.stringify([{ code: 'mapping-empty', message: 'm', subject: 'tierByTypeCode' }]);

    const result = await serviceFor([], true, { warnings }).getRunWarnings();

    expect(result.runTimestamp).toBe(TS);
    expect(result.warnings).toEqual([expect.objectContaining({ code: 'mapping-empty', subject: 'tierByTypeCode' })]);
  });

  it('returns none for a run that carries none, which is not a claim that there were none', async () => {
    await expect(serviceFor([]).getRunWarnings()).resolves.toEqual({ runTimestamp: TS, warnings: [] });
  });

  it('survives unknown codes and free text', async () => {
    const unknown = await serviceFor([], true, { warnings: '[{"code":"from-the-future","message":"x","extra":{"a":1}}]' }).getRunWarnings();
    const free = await serviceFor([], true, { warnings: 'old style note' }).getRunWarnings();

    expect(unknown.warnings[0]?.code).toBe('from-the-future');
    expect(free.warnings[0]?.code).toBe('free-text');
  });
});
