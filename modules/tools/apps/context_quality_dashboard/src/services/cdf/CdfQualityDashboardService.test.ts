import { describe, expect, it } from 'vitest';

import { ALL_DIMENSION_IDS } from '../../domain/dimensions';

import { CdfCatalogService } from './CdfCatalogService';
import { CdfQualityDashboardService, LIST_LIMIT_PER_METRIC } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import {
  FakeRecordsClient,
  RUN_NEW,
  RUN_OLD,
  aggregateRow,
  classificationRow,
  drilldownRow,
  exclusionRow,
  runRow,
} from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');
const OLD_TS = '2026-10-05T06:00:00.000Z';

const ROWS = [
  runRow(RUN_OLD, OLD_TS),
  runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
  // asset hierarchy, two facilities, tiers 1 and 2
  aggregateRow({ metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', facilityId: 'north', tier: 1, eligible: 100, passing: 90, divisionId: 'prod-a' }),
  aggregateRow({ metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', facilityId: 'north', tier: 2, eligible: 50, passing: 40, divisionId: 'prod-a' }),
  aggregateRow({ metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', facilityId: 'south', tier: 1, eligible: 100, passing: 50, divisionId: 'prod-b' }),
  aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', facilityId: 'north', tier: 1, eligible: 100, passing: 95 }),
  // time series
  aggregateRow({ metricId: 'tsToAssetRate', dimensionId: 'time-series', facilityId: 'north', tier: 1, eligible: 200, passing: 150 }),
  // file annotation: only the Approved sub-key represents approvalMix
  aggregateRow({ metricId: 'approvalMix', dimensionId: 'file-annotation', facilityId: 'north', subKey: 'Approved', eligible: 100, passing: 60 }),
  aggregateRow({ metricId: 'approvalMix', dimensionId: 'file-annotation', facilityId: 'north', subKey: 'Rejected', eligible: 100, passing: 10 }),
  aggregateRow({ metricId: 'confidenceAverage', dimensionId: 'file-annotation', facilityId: 'north', eligible: 100, passing: 100, valueSum: 80 }),
  // history for the trend: old run and new run of one headline metric
  aggregateRow({ runId: RUN_OLD, runTimestamp: OLD_TS, metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', facilityId: 'north', tier: 1, eligible: 100, passing: 80 }),
  exclusionRow('time-series', 'synthetic-timeseries', 9),
  exclusionRow('files', 'user:file-extension', 4),
  drilldownRow('orphansByTier', 'asset-hierarchy', 'orph-1', { cellsJson: '{"assetType":"Pump","assetClass":"Rotating"}' }),
  drilldownRow('orphansByTier', 'asset-hierarchy', 'orph-2', { facilityId: 'south', tier: 2 }),
  drilldownRow('activeChildrenUnderRetiredParents', 'asset-hierarchy', 'kid-1'),
  classificationRow({ kind: 'assetType', id: 'pump', name: 'Pump', facilityId: 'north', eligible: 80, passing: 60 }),
  classificationRow({ kind: 'assetType', id: 'pump', name: 'Pump', facilityId: 'south', eligible: 20, passing: 20, tier: 2 }),
  classificationRow({ kind: 'assetType', id: 'valve', name: 'Valve', facilityId: 'north', eligible: 40, passing: 10 }),
  classificationRow({ kind: 'equipmentType', id: 'plc', name: 'Plc', facilityId: 'north', eligible: 30, passing: 27 }),
];

function countingService() {
  const client = new FakeRecordsClient(ROWS);
  const svc = new CdfQualityDashboardService({ reader: new QualityStreamReader(client, () => NOW), now: () => NOW });
  return { svc, queries: client.queries };
}

function service(getTieringEnabled?: () => boolean) {
  const reader = new QualityStreamReader(new FakeRecordsClient(ROWS), () => NOW);
  return { svc: new CdfQualityDashboardService({ reader, now: () => NOW, getTieringEnabled }), reader };
}

describe(CdfQualityDashboardService.name, () => {
  describe('with tiering turned off', () => {
    it('pools every tier into one overall score and drops the tier breakdown', async () => {
      const { svc } = service(() => false);

      const overview = await svc.getOverview({});

      const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');
      expect(card?.tierScores).toEqual([]);
      // (90 + 40 + 50) / (100 + 50 + 100)
      expect(card?.overallPercent).toBe(72);
      expect(card?.weakestTier).toBeNull();
      expect(overview.tierScores).toEqual([]);
    });

    it('returns no tier rows for a dimension but keeps its overall score', async () => {
      const { svc } = service(() => false);

      const dimension = await svc.getDimension('asset-hierarchy', {});

      expect(dimension.tierScores).toEqual([]);
      expect(dimension.overallPercent).toBe(72);
      expect(dimension.status).toBe('critical');
    });
  });

  describe('getOverview', () => {
    it('scores each tier from the dimension headline metric', async () => {
      const overview = await service().svc.getOverview({});

      const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');
      expect(card?.tierScores.map((tier) => tier.scorePercent)).toEqual([
        // tier 1: (90 + 50) / (100 + 100), tier 2: 40 / 50, tier 3: none
        70, 80, null,
      ]);
      expect(card?.overallPercent).toBe(75);
      expect(card?.status).toBe('critical');
    });

    it('counts entities from the headline metrics and fills the portfolio aggregate', async () => {
      const overview = await service().svc.getOverview({});

      // 250 assets + 200 time series + 100 annotations
      expect(overview.scope.entityCount).toBe(550);
      expect(overview.overallAggregatePercent).not.toBeNull();
    });

    it('narrows to one facility', async () => {
      const overview = await service().svc.getOverview({ facilityId: 'south' });

      const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');
      expect(card?.tierScores[0]?.scorePercent).toBe(50);
      expect(overview.scope.facilityId).toBe('south');
    });

    it('marks a dimension with no rows as disabled rather than scoring it 0%', async () => {
      const overview = await service().svc.getOverview({});

      const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'model-3d');
      expect(card).toMatchObject({ enabled: false, status: 'disabled', overallPercent: null });
    });

    it('splits system and user exclusions', async () => {
      const overview = await service().svc.getOverview({});

      expect(overview.excludedSummary).toEqual([
        expect.objectContaining({ kind: 'synthetic-timeseries', count: 9 }),
      ]);
      expect(overview.userExclusionSummary).toEqual([
        expect.objectContaining({ id: 'user-file-extension', count: 4 }),
      ]);
    });

    it('does not read the portfolio trend: nothing on the Overview shows it', async () => {
      const { svc, queries } = countingService();

      const overview = await svc.getOverview({ facilityId: 'north' });

      expect(overview.overallTrend).toBeNull();
      expect(overview.headlineSnapshots).toEqual([]);
      expect(queries.some((query) => query.equals.metricId === undefined && query.equals.rowKind === 'aggregate')).toBe(true);
      expect(queries.filter((query) => query.equals.rowKind === 'aggregate' && query.equals.metricId !== undefined)).toEqual([]);
    });

    it('leaves insights and alerts empty because the stream does not produce them', async () => {
      const overview = await service().svc.getOverview({});

      expect(overview.insights).toEqual([]);
      expect(overview.alertSummaries).toEqual([]);
    });
  });

  describe('getDimension', () => {
    it('builds one aggregate row per metric with percentages computed from counts', async () => {
      const dimension = await service().svc.getDimension('asset-hierarchy', { facilityId: 'north' });

      const hierarchy = dimension.aggregateRows.find((row) => row.metricId === 'hierarchyCompletionRate');
      expect(hierarchy).toMatchObject({ eligibleCount: 150, passingCount: 130, issueCount: 20, valuePercent: 87 });
      expect(dimension.aggregateRows.map((row) => row.metricId)).toContain('orphanRate');
    });

    it('uses only the Approved sub-key for approvalMix', async () => {
      const dimension = await service().svc.getDimension('file-annotation', {});

      const mix = dimension.aggregateRows.find((row) => row.metricId === 'approvalMix');
      expect(mix).toMatchObject({ eligibleCount: 100, passingCount: 60, valuePercent: 60 });
    });

    it('derives confidenceAverage from the stored sum of confidences', async () => {
      const dimension = await service().svc.getDimension('file-annotation', {});

      const confidence = dimension.aggregateRows.find((row) => row.metricId === 'confidenceAverage');
      expect(confidence?.valuePercent).toBe(80);
    });

    it('reads the lifecycle lists apart from the dimension, and only those', async () => {
      const { svc, queries } = countingService();

      const dimension = await svc.getDimension('asset-hierarchy', {});
      expect(dimension.enabled).toBe(true);
      expect(queries.filter((query) => query.equals.rowKind === 'drilldown')).toEqual([]);

      const warnings = await svc.getLifecycleWarnings('asset-hierarchy', {});
      expect(warnings.map((warning) => warning.metricId)).toEqual(['activeChildrenUnderRetiredParents']);
      expect(warnings[0]).toMatchObject({ count: 1, atLeast: false });
      expect(
        queries.filter((query) => query.equals.rowKind === 'drilldown').map((query) => query.equals.metricId)
      ).toEqual(['activeChildrenUnderRetiredParents']);
    });

    it('reports an empty dimension as disabled', async () => {
      const dimension = await service().svc.getDimension('model-3d', {});

      expect(dimension).toMatchObject({ enabled: false, aggregateRows: [] });
    });
  });

  describe('getDrilldown', () => {
    it('maps rows to cells and applies the slice', async () => {
      const drilldown = await service().svc.getDrilldown('orphansByTier', { facilityId: 'north' });

      expect(drilldown.rows).toHaveLength(1);
      expect(drilldown.rows[0]).toMatchObject({
        id: 'orphansByTier:orph-1',
        tier: 1,
        cells: { entity: 'Label orph-1', owner: 'sap-steward' },
      });
      expect(drilldown.dimensionId).toBe('asset-hierarchy');
    });

    it('adds a column for each classification kind the rows carry, and the labels on each row', async () => {
      const drilldown = await service().svc.getDrilldown('orphansByTier', {});

      expect(drilldown.columnKeys).toEqual(['entity', 'tier', 'assetClass', 'assetType', 'detail', 'owner']);
      const classified = drilldown.rows.find((row) => row.id === 'orphansByTier:orph-1');
      expect(classified?.cells).toMatchObject({ assetType: 'Pump', assetClass: 'Rotating' });
      expect(classified?.classes).toEqual({ assetType: 'Pump', assetClass: 'Rotating' });
      const unclassified = drilldown.rows.find((row) => row.id === 'orphansByTier:orph-2');
      expect(unclassified?.classes).toEqual({});
    });

    it('keeps the plain columns when no row is classified', async () => {
      const drilldown = await service().svc.getDrilldown('activeChildrenUnderRetiredParents', {});

      expect(drilldown.columnKeys).toEqual(['entity', 'tier', 'detail', 'owner']);
    });
  });

  describe('getSnapshots', () => {
    it('returns one point per completed run, oldest first, inside the window', async () => {
      const points = await service().svc.getSnapshots('hierarchyCompletionRate', '30d', { facilityId: 'north' });

      expect(points.map((point) => point.valuePercent)).toEqual([80, 87]);
      expect(points[0]?.capturedAt).toBe(OLD_TS);
    });

    it('drops runs outside the window', async () => {
      // the old run is 7 days and 1 hour old at the pinned time, so only the new run is in a 7-day window
      const points = await service().svc.getSnapshots('hierarchyCompletionRate', '7d', { facilityId: 'north' });

      expect(points.map((point) => point.valuePercent)).toEqual([87]);
      const shortNow = new CdfQualityDashboardService({
        reader: new QualityStreamReader(new FakeRecordsClient(ROWS), () => NOW),
        now: () => Date.parse('2026-11-20T00:00:00.000Z'),
      });
      await expect(shortNow.getSnapshots('hierarchyCompletionRate', '7d', {})).resolves.toEqual([]);
    });
  });

  describe('getClassifications', () => {
    it('groups rows by kind, merging the same class across facilities and tiers', async () => {
      const { groups } = await service().svc.getClassifications({});

      expect(groups.map((group) => group.kind)).toEqual(['assetType', 'equipmentType']);
      const pump = groups[0]?.rows.find((row) => row.id === 'pump');
      expect(pump).toMatchObject({ label: 'Pump', totalCount: 100, contextualisedCount: 80, coveragePercent: 80 });
    });

    it('sorts the largest class first', async () => {
      const { groups } = await service().svc.getClassifications({});

      expect(groups[0]?.rows.map((row) => row.id)).toEqual(['pump', 'valve']);
    });

    it('narrows to a facility', async () => {
      const { groups } = await service().svc.getClassifications({ facilityId: 'south' });

      expect(groups).toHaveLength(1);
      expect(groups[0]?.rows).toEqual([
        expect.objectContaining({ id: 'pump', totalCount: 20, coveragePercent: 100 }),
      ]);
    });

    it('folds the smallest classes into Other once there are too many to read', async () => {
      const many = Array.from({ length: 12 }, (_, index) =>
        classificationRow({ kind: 'assetType', id: `t${index}`, facilityId: 'north', eligible: 100 - index, passing: 50 })
      );
      const reader = new QualityStreamReader(
        new FakeRecordsClient([runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'), ...many]),
        () => NOW
      );
      const svc = new CdfQualityDashboardService({ reader, now: () => NOW });

      const { groups } = await svc.getClassifications({});

      const rows = groups[0]?.rows ?? [];
      expect(rows).toHaveLength(8);
      expect(rows[rows.length - 1]).toMatchObject({ id: '__other__', totalCount: 93 + 92 + 91 + 90 + 89 });
    });

    it('returns no groups when the run has no classification rows', async () => {
      const reader = new QualityStreamReader(
        new FakeRecordsClient([runRow(RUN_NEW, '2026-10-12T06:00:00.000Z')]),
        () => NOW
      );
      const svc = new CdfQualityDashboardService({ reader, now: () => NOW });

      expect(await svc.getClassifications({})).toEqual({ groups: [] });
    });
  });

  describe('getIssues', () => {
    it('carries each issue\'s classification labels', async () => {
      const { issues } = await service().svc.getIssues({});

      expect(issues.find((issue) => issue.entityId === 'orph-1')?.classes).toEqual({
        assetType: 'Pump',
        assetClass: 'Rotating',
      });
      expect(issues.find((issue) => issue.entityId === 'orph-2')?.classes).toEqual({});
    });

    it('lists drill-down rows as issues with their kind and tier counts', async () => {
      const issues = await service().svc.getIssues({});

      expect(issues.issues.map((issue) => issue.kind).sort()).toEqual([
        'active-child-retired-parent',
        'orphan-asset',
        'orphan-asset',
      ]);
      expect(issues.summary).toEqual({ total: 3, critical: 2, important: 1, standard: 0, unassigned: 0 });
    });
  });

  describe('getIssues with many items', () => {
    function many(count: number, facilityId = 'north') {
      return Array.from({ length: count }, (_, i) =>
        drilldownRow('staleTimeSeries', 'time-series', `${facilityId}-ts-${i}`, { facilityId })
      );
    }

    it('reads each metric up to a limit instead of failing, and says the list is partial', async () => {
      const rows = [runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'), ...many(LIST_LIMIT_PER_METRIC + 50)];
      const reader = new QualityStreamReader(new FakeRecordsClient(rows, 250), () => NOW);
      const svc = new CdfQualityDashboardService({ reader, now: () => NOW });

      const issues = await svc.getIssues({});

      expect(issues.issues).toHaveLength(LIST_LIMIT_PER_METRIC);
      expect(issues.partial?.limitPerMetric).toBe(LIST_LIMIT_PER_METRIC);
      expect(issues.partial?.cutMetricIds).toEqual(['staleTimeSeries']);
    });

    it('is complete, with no notice, when every metric fits', async () => {
      const rows = [runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'), ...many(3)];
      const reader = new QualityStreamReader(new FakeRecordsClient(rows), () => NOW);
      const svc = new CdfQualityDashboardService({ reader, now: () => NOW });

      const issues = await svc.getIssues({});

      expect(issues.issues).toHaveLength(3);
      expect(issues.partial).toBeNull();
    });

    it('spends the limit inside the chosen facility or location, not across all of them', async () => {
      const rows = [
        runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
        ...many(LIST_LIMIT_PER_METRIC + 10, 'north'),
        ...many(5, 'south'),
      ];
      const reader = new QualityStreamReader(new FakeRecordsClient(rows), () => NOW);
      const svc = new CdfQualityDashboardService({ reader, now: () => NOW });

      const south = await svc.getIssues({ facilityId: 'south' });
      const location = await svc.getIssues({ facilityIds: ['south'] });

      expect(south.issues).toHaveLength(5);
      expect(south.partial).toBeNull();
      expect(location.issues).toHaveLength(5);
      expect(await svc.getIssues({ facilityId: 'north', facilityIds: ['south'] })).toMatchObject({ issues: [] });
    });
  });

  describe('a dimension and a drill-down with more items than a page', () => {
    const stale = Array.from({ length: LIST_LIMIT_PER_METRIC + 50 }, (_, i) =>
      drilldownRow('staleTimeSeries', 'time-series', `ts-${i}`)
    );
    const rows = [
      runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
      aggregateRow({ metricId: 'tsToAssetRate', dimensionId: 'time-series', facilityId: 'north', tier: 1, eligible: 200, passing: 150 }),
      ...stale,
    ];
    const svc = () =>
      new CdfQualityDashboardService({
        reader: new QualityStreamReader(new FakeRecordsClient(rows, 250), () => NOW),
        now: () => NOW,
      });

    it('loads the dimension without reading its lists', async () => {
      const dimension = await svc().getDimension('time-series', {});

      expect(dimension.enabled).toBe(true);
    });

    it('reads the first items of the list and says it is partial', async () => {
      const drilldown = await svc().getDrilldown('staleTimeSeries', {});

      expect(drilldown.rows).toHaveLength(LIST_LIMIT_PER_METRIC);
      expect(drilldown.partial).toEqual({ limit: LIST_LIMIT_PER_METRIC });
    });

    it('has no notice when the list is complete', async () => {
      const complete = new CdfQualityDashboardService({
        reader: new QualityStreamReader(new FakeRecordsClient([runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'), ...stale.slice(0, 3)]), () => NOW),
        now: () => NOW,
      });

      expect((await complete.getDrilldown('staleTimeSeries', {})).partial).toBeNull();
    });
  });

  describe('run status and unsupported parts', () => {
    it('reports the latest completed run as the last success', async () => {
      await expect(service().svc.getRunStatus()).resolves.toEqual({
        lastSuccessAt: '2026-10-12T06:00:00.000Z',
        inProgress: false,
        scope: { kind: 'all' },
        // a run that names no scope measured every dimension
        dimensionRuns: Object.fromEntries(ALL_DIMENSION_IDS.map((id) => [id, '2026-10-12T06:00:00.000Z'])),
      });
    });

    it('returns no alerts or custom rules', async () => {
      const { svc } = service();

      await expect(svc.getAlerts({})).resolves.toMatchObject({ alerts: [], summary: { total: 0 } });
      await expect(svc.getCustomQualityMetrics({})).resolves.toEqual([]);
      await expect(svc.getCustomRuleDrilldown('r1', {})).rejects.toThrow('not available');
    });
  });
});

describe(CdfCatalogService.name, () => {
  it('derives the filter options from the latest run and discovers no data models', async () => {
    const { reader } = service();
    const catalog = new CdfCatalogService({ reader });

    await expect(catalog.getFacilities()).resolves.toEqual([
      { id: 'north', name: 'North' },
      { id: 'south', name: 'South' },
    ]);
    await expect(catalog.getDivisions()).resolves.toEqual([
      { id: 'prod-a', name: 'Prod A' },
      { id: 'prod-b', name: 'Prod B' },
    ]);
    await expect(catalog.getDataModels()).resolves.toEqual([]);
    await expect(catalog.getViews('any_space')).resolves.toEqual([]);
  });
});
