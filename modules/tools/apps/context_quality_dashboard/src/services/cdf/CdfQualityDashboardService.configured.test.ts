import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, RUN_NEW, RUN_OLD, aggregateRow, extraLinkRow, runRow } from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');
const OTHER = 'my_space:MyModel:v2';

const ROWS = [
  runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
  aggregateRow({ metricId: 'tsToAssetRate', dimensionId: 'time-series', facilityId: 'north', eligible: 10, passing: 8 }),
  extraLinkRow({ view: 'CogniteAsset', property: 'type', facilityId: 'north', eligible: 100, passing: 90 }),
  extraLinkRow({ view: 'CogniteAsset', property: 'type', facilityId: 'south', eligible: 50, passing: 10 }),
  extraLinkRow({ view: 'CogniteAsset', property: 'assetClass', facilityId: 'north', eligible: 100, passing: 25 }),
  extraLinkRow({ view: 'CogniteEquipment', property: 'equipmentType', facilityId: 'north', eligible: 40, passing: 40 }),
  extraLinkRow({ model: OTHER, view: 'Widget', property: 'owner', facilityId: 'north', eligible: 0, passing: 0, notApplicable: true }),
];

function service(rows = ROWS): CdfQualityDashboardService {
  const reader = new QualityStreamReader(new FakeRecordsClient(rows), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW });
}

function flatGroups(dto: Awaited<ReturnType<CdfQualityDashboardService['getConfiguredMetrics']>>) {
  return dto.models.flatMap((model) => model.groups);
}

describe('configured metrics', () => {
  it('makes one group per model and view found in the run, with one metric per property', async () => {
    const groups = flatGroups(await service().getConfiguredMetrics({}));

    expect(groups.map((group) => [group.modelLabel, group.viewLabel, group.metrics.map((metric) => metric.label)])).toEqual([
      ['CogniteCore', 'CogniteAsset', ['type', 'assetClass']],
      ['CogniteCore', 'CogniteEquipment', ['equipmentType']],
      ['MyModel', 'Widget', ['owner']],
    ]);
  });

  it('returns one data model entry per distinct model id', async () => {
    const { models } = await service().getConfiguredMetrics({});

    expect(models.map((model) => [model.id, model.label, model.groups.length])).toEqual([
      ['cdf_cdm:CogniteCore:v1', 'CogniteCore', 2],
      [OTHER, 'MyModel', 1],
    ]);
  });

  it('adds the facilities together', async () => {
    const groups = flatGroups(await service().getConfiguredMetrics({}));

    expect(groups[0]?.metrics[0]).toMatchObject({ label: 'type', totalCount: 150, linkedCount: 100, coveragePercent: 67 });
  });

  it('applies the facility filter', async () => {
    const groups = flatGroups(await service().getConfiguredMetrics({ facilityId: 'south' }));

    expect(groups).toHaveLength(1);
    expect(groups[0]?.metrics).toMatchObject([{ label: 'type', totalCount: 50, linkedCount: 10, coveragePercent: 20 }]);
  });

  it('gives a metric with nothing to count no percentage instead of 0', async () => {
    const groups = flatGroups(await service().getConfiguredMetrics({}));

    expect(groups[2]?.metrics[0]).toMatchObject({ label: 'owner', totalCount: 0, coveragePercent: null });
  });

  it('shows nothing under a division, discipline or owner filter, which these metrics are not split by', async () => {
    const { models } = await service().getConfiguredMetrics({ divisionId: 'prod-a' });

    expect(models).toEqual([]);
  });

  it('shows nothing for a run that was started without a config file', async () => {
    const { models } = await service([ROWS[0], ROWS[1]].filter((row) => row !== undefined)).getConfiguredMetrics({});

    expect(models).toEqual([]);
  });

  it('keeps ids unique when two models have the same view and property', async () => {
    const rows = [
      runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
      extraLinkRow({ view: 'Asset', property: 'type', eligible: 1, passing: 1 }),
      extraLinkRow({ model: OTHER, view: 'Asset', property: 'type', eligible: 2, passing: 1 }),
    ];

    const groups = flatGroups(await service(rows).getConfiguredMetrics({}));

    expect(new Set(groups.map((group) => group.id)).size).toBe(2);
    expect(groups.every((group) => group.modelId.length > 0)).toBe(true);
  });

  it('returns per-run coverage history for one configured metric', async () => {
    const rows = [
      runRow(RUN_OLD, '2026-10-01T06:00:00.000Z'),
      runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
      { ...extraLinkRow({ view: 'CogniteAsset', property: 'type', facilityId: 'north', eligible: 100, passing: 50 }), runId: RUN_OLD, runTimestamp: '2026-10-01T06:00:00.000Z' },
      extraLinkRow({ view: 'CogniteAsset', property: 'type', facilityId: 'north', eligible: 100, passing: 90 }),
    ];

    const points = await service(rows).getConfiguredMetricTrends(
      { modelId: 'cdf_cdm:CogniteCore:v1', view: 'CogniteAsset', property: 'type' },
      '30d',
      { facilityId: 'north' }
    );

    expect(points).toEqual([
      {
        capturedAt: '2026-10-01T06:00:00.000Z',
        valuePercent: 50,
        linkedCount: 50,
        totalCount: 100,
      },
      {
        capturedAt: '2026-10-12T06:00:00.000Z',
        valuePercent: 90,
        linkedCount: 90,
        totalCount: 100,
      },
    ]);
  });
});
