import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, extraLinkGapRow, extraLinkRow, runRow } from './testSupport';

const NOW = Date.parse('2026-11-05T10:00:00.000Z');
const FULL = 'run-config-all';
const FULL_AT = '2026-11-04T06:00:00.000Z';
const ONE = 'run-config-one';
const ONE_AT = '2026-11-05T07:47:26.000Z';
const KEY = { modelId: 'cdf_cdm:CogniteCore:v1', view: 'CogniteAsset' };

/** A run of all three configured metrics, then a run that measured one of them again. */
const ROWS = [
  runRow(FULL, FULL_AT, 'complete', { scope: 'all+configured' }),
  runRow(ONE, ONE_AT, 'complete', { scope: 'configured' }),
  inRun(FULL, FULL_AT, extraLinkRow({ view: 'CogniteAsset', property: 'type', eligible: 100, passing: 80 })),
  inRun(FULL, FULL_AT, extraLinkRow({ view: 'CogniteAsset', property: 'assetClass', eligible: 100, passing: 60 })),
  inRun(FULL, FULL_AT, extraLinkRow({ view: 'CogniteEquipment', property: 'equipmentType', eligible: 40, passing: 30 })),
  inRun(ONE, ONE_AT, extraLinkRow({ view: 'CogniteAsset', property: 'type', eligible: 110, passing: 105 })),
  inRun(FULL, FULL_AT, extraLinkGapRow({ view: 'CogniteAsset', property: 'assetClass', entityId: 'asset-old' })),
  inRun(FULL, FULL_AT, extraLinkGapRow({ view: 'CogniteAsset', property: 'type', entityId: 'type-old' })),
  inRun(ONE, ONE_AT, extraLinkGapRow({ view: 'CogniteAsset', property: 'type', entityId: 'type-new' })),
];

describe('a run of some of the configured metrics', () => {
  it('keeps the other configured metrics as the run that measured them last', async () => {
    // Act
    const { models } = await service().getConfiguredMetrics({});

    // Assert
    const metrics = models.flatMap((model) => model.groups.flatMap((group) => group.metrics));
    expect(metrics.map((metric) => `${metric.view}.${metric.label}`).sort()).toEqual([
      'CogniteAsset.assetClass',
      'CogniteAsset.type',
      'CogniteEquipment.equipmentType',
    ]);
    expect(metric(metrics, 'assetClass')?.totalCount).toBe(100);
    expect(metric(metrics, 'assetClass')?.linkedCount).toBe(60);
  });

  it('shows a metric the later run measured as that run measured it, not as the earlier one did', async () => {
    const { models } = await service().getConfiguredMetrics({});

    const type = metric(
      models.flatMap((model) => model.groups.flatMap((group) => group.metrics)),
      'type'
    );
    expect(type?.totalCount).toBe(110); // the run of one metric, not the 100 of the full run
    expect(type?.linkedCount).toBe(105);
  });

  it('lists every configured metric as the configuration, from the latest run that measured any of them', async () => {
    const config = await service().getLatestRunConfiguration();

    expect(config?.runTimestamp).toBe(ONE_AT);
    expect(config?.models).toEqual([
      {
        model: 'cdf_cdm:CogniteCore:v1',
        views: [
          { view: 'CogniteAsset', properties: ['assetClass', 'type'] },
          { view: 'CogniteEquipment', properties: ['equipmentType'] },
        ],
      },
    ]);
  });

  it('lists the unlinked instances of a metric from the run that measured it last', async () => {
    const kept = await service().getConfiguredDrilldown({ ...KEY, property: 'assetClass' }, {});
    const remeasured = await service().getConfiguredDrilldown({ ...KEY, property: 'type' }, {});

    expect(kept.rows.map((row) => row.entityId)).toEqual(['asset-old']);
    expect(remeasured.rows.map((row) => row.entityId)).toEqual(['type-new']);
  });

  it('ignores the configured rows of a run that never completed', async () => {
    const rows = [
      ...ROWS.filter((row) => row.runId !== ONE),
      runRow(ONE, ONE_AT, 'running', { scope: 'configured' }),
      inRun(ONE, ONE_AT, extraLinkRow({ view: 'CogniteAsset', property: 'type', eligible: 110, passing: 105 })),
    ];

    const { models } = await service(rows).getConfiguredMetrics({});

    const type = metric(
      models.flatMap((model) => model.groups.flatMap((group) => group.metrics)),
      'type'
    );
    expect(type?.totalCount).toBe(100);
  });

  it('is read in parts when the rows of the runs fill a page', async () => {
    const reader = new QualityStreamReader(new FakeRecordsClient(ROWS, 4), () => NOW);

    const { models } = await new CdfQualityDashboardService({ reader, now: () => NOW }).getConfiguredMetrics({});

    expect(models.flatMap((model) => model.groups.flatMap((group) => group.metrics))).toHaveLength(3);
  });
});

function service(rows: readonly Readonly<Record<string, unknown>>[] = ROWS): CdfQualityDashboardService {
  const reader = new QualityStreamReader(new FakeRecordsClient(rows), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW });
}

function inRun<Row extends Readonly<Record<string, unknown>>>(runId: string, runTimestamp: string, row: Row) {
  return { ...row, runId, runTimestamp };
}

function metric<Metric extends { label: string }>(metrics: readonly Metric[], label: string): Metric | undefined {
  return metrics.find((entry) => entry.label === label);
}
