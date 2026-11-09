import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, RUN_NEW, RUN_OLD, aggregateRow, extraLinkRow, runRow } from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');
const RUN_TS = '2026-10-12T06:00:00.000Z';
const OTHER = 'my_space:MyModel:v2';

function service(rows: Parameters<typeof rowsOf>[0]): CdfQualityDashboardService {
  const reader = new QualityStreamReader(new FakeRecordsClient(rowsOf(rows)), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW });
}

function rowsOf(rows: readonly Readonly<Record<string, unknown>>[]) {
  return rows;
}

describe('the configuration the latest run was started with', () => {
  it('lists the models, views and properties the run measured, in alphabetical order and once each', async () => {
    const config = await service([
      runRow(RUN_NEW, RUN_TS),
      extraLinkRow({ view: 'CogniteEquipment', property: 'equipmentType', facilityId: 'north', eligible: 1, passing: 1 }),
      extraLinkRow({ view: 'CogniteAsset', property: 'type', facilityId: 'north', eligible: 1, passing: 1 }),
      extraLinkRow({ view: 'CogniteAsset', property: 'type', facilityId: 'south', eligible: 1, passing: 1 }),
      extraLinkRow({ view: 'CogniteAsset', property: 'assetClass', facilityId: 'north', eligible: 1, passing: 1 }),
      extraLinkRow({ model: OTHER, view: 'Widget', property: 'owner', facilityId: 'north', eligible: 0, passing: 0, notApplicable: true }),
    ]).getLatestRunConfiguration();

    expect(config).toEqual({
      runTimestamp: RUN_TS,
      models: [
        {
          model: 'cdf_cdm:CogniteCore:v1',
          views: [
            { view: 'CogniteAsset', properties: ['assetClass', 'type'] },
            { view: 'CogniteEquipment', properties: ['equipmentType'] },
          ],
        },
        { model: OTHER, views: [{ view: 'Widget', properties: ['owner'] }] },
      ],
    });
  });

  it('says the run used no configuration when it measured nothing extra', async () => {
    const config = await service([
      runRow(RUN_NEW, RUN_TS),
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', eligible: 1, passing: 1 }),
    ]).getLatestRunConfiguration();

    expect(config).toEqual({ runTimestamp: RUN_TS, models: [] });
  });

  it('reads only the latest run, not an earlier one that had a configuration', async () => {
    const config = await service([
      runRow(RUN_OLD, '2026-10-05T06:00:00.000Z'),
      { ...extraLinkRow({ view: 'Old', property: 'gone', eligible: 1, passing: 1 }), runId: RUN_OLD },
      runRow(RUN_NEW, RUN_TS),
    ]).getLatestRunConfiguration();

    expect(config).toEqual({ runTimestamp: RUN_TS, models: [] });
  });

  it('is nothing at all, not an error, when no run has completed yet', async () => {
    await expect(service([]).getLatestRunConfiguration()).resolves.toBeNull();
    await expect(
      service([runRow('run-x', RUN_TS, 'started')]).getLatestRunConfiguration()
    ).resolves.toBeNull();
  });
});
