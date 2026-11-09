import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, RUN_NEW, aggregateRow, drilldownRow, runRow } from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');

const ROWS = [
  runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
  aggregateRow({ metricId: 'tsToAssetRate', dimensionId: 'time-series', facilityId: 'north', eligible: 10, passing: 8 }),
  aggregateRow({ metricId: 'freshness', dimensionId: 'time-series', facilityId: 'north', eligible: 10, passing: 9 }),
  aggregateRow({ metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', facilityId: 'north', eligible: 10, passing: 10 }),
  drilldownRow('orphansByPrefix', 'time-series', 'ts-1'),
  drilldownRow('orphansByPrefix', 'time-series', 'ts-2', { facilityId: 'south' }),
  drilldownRow('staleTimeSeries', 'time-series', 'ts-3'),
];

function service(): CdfQualityDashboardService {
  const reader = new QualityStreamReader(new FakeRecordsClient(ROWS), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW });
}

describe('listing the unlinked items of a link metric', () => {
  it('marks a link metric as having a drill-down, and one that counts no links as having none', async () => {
    const dimension = await service().getDimension('time-series', {});
    const hierarchy = await service().getDimension('asset-hierarchy', {});

    expect(dimension.aggregateRows.find((row) => row.metricId === 'tsToAssetRate')?.hasDrilldown).toBe(true);
    expect(dimension.aggregateRows.find((row) => row.metricId === 'freshness')?.hasDrilldown).toBe(true);
    expect(hierarchy.aggregateRows.find((row) => row.metricId === 'hierarchyCompletionRate')?.hasDrilldown).toBe(false);
  });

  it('lists the entities the link metric failed on, under the metric that was opened', async () => {
    const drilldown = await service().getDrilldown('tsToAssetRate', {});

    expect(drilldown.metricId).toBe('tsToAssetRate');
    expect(drilldown.dimensionId).toBe('time-series');
    expect(drilldown.rows.map((row) => row.id).sort()).toEqual(['tsToAssetRate:ts-1', 'tsToAssetRate:ts-2']);
  });

  it('applies the slice to the list', async () => {
    const drilldown = await service().getDrilldown('tsToAssetRate', { facilityId: 'south' });

    expect(drilldown.rows.map((row) => row.cells.entity)).toEqual(['Label ts-2']);
  });

  it('still opens a drill-down metric as itself', async () => {
    const drilldown = await service().getDrilldown('staleTimeSeries', {});

    expect(drilldown.rows.map((row) => row.id)).toEqual(['staleTimeSeries:ts-3']);
  });
});
