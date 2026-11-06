import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, RUN_NEW, RUN_OLD, aggregateRow, runRow } from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');
const OLD_TS = '2026-10-05T06:00:00.000Z';
const NEW_TS = '2026-10-12T06:00:00.000Z';

function service(rows: readonly Readonly<Record<string, unknown>>[]): CdfQualityDashboardService {
  const reader = new QualityStreamReader(new FakeRecordsClient(rows), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW });
}

const completeness = (subKey: string, eligible: number, passing: number, extra: object = {}) =>
  aggregateRow({ metricId: 'serialMfrCompleteness', dimensionId: 'equipment-asset', facilityId: 'north', subKey, eligible, passing, ...extra });

describe('a metric measured once per field', () => {
  it('is reported as one card per field, each over the same entities, never added together', async () => {
    const dimension = await service([
      runRow(RUN_NEW, NEW_TS),
      completeness('serialNumber', 801, 700),
      completeness('manufacturer', 801, 723),
    ]).getDimension('equipment-asset', {});

    const cards = dimension.aggregateRows.filter((row) => row.metricId === 'serialMfrCompleteness');
    expect(cards.map((card) => [card.subKey, card.eligibleCount, card.passingCount, card.labelKey])).toEqual([
      ['serialNumber', 801, 700, 'metric.serialMfrCompleteness.serialNumber'],
      ['manufacturer', 801, 723, 'metric.serialMfrCompleteness.manufacturer'],
    ]);
  });

  it('gives each field its own trend', async () => {
    const svc = service([
      runRow(RUN_OLD, OLD_TS),
      runRow(RUN_NEW, NEW_TS),
      completeness('serialNumber', 100, 50),
      completeness('manufacturer', 100, 90),
      { ...completeness('serialNumber', 100, 10), runId: RUN_OLD, runTimestamp: OLD_TS },
      { ...completeness('manufacturer', 100, 20), runId: RUN_OLD, runTimestamp: OLD_TS },
    ]);

    const serial = await svc.getSnapshots('serialMfrCompleteness', '30d', {}, 'serialNumber');
    const manufacturer = await svc.getSnapshots('serialMfrCompleteness', '30d', {}, 'manufacturer');

    expect(serial.map((point) => point.valuePercent)).toEqual([10, 50]);
    expect(manufacturer.map((point) => point.valuePercent)).toEqual([20, 90]);
  });

  it('shows no card for a field name it does not know', async () => {
    const dimension = await service([
      runRow(RUN_NEW, NEW_TS),
      completeness('serialNumber', 801, 700),
      completeness('manufacturer', 801, 723),
      completeness('serial', 801, 700),
    ]).getDimension('equipment-asset', {});

    expect(
      dimension.aggregateRows.filter((row) => row.metricId === 'serialMfrCompleteness').map((row) => row.subKey)
    ).toEqual(['serialNumber', 'manufacturer']);
  });

  it('opens the list of entities missing that very field', async () => {
    const dimension = await service([
      runRow(RUN_NEW, NEW_TS),
      completeness('serialNumber', 801, 700),
      completeness('manufacturer', 801, 723),
    ]).getDimension('equipment-asset', {});

    const cards = dimension.aggregateRows.filter((row) => row.metricId === 'serialMfrCompleteness');
    expect(cards.map((card) => [card.subKey, card.hasDrilldown, card.drilldownMetricId])).toEqual([
      ['serialNumber', true, 'equipmentMissingSerial'],
      ['manufacturer', true, 'equipmentMissingManufacturer'],
    ]);
  });

  it('lets a work order completion card list the work orders that are not completed', async () => {
    const dimension = await service([
      runRow(RUN_NEW, NEW_TS),
      aggregateRow({ metricId: 'woCompletionRate', dimensionId: 'maintenance', facilityId: 'north', eligible: 600, passing: 421 }),
    ]).getDimension('maintenance', {});

    expect(dimension.aggregateRows.find((row) => row.metricId === 'woCompletionRate')?.hasDrilldown).toBe(true);
  });
});
