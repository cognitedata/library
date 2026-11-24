import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, RUN_NEW, extraLinkGapRow, extraLinkRow, runRow } from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');
const CORE = 'cdf_cdm:CogniteCore:v1';
const KEY = { modelId: CORE, view: 'CogniteAsset', property: 'type' };

function service(
  rows: readonly Readonly<Record<string, unknown>>[],
  warnings?: string,
  pageLimit?: number
): CdfQualityDashboardService {
  const all = [runRow(RUN_NEW, '2026-10-12T06:00:00.000Z', 'complete', warnings === undefined ? {} : { warnings }), ...rows];
  const reader = new QualityStreamReader(new FakeRecordsClient(all, pageLimit), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW });
}

const counted = (facilityId: string) =>
  extraLinkRow({ view: 'CogniteAsset', property: 'type', facilityId, eligible: 10, passing: 5 });
const gap = (entityId: string, facilityId: string, entityLabel?: string) =>
  extraLinkGapRow({ view: 'CogniteAsset', property: 'type', facilityId, entityId, entityLabel });

describe('the drill-down of a configured metric', () => {
  it('lists the instances without the link, named by their label, sorted by name', async () => {
    const drilldown = await service([
      counted('north'),
      gap('a2', 'north', 'Pump P-102'),
      gap('a1', 'north', 'Pump P-101'),
      extraLinkGapRow({ view: 'CogniteEquipment', property: 'equipmentType', entityId: 'other' }),
    ]).getConfiguredDrilldown(KEY, {});

    expect(drilldown.rows.map((row) => [row.entity, row.entityId, row.space, row.detail])).toEqual([
      ['Pump P-101', 'a1', 'north', 'link not set'],
      ['Pump P-102', 'a2', 'north', 'link not set'],
    ]);
    expect(drilldown.cutAt).toBeNull();
  });

  it('follows the facility filter', async () => {
    const drilldown = await service([counted('north'), counted('south'), gap('a1', 'north'), gap('a2', 'south')])
      .getConfiguredDrilldown(KEY, { facilityId: 'south' });

    expect(drilldown.rows.map((row) => row.entityId)).toEqual(['a2']);
  });

  it('reads a long list in parts, one facility at a time, when a page fills up', async () => {
    const rows = [
      counted('north'),
      counted('south'),
      ...Array.from({ length: 3 }, (_, i) => gap(`n${i}`, 'north')),
      ...Array.from({ length: 2 }, (_, i) => gap(`s${i}`, 'south')),
    ];

    const drilldown = await service(rows, undefined, 4).getConfiguredDrilldown(KEY, {});

    expect(drilldown.rows).toHaveLength(5);
  });

  it('says when the run cut the list, and how much was left out', async () => {
    const warnings = JSON.stringify([
      { code: 'drilldown-truncated', message: 'm', subject: 'CogniteAsset.type', limit: 500, dropped: 40, units: 2 },
      { code: 'drilldown-truncated', message: 'm', subject: 'CogniteEquipment.files', limit: 500, dropped: 9, units: 1 },
    ]);

    const drilldown = await service([counted('north'), gap('a1', 'north')], warnings).getConfiguredDrilldown(KEY, {});

    expect(drilldown.cutAt).toBe(500);
    expect(drilldown.dropped).toBe(40);
  });

  it('is empty when the run listed no instances', async () => {
    const drilldown = await service([counted('north')]).getConfiguredDrilldown(KEY, {});

    expect(drilldown.rows).toEqual([]);
  });
});
