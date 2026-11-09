import { describe, expect, it } from 'vitest';

import { CdfCatalogService } from './CdfCatalogService';
import { QualityStreamReader } from './QualityStreamReader';
import type { PropertyBag } from './recordsClient';
import { FakeRecordsClient, RUN_NEW, aggregateRow, extraLinkRow, locationRow, runRow } from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');

function catalog(rows: readonly PropertyBag[]): CdfCatalogService {
  return new CdfCatalogService({ reader: new QualityStreamReader(new FakeRecordsClient(rows), () => NOW) });
}

describe(CdfCatalogService.name, () => {
  const rows = [
    runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
    aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', facilityId: 'north', eligible: 1, passing: 1 }),
    extraLinkRow({ view: 'Ucp', property: 'timeSeries', facilityId: 'north', eligible: 1, passing: 1 }),
    extraLinkRow({ view: 'Ucp', property: 'timeSeries', facilityId: 'sp_ucp_south', eligible: 1, passing: 0 }),
  ];

  it('lists the facilities of the built-in metrics', async () => {
    const facilities = await catalog(rows.slice(0, 2)).getFacilities();

    expect(facilities.map((facility) => facility.id)).toEqual(['north']);
  });

  it('also lists a facility that only the configured metrics were counted in, once', async () => {
    const facilities = await catalog(rows).getFacilities();

    expect(facilities.map((facility) => facility.id)).toEqual(['north', 'sp_ucp_south']);
  });

  it('lists the locations the latest run wrote, with their facilities and parent', async () => {
    const locations = await catalog([
      runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
      locationRow('region', 'Region', ['north', 'south']),
      locationRow('site', 'Site', ['north'], 'region'),
      { rowKind: 'location', runId: RUN_NEW, runTimestamp: '2026-10-12T06:00:00.000Z', entityId: 'broken', cellsJson: 'x' },
    ]).getLocations();

    expect(locations).toEqual([
      { id: 'region', name: 'Region', parentId: null, facilityIds: ['north', 'south'] },
      { id: 'site', name: 'Site', parentId: 'region', facilityIds: ['north'] },
    ]);
  });

  it('lists no locations when the run wrote none', async () => {
    expect(await catalog(rows).getLocations()).toEqual([]);
  });
});
