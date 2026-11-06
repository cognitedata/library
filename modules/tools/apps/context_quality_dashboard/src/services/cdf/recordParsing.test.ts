import { describe, expect, it } from 'vitest';

import { isMetricId } from '../../features/dimension/metricIds';

import { buildExclusionBuckets } from './exclusionBuckets';
import { parseAggregateRow, parseClassificationRow, parseDrilldownRow, parseRunRow } from './metricRows';
import { QualityStreamReader } from './QualityStreamReader';
import type { PropertyBag } from './recordsClient';
import { FakeRecordsClient } from './testSupport';

/** How the dashboard reads the records the backend writes: older runs, unknown values and the run-complete marker. */

// Records as the backend writes them: a full one and, where optional properties exist, a minimal one.
const RUN_COMPLETE: PropertyBag = {
  'rowKind': 'run',
  'runId': 'run-20261013T053425-c2326a',
  'runTimestamp': '2026-10-13T05:34:25.000Z',
  'status': 'complete',
  'scope': 'all',
  'warnings': 'file annotation not run',
  'rowsWritten': 4079
};

const AGGREGATE_FULL: PropertyBag = {
  'rowKind': 'aggregate',
  'runId': 'run-20261013T053425-c2326a',
  'runTimestamp': '2026-10-13T05:34:25.000Z',
  'metricId': 'approvalMix',
  'dimensionId': 'file-annotation',
  'reportingUnitId': 'unit-north',
  'facilityId': 'north',
  'divisionId': 'prod-a',
  'disciplineId': 'instrumentation',
  'ownerId': 'pid-review-lead',
  'tier': 1,
  'ruleSetVersion': 'v1',
  'eligibleCount': 120,
  'passingCount': 96,
  'issueCount': 24,
  'valueSum': 0,
  'notApplicable': false,
  'matchType': 'rule-based',
  'subKey': 'Approved'
};

const AGGREGATE_MINIMAL: PropertyBag = {
  'rowKind': 'aggregate',
  'runId': 'run-20261013T053425-c2326a',
  'runTimestamp': '2026-10-13T05:34:25.000Z',
  'metricId': 'orphanRate',
  'dimensionId': 'asset-hierarchy',
  'reportingUnitId': 'unit-north'
};

const DRILLDOWN_FULL: PropertyBag = {
  'rowKind': 'drilldown',
  'runId': 'run-20261013T053425-c2326a',
  'runTimestamp': '2026-10-13T05:34:25.000Z',
  'metricId': 'unmatchedWithCandidates',
  'dimensionId': 'equipment-asset',
  'reportingUnitId': 'unit-north',
  'facilityId': 'north',
  'divisionId': 'prod-a',
  'disciplineId': 'mechanical',
  'ownerId': 'sap-steward',
  'tier': 1,
  'entityId': 'eq-north-0012',
  'entityLabel': 'P-1012 Feed pump',
  'entityRef': 'sp_site_north:eq-north-0012',
  'detail': 'no asset link; 2 candidate assets',
  'cellsJson': '{"equipmentType":"Centrifugal Pump","assetClass":"Rotating equipment","ignored":7}',
  'matched': false
};

const DRILLDOWN_MINIMAL: PropertyBag = {
  'rowKind': 'drilldown',
  'runId': 'run-20261013T053425-c2326a',
  'runTimestamp': '2026-10-13T05:34:25.000Z',
  'metricId': 'orphansByTier',
  'dimensionId': 'asset-hierarchy',
  'entityId': 'a-1'
};

describe('unknown values', () => {
  it('drops an exclusion category the dashboard does not know', () => {
    expect(bucketCount('invented-category')).toBe(0);
  });

  it('does not recognise an unknown metric id', () => {
    expect(isMetricId('someNewMetric')).toBe(false);
  });

  it('keeps an unknown classification kind', () => {
    const row = parseClassificationRow({ classificationKind: 'regionCode', classificationId: 'eu' });

    expect(row).toMatchObject({ kind: 'regionCode', classificationId: 'eu' });
  });
});

describe('run-complete marker', () => {
  const golden = RUN_COMPLETE;
  const read = (rows: PropertyBag[]) => new QualityStreamReader(new FakeRecordsClient(rows), () => 0, 0);

  it('makes the latest complete run current', async () => {
    const reader = read([
      { ...golden, runId: 'run-a', runTimestamp: '2026-10-12T05:00:00.000Z' },
      { ...golden, runId: 'run-b', runTimestamp: '2026-10-13T05:00:00.000Z' },
    ]);

    expect((await reader.latestRun()).runId).toBe('run-b');
  });

  it('never treats a run that only has a started marker as current', async () => {
    const reader = read([
      { ...golden, runId: 'run-a', runTimestamp: '2026-10-12T05:00:00.000Z' },
      { ...golden, runId: 'run-b', runTimestamp: '2026-10-13T05:00:00.000Z', status: 'started' },
    ]);

    expect((await reader.latestRun()).runId).toBe('run-a');
  });

  it('has no current run when nothing is complete', async () => {
    const reader = read([{ ...golden, status: 'started' }]);

    await expect(reader.latestRun()).rejects.toThrow('No completed quality run');
  });
});

const WARNING_CODES = [
  'mapping-empty',
  'unmapped-asset-types',
  'parents-not-loaded',
  'optional-source-unavailable',
  'no-source-entities',
  'metric-not-applicable',
  'drilldown-truncated',
  'warnings-truncated',
  'tiering-off',
  'export-failed',
  'location-unsupported',
  'locations-unavailable',
];

describe('notApplicable rows (needs notApplicable on AggregateRow)', () => {
  const golden = AGGREGATE_FULL;
  const notApplicable = Object.fromEntries(
    Object.entries({ ...golden, eligibleCount: 0, passingCount: 0, issueCount: 0, notApplicable: true }).filter(([key]) => key !== 'tier')
  );

  it('reads a row with eligibleCount 0 and notApplicable true as not applicable', () => {
    const row = parseAggregateRow(notApplicable);

    expect(row).toMatchObject({ eligibleCount: 0, passingCount: 0, notApplicable: true, tier: null });
  });

  it('reads a measured zero (eligible entities, none passing) as measured, not not-applicable', () => {
    const row = parseAggregateRow({ ...golden, eligibleCount: 8, passingCount: 0, issueCount: 8 });

    expect(row).toMatchObject({ eligibleCount: 8, passingCount: 0, notApplicable: false });
  });

  it('reads a version-1 row that has no notApplicable property as measured', () => {
    const row = parseAggregateRow(golden);

    expect(row).toMatchObject({ notApplicable: false });
  });

  it('does not treat a non-boolean notApplicable as true', () => {
    expect(parseAggregateRow({ ...golden, notApplicable: 'true' })).toMatchObject({ notApplicable: false });
    expect(parseAggregateRow({ ...golden, notApplicable: 1 })).toMatchObject({ notApplicable: false });
  });

  it('writes no value for a not-applicable row (valueSum stays 0)', () => {
    expect(parseAggregateRow(notApplicable)).toMatchObject({ valueSum: 0 });
  });
});

describe('untiered rows (needs DrilldownRow.tier to be Tier | null)', () => {
  it('reads an aggregate row without a tier as untiered', () => {
    const minimal = AGGREGATE_MINIMAL;

    expect(parseAggregateRow(minimal)).toMatchObject({ tier: null });
  });

  it('reads a tier outside 1 to 3, a string tier and a null tier as untiered, never as tier 3', () => {
    const golden = AGGREGATE_FULL;

    for (const tier of [0, 4, 9, '1', null]) {
      expect(parseAggregateRow({ ...golden, tier }), `tier ${String(tier)}`).toMatchObject({ tier: null });
    }
  });

  it('reads a drill-down record without a tier as untiered instead of defaulting it to tier 3', () => {
    const minimal = DRILLDOWN_MINIMAL;

    expect(parseDrilldownRow(minimal)).toMatchObject({ tier: null });
  });

  it('keeps a valid drill-down tier, including a tier 3 the backend wrote on purpose', () => {
    const golden = DRILLDOWN_FULL;

    expect(parseDrilldownRow({ ...golden, tier: 3 })).toMatchObject({ tier: 3 });
    expect(parseDrilldownRow({ ...golden, tier: 1 })).toMatchObject({ tier: 1 });
  });
});

describe('run warnings (needs src/services/cdf/runWarnings.ts: parseRunWarnings)', () => {
  const warningsText = JSON.stringify(
    WARNING_CODES.map((code) => ({ code, message: `message for ${code}`, subject: 'x', unmapped: 2, codes: ['widget'] }))
  );

  async function parseWarnings(text: unknown): Promise<unknown[]> {
    const { parseRunWarnings } = await import('./runWarnings');
    return parseRunWarnings(text);
  }

  it('reads an absent, null or empty warnings value as no warnings', async () => {
    for (const value of [undefined, null, '', '   ']) {
      expect(await parseWarnings(value), `value ${String(value)}`).toEqual([]);
    }
  });

  it('reads the JSON text the backend writes, one entry per warning, in order', async () => {
    const parsed = await parseWarnings(warningsText);

    expect(parsed).toHaveLength(WARNING_CODES.length);
    expect(parsed.map((warning) => (isRecord(warning) ? warning.code : undefined))).toEqual(WARNING_CODES);
  });

  it('keeps a warning whose code or fields it does not know, without failing', async () => {
    const parsed = await parseWarnings(JSON.stringify([{ code: 'a-future-code', message: 'm', brandNew: { nested: true } }]));

    expect(parsed).toHaveLength(1);
  });

  it('survives legacy free text, malformed JSON and JSON that is not a list', async () => {
    for (const value of ['file annotation not run', '[{"code":', '{"code":"mapping-empty"}', '42', 'null']) {
      await expect(parseWarnings(value), `value ${value}`).resolves.toBeInstanceOf(Array);
    }
  });

  it('survives a value that is not text', async () => {
    for (const value of [42, true, {}, ['mapping-empty']]) {
      await expect(parseWarnings(value)).resolves.toBeInstanceOf(Array);
    }
  });

  it('skips list entries that are not warning objects instead of failing', async () => {
    const parsed = await parseWarnings(JSON.stringify([null, 3, 'text', { code: 'mapping-empty', message: 'm' }]));

    expect(parsed.length).toBeLessThanOrEqual(4);
    expect(parsed.some((warning) => isRecord(warning) && warning.code === 'mapping-empty')).toBe(true);
  });

  it('puts the parsed warnings on the run marker', () => {
    const marker = RUN_COMPLETE;

    const row = parseRunRow({ ...marker, warnings: warningsText });

    expect(row).toMatchObject({ runId: marker.runId });
    expect(isRecord(row) && Array.isArray(row.warnings) ? row.warnings : []).toHaveLength(WARNING_CODES.length);
  });

  it('reads the golden run sample, whatever its warnings value is, as a usable run', () => {
    const marker = RUN_COMPLETE;

    expect(parseRunRow(marker)).toMatchObject({ runId: marker.runId, runTimestamp: marker.runTimestamp });
  });
});

describe('historical compatibility (version 1)', () => {
  it('reads a run marker with no scope and no warnings as a run of every dimension', () => {
    const row = parseRunRow({ rowKind: 'run', status: 'complete', runId: 'run-old', runTimestamp: '2026-10-01T00:00:00.000Z' });

    expect(row).toMatchObject({ runId: 'run-old' });
  });

  it('still reads rows written with the readable record keys, because the dashboard never reads keys', () => {
    const golden = DRILLDOWN_FULL;

    // the key is not a property of the record: the same properties parse the same whatever the key scheme was
    expect(parseDrilldownRow({ ...golden, externalId: 'run-x:drill:metric|entity' })).toEqual(parseDrilldownRow(golden));
    expect(parseDrilldownRow({ ...golden, externalId: 'run-x:drill:metric:0123456789abcdef0123456789abcdef' })).toEqual(
      parseDrilldownRow(golden)
    );
  });

  it('reads a synthetic demo run as an ordinary run', () => {
    const row = parseRunRow({
      rowKind: 'run',
      status: 'complete',
      scope: 'synthetic-demo',
      runId: 'synthetic-run-20261001T000000-w01',
      runTimestamp: '2026-10-01T00:00:00.000Z',
    });

    expect(row).toMatchObject({ runId: 'synthetic-run-20261001T000000-w01' });
  });
});

function bucketCount(category: string): number {
  return buildExclusionBuckets([
    {
      dimensionId: 'files',
      exclusionCategory: category,
      facilityId: null,
      divisionId: null,
      disciplineId: null,
      excludedCount: 1,
    },
  ]).length;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

