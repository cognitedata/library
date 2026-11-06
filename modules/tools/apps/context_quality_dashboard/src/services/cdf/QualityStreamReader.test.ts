import { describe, expect, it } from 'vitest';

import { QualityStreamReader } from './QualityStreamReader';
import {
  FakeRecordsClient,
  RUN_INCOMPLETE,
  RUN_NEW,
  RUN_OLD,
  aggregateRow,
  classificationRow,
  runRow,
} from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');

describe(QualityStreamReader.name, () => {
  it('returns the newest run that has a complete marker', async () => {
    const reader = readerFor([
      runRow(RUN_OLD, '2026-10-05T06:00:00.000Z'),
      runRow(RUN_NEW, '2026-10-12T06:00:00.000Z'),
      runRow(RUN_INCOMPLETE, '2026-10-13T06:00:00.000Z', 'started'),
    ]);

    await expect(reader.latestRun()).resolves.toMatchObject({ runId: RUN_NEW });
  });

  it('fails clearly when no run has completed', async () => {
    const reader = readerFor([runRow(RUN_INCOMPLETE, '2026-10-13T06:00:00.000Z', 'started')]);

    await expect(reader.latestRun()).rejects.toThrow('No completed quality run');
  });

  it('splits a truncated aggregate query by dimension and tier', async () => {
    const rows = [
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: 1, eligible: 1, passing: 1 }),
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: 2, eligible: 1, passing: 1 }),
      aggregateRow({ metricId: 'associationRate', dimensionId: 'equipment-asset', tier: 1, eligible: 1, passing: 1 }),
    ];
    const client = new FakeRecordsClient(rows, 2);
    const reader = new QualityStreamReader(client, () => NOW);

    const result = await reader.aggregates(RUN_NEW);

    expect(result).toHaveLength(3);
    expect(client.queries.some((query) => query.equals.dimensionId === 'asset-hierarchy')).toBe(true);
  });

  it('keeps untiered aggregate rows when a truncated query is split by tier', async () => {
    const rows = [
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: 1, eligible: 1, passing: 1 }),
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: 2, eligible: 1, passing: 1 }),
      aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: null, eligible: 7, passing: 6 }),
    ];
    const reader = new QualityStreamReader(new FakeRecordsClient(rows, 2), () => NOW);

    const result = await reader.aggregates(RUN_NEW);

    expect(result.map((row) => row.tier).sort()).toEqual([1, 2, null]);
    expect(result.find((row) => row.tier === null)?.eligibleCount).toBe(7);
  });

  it('keeps untiered classification rows when a truncated query is split by tier', async () => {
    const cls = [
      classificationRow({ kind: 'assetType', id: 'p', tier: 1, eligible: 1, passing: 1 }),
      classificationRow({ kind: 'assetType', id: 'p', tier: 2, eligible: 1, passing: 1 }),
      { ...classificationRow({ kind: 'assetType', id: 'p', eligible: 1, passing: 1 }), tier: undefined },
    ];

    const classified = await new QualityStreamReader(new FakeRecordsClient(cls, 2), () => NOW).classifications(RUN_NEW);

    expect(classified).toHaveLength(3);
  });

  it('carries the warnings of the latest completed run', async () => {
    const warnings = '[{"code":"mapping-empty","message":"m","subject":"tierByTypeCode"}]';
    const reader = readerFor([runRow(RUN_OLD, '2026-10-05T06:00:00.000Z'), runRow(RUN_NEW, '2026-10-12T06:00:00.000Z', 'complete', { warnings })]);

    const run = await reader.latestRun();

    expect(run.warnings).toEqual([expect.objectContaining({ code: 'mapping-empty' })]);
  });

  it('has no warnings for a run whose marker carries none', async () => {
    const reader = readerFor([runRow(RUN_NEW, '2026-10-12T06:00:00.000Z')]);

    await expect(reader.latestRun()).resolves.toMatchObject({ warnings: [] });
  });

  it('gives up when a query stays truncated after every split', async () => {
    const same = aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', tier: 1, eligible: 1, passing: 1 });
    const reader = new QualityStreamReader(new FakeRecordsClient([same], 1), () => NOW);

    await expect(reader.aggregates(RUN_NEW)).rejects.toThrow('cannot be split further');
  });

  it('keeps history only from completed runs', async () => {
    const reader = readerFor([
      runRow(RUN_OLD, '2026-10-05T06:00:00.000Z'),
      aggregateRow({ runId: RUN_OLD, metricId: 'orphanRate', dimensionId: 'asset-hierarchy', eligible: 10, passing: 5 }),
      aggregateRow({ runId: RUN_INCOMPLETE, metricId: 'orphanRate', dimensionId: 'asset-hierarchy', eligible: 10, passing: 1 }),
    ]);

    const history = await reader.history('orphanRate');

    expect(history.map((row) => row.runId)).toEqual([RUN_OLD]);
  });

  it('serves repeated run lookups from cache until invalidated', async () => {
    const client = new FakeRecordsClient([runRow(RUN_NEW, '2026-10-12T06:00:00.000Z')]);
    const reader = new QualityStreamReader(client, () => NOW);

    await reader.latestRun();
    await reader.latestRun();
    reader.invalidate();
    await reader.latestRun();

    expect(client.queries).toHaveLength(2);
  });
});

describe('when no run has completed', () => {
  it('says the stream is empty when it holds no runs at all', async () => {
    await expect(readerFor([]).latestRun()).rejects.toMatchObject({ kind: 'no-completed-run', detail: 'empty' });
  });

  it('says runs did not finish when the stream only holds unfinished runs', async () => {
    const reader = readerFor([runRow(RUN_INCOMPLETE, '2026-10-13T06:00:00.000Z', 'started')]);

    await expect(reader.latestRun()).rejects.toMatchObject({ kind: 'no-completed-run', detail: 'incomplete' });
  });

  it('keeps the diagnosis even if the follow-up look at the stream fails', async () => {
    let calls = 0;
    const reader = new QualityStreamReader(
      {
        async query() {
          calls += 1;
          if (calls > 1) {
            throw new Error('boom');
          }
          return { rows: [], truncated: false };
        },
      },
      () => NOW
    );

    await expect(reader.latestRun()).rejects.toMatchObject({ kind: 'no-completed-run', detail: undefined });
  });
});

function readerFor(rows: ConstructorParameters<typeof FakeRecordsClient>[0]) {
  return new QualityStreamReader(new FakeRecordsClient(rows), () => NOW);
}
