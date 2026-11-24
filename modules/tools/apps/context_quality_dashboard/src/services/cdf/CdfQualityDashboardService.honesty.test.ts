import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, RUN_NEW, runRow } from './testSupport';

const NOW = Date.parse('2026-10-12T07:00:00.000Z');

function setup() {
  const client = new FakeRecordsClient([runRow(RUN_NEW, '2026-10-12T06:00:00.000Z')]);
  const service = new CdfQualityDashboardService({
    reader: new QualityStreamReader(client, () => NOW, 0),
    now: () => NOW,
  });
  return { client, service };
}

describe('actions the metrics backend does not implement', () => {
  it('does not claim to have notified an owner', async () => {
    await expect(
      setup().service.notifyOwner({ metricId: 'orphanRate', ownerId: 'owner-a', entityIds: ['a-1'] })
    ).rejects.toMatchObject({ kind: 'unsupported-operation', subject: 'notifyOwner' });
  });

  it('does not claim to have started a run', async () => {
    const { service } = setup();

    await expect(service.rerun({ dimensionId: 'time-series' })).rejects.toMatchObject({ kind: 'unsupported-operation' });
    await expect(service.rerun({ metricId: 'orphanRate' })).rejects.toMatchObject({ kind: 'unsupported-operation' });
  });

  it('refuses to evaluate a custom rule', async () => {
    await expect(setup().service.getCustomRuleDrilldown('rule-1', {})).rejects.toMatchObject({
      kind: 'unsupported-operation',
    });
  });

  it('still refreshes, which only re-reads the latest completed run', async () => {
    const { client, service } = setup();

    await expect(service.refresh()).resolves.toMatchObject({ lastSuccessAt: '2026-10-12T06:00:00.000Z', inProgress: false });
    expect(client.queries.every((query) => query.equals.rowKind === 'run')).toBe(true);
  });
});
