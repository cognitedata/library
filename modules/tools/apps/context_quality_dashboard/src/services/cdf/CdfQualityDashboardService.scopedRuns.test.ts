import { describe, expect, it } from 'vitest';

import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { FakeRecordsClient, aggregateRow, classificationRow, drilldownRow, extraLinkRow, runRow } from './testSupport';

const NOW = Date.parse('2026-11-02T10:00:00.000Z');
const FULL = 'run-full';
const FULL_AT = '2026-10-30T06:00:00.000Z';
const FILES_ONLY = 'run-files';
const FILES_AT = '2026-11-02T06:00:00.000Z';

/** A full run, then a run that measured only the files dimension. */
const ROWS = [
  runRow(FULL, FULL_AT, 'complete', { scope: 'all' }),
  runRow(FILES_ONLY, FILES_AT, 'complete', { scope: 'files' }),
  aggregateRow({ runId: FULL, runTimestamp: FULL_AT, metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', eligible: 100, passing: 90 }),
  aggregateRow({ runId: FULL, runTimestamp: FULL_AT, metricId: 'fileToAssetRate', dimensionId: 'files', eligible: 40, passing: 20 }),
  aggregateRow({ runId: FILES_ONLY, runTimestamp: FILES_AT, metricId: 'fileToAssetRate', dimensionId: 'files', eligible: 50, passing: 45 }),
  drilldownRow('orphansByTier', 'asset-hierarchy', 'orph-1', { runId: FULL }),
  drilldownRow('filesMissingCategory', 'files', 'file-old', { runId: FULL }),
  drilldownRow('filesMissingCategory', 'files', 'file-new', { runId: FILES_ONLY }),
  { ...classificationRow({ kind: 'assetType', id: 'pump', name: 'Pump', eligible: 80, passing: 60 }), runId: FULL },
];

function service(rows = ROWS): CdfQualityDashboardService {
  const reader = new QualityStreamReader(new FakeRecordsClient(rows), () => NOW);
  return new CdfQualityDashboardService({ reader, now: () => NOW });
}

describe('a run for some dimensions only', () => {
  it('leaves the other dimensions as the run that measured them last', async () => {
    // Act
    const overview = await service().getOverview({});

    // Assert
    const card = (id: string) => overview.dimensionCards.find((entry) => entry.dimensionId === id);
    expect(card('asset-hierarchy')?.enabled).toBe(true);
    expect(card('asset-hierarchy')?.eligibleCount).toBe(100);
    expect(card('asset-hierarchy')?.measuredAt).toBe(FULL_AT);
  });

  it('shows a dimension the later run measured as that run measured it, not as the earlier one did', async () => {
    const overview = await service().getOverview({});

    const files = overview.dimensionCards.find((entry) => entry.dimensionId === 'files');
    expect(files?.eligibleCount).toBe(50); // the run of files, not the 40 of the full run
    expect(files?.passingCount).toBe(45);
    expect(files?.measuredAt).toBe(FILES_AT);
  });

  it('is disabled for a dimension no completed run measured', async () => {
    const onlyFiles = service([ROWS[1] ?? {}, ROWS[4] ?? {}]);

    const overview = await onlyFiles.getOverview({});

    expect(overview.dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy')?.enabled).toBe(false);
    expect(overview.dimensionCards.find((entry) => entry.dimensionId === 'files')?.enabled).toBe(true);
  });

  it('opens a dimension from the run that measured it', async () => {
    const dimension = await service().getDimension('asset-hierarchy', {});

    expect(dimension.enabled).toBe(true);
    expect(dimension.entityCount).toBe(100);
  });

  it('lists the unlinked items of a dimension from the run that measured it', async () => {
    const hierarchy = await service().getDrilldown('orphansByTier', {});
    const files = await service().getDrilldown('filesMissingCategory', {});

    expect(hierarchy.rows.map((row) => row.cells.entity)).toEqual(['Label orph-1']);
    expect(files.rows.map((row) => row.cells.entity)).toEqual(['Label file-new']);
  });

  it('keeps the classifications of a dimension the later run did not measure', async () => {
    const { groups } = await service().getClassifications({});

    expect(groups.map((group) => group.kind)).toEqual(['assetType']);
  });

  it('reports when the latest run finished and when each dimension was last measured', async () => {
    const status = await service().getRunStatus();

    expect(status.lastSuccessAt).toBe(FILES_AT);
    expect(status.dimensionRuns?.files).toBe(FILES_AT);
    expect(status.dimensionRuns?.['asset-hierarchy']).toBe(FULL_AT);
    expect(status.configuredRunAt).toBeUndefined();
  });
});

describe('the metrics of a config file', () => {
  const CONFIG_RUN = 'run-config';
  const LATER_FULL = 'run-later';
  const rows = [
    runRow(CONFIG_RUN, '2026-10-30T06:00:00.000Z', 'complete', { scope: 'all+configured' }),
    runRow(LATER_FULL, '2026-11-02T06:00:00.000Z', 'complete', { scope: 'all' }),
    { ...extraLinkRow({ view: 'CogniteAsset', property: 'type', eligible: 100, passing: 90 }), runId: CONFIG_RUN },
  ];

  it('stay as the latest run that measured them when a later run did not', async () => {
    const { models } = await service(rows).getConfiguredMetrics({});

    expect(models.flatMap((model) => model.groups.flatMap((group) => group.metrics.map((metric) => metric.label)))).toEqual([
      'type',
    ]);
    expect((await service(rows).getRunStatus()).configuredRunAt).toBe('2026-10-30T06:00:00.000Z');
  });

  it('are read from the latest run when no run says it measured them, as before scopes existed', async () => {
    const legacy = [
      runRow('run-a', '2026-10-30T06:00:00.000Z'),
      { ...extraLinkRow({ view: 'CogniteAsset', property: 'type', eligible: 10, passing: 5 }), runId: 'run-a' },
    ];

    const { models } = await service(legacy).getConfiguredMetrics({});

    expect(models).toHaveLength(1);
  });
});
