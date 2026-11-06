import { describe, expect, it } from 'vitest';

import { MOCK_DATASET, type MockDataset } from '../../mock/dataset';
import { KNOWN_WARNING_CODES } from '../cdf/runWarnings';

import { createMockServices } from './createMockServices';
import { MockQualityDashboardService } from './MockQualityDashboardService';

function serviceWith(dataset: MockDataset = MOCK_DATASET, tieringEnabled = true) {
  return new MockQualityDashboardService({
    dataset,
    getConfig: () => ({ ...dataset.defaultConfig, tieringEnabled }),
    getQualityRules: () => [],
    latency: 0,
  });
}

function withoutTieredEntities(): MockDataset {
  return {
    ...MOCK_DATASET,
    assets: MOCK_DATASET.assets.filter((asset) => asset.tier === 3),
    states: {
      ...MOCK_DATASET.states,
      untieredPools: [{ dimensionId: 'asset-hierarchy', facilityId: 'demo-north', eligibleCount: 40, passingCount: 20 }],
    },
  };
}

describe('mock demo: untiered entities', () => {
  it('shows the untiered pool beside the tiers and never counts it as tier 3', async () => {
    const overview = await createMockServices({ latency: 0 }).qualityDashboardService.getOverview({});

    const card = overview.dimensionCards.find((entry) => entry.dimensionId === 'asset-hierarchy');
    expect(card?.untiered).toMatchObject({ eligibleCount: 14, passingCount: 9, scorePercent: 64 });
    expect(card?.scoreBasis).toBe('tiered');
    expect(overview.untiered?.eligibleCount).toBeGreaterThan(0);
  });

  it('keeps the pool out of a facility that does not own it and out of an active division filter', async () => {
    const svc = serviceWith();

    const south = await svc.getDimension('asset-hierarchy', { facilityId: 'demo-south' });
    const division = await svc.getDimension('asset-hierarchy', { divisionId: 'prod-a' });

    expect(south.untiered).toBeNull();
    expect(division.untiered).toBeNull();
  });

  it('pools all entities and says so when a slice has no tier 1 or 2 entity', async () => {
    const dimension = await serviceWith(withoutTieredEntities()).getDimension('asset-hierarchy', { facilityId: 'demo-north' });

    expect(dimension.scoreBasis).toBe('pooled-fallback');
    expect(dimension.overallPercent).not.toBeNull();
    expect(dimension.untiered).toMatchObject({ eligibleCount: 40, passingCount: 20, scorePercent: 50 });
  });

  it('scores everything as one pool with no untiered figure when tiering is off', async () => {
    const dimension = await serviceWith(MOCK_DATASET, false).getDimension('asset-hierarchy', {});

    expect(dimension.scoreBasis).toBe('pooled');
    expect(dimension.untiered).toBeNull();
  });

  it('lists untiered issues with no tier and counts them as unassigned', async () => {
    const { issues, summary } = await serviceWith().getIssues({});

    const untiered = issues.filter((issue) => issue.tier === null);
    expect(untiered.length).toBeGreaterThan(0);
    expect(summary.unassigned).toBe(untiered.length);
    expect(summary.total).toBe(summary.critical + summary.important + summary.standard + summary.unassigned);
  });

  it('adds untiered rows to a drill-down with an empty tier cell, and filters them by facility', async () => {
    const svc = serviceWith();

    const all = await svc.getDrilldown('orphansByTier', {});
    const south = await svc.getDrilldown('orphansByTier', { facilityId: 'demo-south' });

    const untiered = all.rows.filter((row) => row.tier === null);
    expect(untiered.length).toBeGreaterThan(0);
    expect(untiered.every((row) => row.cells.tier === '')).toBe(true);
    expect(south.rows.some((row) => row.tier === null)).toBe(false);
  });
});

describe('mock demo: not applicable', () => {
  it('flags the fixture metrics with no value, a not-applicable status and the reason', async () => {
    const dimension = await serviceWith().getDimension('time-series', {});

    const coverage = dimension.aggregateRows.find((row) => row.metricId === 'monitoringCoverage');
    expect(coverage).toMatchObject({
      notApplicable: true,
      valuePercent: null,
      status: 'not-applicable',
      eligibleCount: 0,
      notApplicableReason: { kind: 'mapping-empty', mapping: 'tierByTypeCode' },
    });
    const measured = dimension.aggregateRows.find((row) => row.metricId === 'tsToAssetRate');
    expect(measured).toMatchObject({ notApplicable: false, notApplicableReason: null });
  });

  it('shows the all-excluded and no-eligible reasons on their metrics', async () => {
    const series = await serviceWith().getDimension('time-series', {});
    const model = await serviceWith().getDimension('model-3d', {});

    expect(series.aggregateRows.find((row) => row.metricId === 'freshness')?.notApplicableReason?.kind).toBe('all-excluded');
    expect(model.aggregateRows.find((row) => row.metricId === 'asset3dCoverage')?.notApplicableReason?.kind).toBe('no-eligible-entities');
  });

  it('calls a dimension with nothing to measure not applicable instead of good', async () => {
    const svc = serviceWith();

    const overview = await svc.getOverview({ facilityId: 'missing-facility' });
    const dimension = await svc.getDimension('asset-hierarchy', { facilityId: 'missing-facility' });

    expect(dimension).toMatchObject({ status: 'not-applicable', entityCount: 0, overallPercent: null });
    expect(overview.dimensionCards.filter((card) => card.enabled).every((card) => card.status === 'not-applicable')).toBe(true);
  });
});

describe('mock demo: run warnings', () => {
  it('returns one fixture per documented warning code plus a legacy free-text note', async () => {
    const { warnings, runTimestamp } = await serviceWith().getRunWarnings();

    const codes = warnings.map((warning) => warning.code);
    for (const code of KNOWN_WARNING_CODES) {
      expect(codes).toContain(code);
    }
    expect(codes).toContain('free-text');
    expect(runTimestamp).toBe('2026-08-31T22:00:00.000Z');
  });

  it('carries counts and type codes but no entity identifiers', async () => {
    const { warnings } = await serviceWith().getRunWarnings();

    const unmapped = warnings.find((warning) => warning.code === 'unmapped-asset-types');
    expect(unmapped?.counts).toMatchObject({ untyped: expect.any(Number), unmapped: expect.any(Number) });
    expect(unmapped?.codes.length).toBeGreaterThan(0);
    expect(JSON.stringify(warnings)).not.toMatch(/asset-\d|UNTIERED-/);
  });
});
