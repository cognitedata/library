import { describe, expect, it } from 'vitest';

import { MOCK_ALERTS } from '../../mock/alerts';
import { MOCK_DATASET } from '../../mock/dataset';

import { buildTrendPoints, filterAlertsBySlice } from './trends';

describe('trend generation', () => {
  it('returns weekly points for a metric within the selected window', () => {
    const points7d = buildTrendPoints(MOCK_DATASET.snapshots, 'failureDocumentationRate', '7d', {});
    const points90d = buildTrendPoints(MOCK_DATASET.snapshots, 'failureDocumentationRate', '90d', {});

    expect(points7d.length).toBeLessThanOrEqual(points90d.length);
    expect(points90d.some((point) => point.valuePercent === 61)).toBe(true);
  });

  it('sorts snapshot points chronologically', () => {
    const points = buildTrendPoints(MOCK_DATASET.snapshots, 'hierarchyCompletionRate', '90d', {});
    const timestamps = points.map((point) => Date.parse(point.capturedAt));

    for (let index = 1; index < timestamps.length; index += 1) {
      expect(timestamps[index]).toBeGreaterThanOrEqual(timestamps[index - 1] ?? 0);
    }
  });

  it('includes metric identity and scope on each snapshot point', () => {
    const points = buildTrendPoints(MOCK_DATASET.snapshots, 'tsToAssetRate', '30d', {
      facilityId: 'demo-central',
    });

    expect(points.length).toBeGreaterThan(0);
    expect(points.every((point) => point.metricId === 'tsToAssetRate')).toBe(true);
    expect(points.every((point) => point.scope.facilityId === 'demo-central')).toBe(true);
  });

  it('adjusts values when facility filter changes', () => {
    const north = buildTrendPoints(MOCK_DATASET.snapshots, 'hierarchyCompletionRate', '90d', {
      facilityId: 'demo-north',
    });
    const central = buildTrendPoints(MOCK_DATASET.snapshots, 'hierarchyCompletionRate', '90d', {
      facilityId: 'demo-central',
    });

    expect(north[north.length - 1]?.valuePercent).not.toBe(central[central.length - 1]?.valuePercent);
  });
});

describe('alert generation', () => {
  it('filters alerts by facility slice', () => {
    const northAlerts = filterAlertsBySlice(MOCK_ALERTS, { facilityId: 'demo-north' });

    expect(northAlerts.length).toBeGreaterThan(0);
    expect(northAlerts.every((alert) => alert.facilityId === 'demo-north')).toBe(true);
  });

  it('includes all four alert types in the fixture', () => {
    const types = new Set(MOCK_ALERTS.map((alert) => alert.type));
    expect(types.has('tier-threshold')).toBe(true);
    expect(types.has('pending-review')).toBe(true);
    expect(types.has('recurrence')).toBe(true);
    expect(types.has('workflow-failure')).toBe(true);
  });
});
