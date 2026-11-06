import { describe, expect, it } from 'vitest';

import { DEFAULT_APP_CONFIG } from '../../mock/config';
import { MOCK_DATASET } from '../../mock/dataset';

import { buildQualityIssues } from './aggregation';
import { computeMetricCounts, computeValuePercent } from './metricComputation';

describe('metricComputation', () => {
  const issues = buildQualityIssues(MOCK_DATASET, DEFAULT_APP_CONFIG);

  it('derives valuePercent from passingCount and eligibleCount', () => {
    const counts = computeMetricCounts(
      'hierarchyCompletionRate',
      MOCK_DATASET,
      DEFAULT_APP_CONFIG,
      {},
      issues
    );

    expect(counts.eligibleCount).toBeGreaterThan(0);
    expect(counts.passingCount + counts.issueCount).toBeGreaterThanOrEqual(0);
    expect(counts.valuePercent).toBe(
      computeValuePercent(counts.passingCount, counts.eligibleCount)
    );
    expect(counts.passingCount).toBeLessThanOrEqual(counts.eligibleCount);
  });

  it('returns null valuePercent when eligibleCount is zero', () => {
    expect(computeValuePercent(0, 0)).toBeNull();

    const counts = computeMetricCounts(
      'hierarchyCompletionRate',
      MOCK_DATASET,
      DEFAULT_APP_CONFIG,
      { facilityId: 'missing-facility' },
      issues
    );

    expect(counts.eligibleCount).toBe(0);
    expect(counts.valuePercent).toBeNull();
  });

  it('reports all passing when no issues exist for a drill-down metric pool', () => {
    const counts = computeMetricCounts(
      'depthOutliers',
      MOCK_DATASET,
      DEFAULT_APP_CONFIG,
      { facilityId: 'demo-south' },
      []
    );

    if (counts.eligibleCount > 0) {
      expect(counts.issueCount).toBe(0);
      expect(counts.passingCount).toBe(counts.eligibleCount);
      expect(counts.valuePercent).toBe(100);
    }
  });

  it('returns deterministic results for the same inputs', () => {
    const first = computeMetricCounts(
      'tsToAssetRate',
      MOCK_DATASET,
      DEFAULT_APP_CONFIG,
      { facilityId: 'demo-north' },
      issues
    );
    const second = computeMetricCounts(
      'tsToAssetRate',
      MOCK_DATASET,
      DEFAULT_APP_CONFIG,
      { facilityId: 'demo-north' },
      issues
    );

    expect(first).toEqual(second);
  });

  it('does not use issueCount as a percentage shortcut', () => {
    const counts = computeMetricCounts(
      'orphansByTier',
      MOCK_DATASET,
      DEFAULT_APP_CONFIG,
      {},
      issues
    );

    if (counts.eligibleCount > 0 && counts.issueCount > 0) {
      expect(counts.valuePercent).not.toBe(Math.max(0, 100 - counts.issueCount));
    }
  });
});
