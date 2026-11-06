import { describe, expect, it } from 'vitest';

import { MOCK_DATASET } from '../../mock/dataset';
import { HEADLINE_METRIC_IDS } from '../../mock/snapshots';

import { createMockServices } from './createMockServices';
import {
  computeOverallTrend,
  computePortfolioAggregatePercent,
} from './overviewAggregation';

describe('overviewAggregation', () => {
  it('returns null aggregate when no enabled dimension cards are scored', () => {
    const aggregate = computePortfolioAggregatePercent([
      {
        dimensionId: 'model-3d',
        nameKey: 'dimension.model-3d',
        enabled: false,
        optional: true,
        overallPercent: null,
        tierScores: [],
        untiered: null,
        scoreBasis: 'tiered',
        issueCount: null,
        status: 'disabled',
        weakestTier: null,
      },
    ]);

    expect(aggregate).toBeNull();
  });

  it('counts zero scoped entities for unknown facility filters', async () => {
    const { qualityDashboardService } = createMockServices({ latency: 0 });
    const overview = await qualityDashboardService.getOverview({ facilityId: 'missing-facility' });

    expect(overview.scope.entityCount).toBe(0);
    expect(overview.overallAggregatePercent).toBeNull();
    expect(overview.overallTrend).toBeNull();
  });

  it('computes a 30-day overall trend from headline snapshots', () => {
    const trend = computeOverallTrend(MOCK_DATASET.snapshots, HEADLINE_METRIC_IDS, {});

    expect(trend).not.toBeNull();
    expect(trend?.currentPercent).not.toBeNull();
    expect(trend?.previousPercent).not.toBeNull();
    expect(trend?.changePts).toBe(
      (trend?.currentPercent ?? 0) - (trend?.previousPercent ?? 0)
    );
  });
});
