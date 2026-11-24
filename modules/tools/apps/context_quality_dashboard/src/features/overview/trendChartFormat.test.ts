import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';
import type { SnapshotPointDto } from '../../types';

import {
  contextualisedCountAtPercent,
  formatDimensionTrendPointChange,
} from './trendChartFormat';

describe('trendChartFormat', () => {
  it('derives contextualised count from total and percent', () => {
    expect(contextualisedCountAtPercent(97, 61)).toBe(59);
    expect(contextualisedCountAtPercent(97, 84)).toBe(81);
  });

  it('formats increase vs previous snapshot point', () => {
    const points = samplePoints([60, 84]);
    const t = (key: string) => translate(key, 'en');

    expect(formatDimensionTrendPointChange(points, 0, t)).toBe('No change vs previous point');
    expect(formatDimensionTrendPointChange(points, 1, t)).toBe('↑ +24% vs previous point');
  });
});

function samplePoints(values: readonly number[]): SnapshotPointDto[] {
  return values.map((valuePercent, index) => ({
    metricId: 'hierarchyCompletionRate',
    capturedAt: new Date(Date.UTC(2026, 7, 4 + index * 7)).toISOString(),
    valuePercent,
    scope: {},
  }));
}
