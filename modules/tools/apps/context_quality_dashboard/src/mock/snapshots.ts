import type { MetricId, TrendSnapshot  } from '../types';

const BASELINE_WEEKLY: readonly number[] = [72, 74, 73, 75, 76, 78, 61, 79];

function buildWeeklySnapshots(metricId: MetricId, values: readonly number[]): TrendSnapshot[] {
  const start = new Date('2026-07-07T00:00:00.000Z');
  return values.map((valuePercent, index) => {
    const capturedAt = new Date(start);
    capturedAt.setUTCDate(start.getUTCDate() + index * 7);
    return {
      metricId,
      capturedAt: capturedAt.toISOString(),
      valuePercent,
      windowLabel: 'weekly',
    };
  });
}

export const MOCK_SNAPSHOTS: readonly TrendSnapshot[] = [
  ...buildWeeklySnapshots('hierarchyCompletionRate', BASELINE_WEEKLY),
  ...buildWeeklySnapshots('tsToAssetRate', [68, 69, 70, 71, 70, 72, 71, 73]),
  ...buildWeeklySnapshots('associationRate', [74, 75, 76, 75, 77, 78, 79, 80]),
  ...buildWeeklySnapshots('fileToAssetRate', [52, 53, 54, 55, 54, 56, 57, 58]),
  ...buildWeeklySnapshots('failureDocumentationRate', [78, 79, 80, 78, 77, 78, 61, 82]),
  ...buildWeeklySnapshots('annotationCount', [55, 56, 58, 57, 59, 60, 61, 62]),
  ...buildWeeklySnapshots('threeDToAssetRate', [48, 49, 50, 51, 50, 52, 53, 54]),
];

export const HEADLINE_METRIC_IDS: readonly MetricId[] = [
  'hierarchyCompletionRate',
  'tsToAssetRate',
  'associationRate',
  'fileToAssetRate',
];
