import type { ConfiguredMetricTrendPointDto, MetricId, SnapshotPointDto } from '../../types';

/** Chart layer only; metric id is unused but required by the snapshot DTO shape. */
const CHART_METRIC_PLACEHOLDER: MetricId = 'fileToAssetRate';

export function configuredMetricTrendToSnapshots(
  points: readonly ConfiguredMetricTrendPointDto[]
): SnapshotPointDto[] {
  return points.map((point) => ({
    metricId: CHART_METRIC_PLACEHOLDER,
    capturedAt: point.capturedAt,
    valuePercent: point.valuePercent,
    scope: {},
  }));
}
