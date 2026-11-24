import type { DimensionId, MetricId } from '../../types';

import type { TrendDirection } from './overviewPresentation';

const HEADLINE_DIMENSION: Partial<Record<MetricId, DimensionId>> = {
  hierarchyCompletionRate: 'asset-hierarchy',
  tsToAssetRate: 'time-series',
  associationRate: 'equipment-asset',
  fileToAssetRate: 'files',
};

const PRIMARY_TREND_METRIC: Record<DimensionId, MetricId> = {
  'asset-hierarchy': 'hierarchyCompletionRate',
  'time-series': 'tsToAssetRate',
  'equipment-asset': 'associationRate',
  files: 'fileToAssetRate',
  maintenance: 'failureDocumentationRate',
  'file-annotation': 'annotationCount',
  'model-3d': 'threeDToAssetRate',
};

export function dimensionForHeadlineMetric(metricId: MetricId): DimensionId | null {
  return HEADLINE_DIMENSION[metricId] ?? null;
}

export function primaryTrendMetricForDimension(dimensionId: DimensionId): MetricId {
  return PRIMARY_TREND_METRIC[dimensionId];
}

export function trendDeltaClassName(direction: TrendDirection): string {
  if (direction === 'up') {
    return 'text-success-foreground';
  }
  if (direction === 'down') {
    return 'text-destructive-foreground';
  }
  return 'text-muted-foreground';
}
