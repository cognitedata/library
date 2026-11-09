import { describe, expect, it } from 'vitest';

import {
  dimensionForHeadlineMetric,
  primaryTrendMetricForDimension,
  trendDeltaClassName,
} from './trendMetricPresentation';

describe(dimensionForHeadlineMetric.name, () => {
  it('maps each headline metric to the dimension it summarises', () => {
    expect(dimensionForHeadlineMetric('hierarchyCompletionRate')).toBe('asset-hierarchy');
    expect(dimensionForHeadlineMetric('tsToAssetRate')).toBe('time-series');
    expect(dimensionForHeadlineMetric('associationRate')).toBe('equipment-asset');
    expect(dimensionForHeadlineMetric('fileToAssetRate')).toBe('files');
  });

  it('returns null for metrics that are not headline metrics', () => {
    expect(dimensionForHeadlineMetric('orphanRate')).toBeNull();
  });

});

describe(primaryTrendMetricForDimension.name, () => {
  it('maps each catalog dimension to a primary trend metric', () => {
    expect(primaryTrendMetricForDimension('asset-hierarchy')).toBe('hierarchyCompletionRate');
    expect(primaryTrendMetricForDimension('maintenance')).toBe('failureDocumentationRate');
    expect(primaryTrendMetricForDimension('model-3d')).toBe('threeDToAssetRate');
  });
});

describe(trendDeltaClassName.name, () => {
  it('colours improving, declining, and flat trends differently', () => {
    expect(trendDeltaClassName('up')).toBe('text-success-foreground');
    expect(trendDeltaClassName('down')).toBe('text-destructive-foreground');
    expect(trendDeltaClassName('stable')).toBe('text-muted-foreground');
  });
});
