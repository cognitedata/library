import { computeValuePercent } from '../../domain/metricMath';
import type { DimensionId, MetricId, QualityIssueKind } from '../../types';

import type { AggregateRow, RowTotals } from './metricRows';
import { sumRows } from './metricRows';

type HeadlineMetric = { metricId: MetricId; subKey?: string };

/** The one metric whose pass rate stands for a dimension on the Overview. */
export const DIMENSION_HEADLINE: Readonly<Record<DimensionId, HeadlineMetric>> = {
  'asset-hierarchy': { metricId: 'hierarchyCompletionRate' },
  'equipment-asset': { metricId: 'associationRate' },
  'time-series': { metricId: 'tsToAssetRate' },
  maintenance: { metricId: 'woToAssetRate' },
  'file-annotation': { metricId: 'approvalMix', subKey: 'Approved' },
  'model-3d': { metricId: 'threeDToAssetRate' },
  files: { metricId: 'fileToAssetRate' },
};

export const TREND_HEADLINE_METRIC_IDS: readonly MetricId[] = [
  'hierarchyCompletionRate',
  'tsToAssetRate',
  'associationRate',
  'fileToAssetRate',
];

/** Metrics written once per sub-key; only one sub-key stands for the whole metric. */
const REPRESENTATIVE_SUBKEY: Partial<Record<MetricId, string>> = {
  approvalMix: 'Approved',
};

/**
 * Metrics measured once per field over the same entities (serial number and manufacturer of every equipment, the
 * three metadata fields of every file). Adding the fields together counts each entity once per field, so each field
 * is reported on its own.
 */
const FIELDS_OF_SPLIT_METRIC: Partial<Record<MetricId, readonly string[]>> = {
  serialMfrCompleteness: ['serialNumber', 'manufacturer'],
  metadataCompleteness: ['category', 'description', 'sourceId'],
};

export function isSplitBySubKey(metricId: MetricId): boolean {
  return FIELDS_OF_SPLIT_METRIC[metricId] !== undefined;
}

/** The fields of a split metric that get a card, in display order. A field name this dashboard does not know gets none. */
export function splitMetricFields(metricId: MetricId, present: readonly (string | null)[]): string[] {
  return (FIELDS_OF_SPLIT_METRIC[metricId] ?? []).filter((field) => present.includes(field));
}

export function selectMetricRows(
  rows: readonly AggregateRow[],
  metricId: MetricId,
  subKey?: string
): AggregateRow[] {
  if (subKey !== undefined) {
    return rows.filter((row) => row.metricId === metricId && row.subKey === subKey);
  }
  const representative = REPRESENTATIVE_SUBKEY[metricId];
  return rows.filter(
    (row) => row.metricId === metricId && (representative === undefined || row.subKey === representative)
  );
}

export function selectHeadlineRows(
  rows: readonly AggregateRow[],
  dimensionId: DimensionId
): AggregateRow[] {
  const headline = DIMENSION_HEADLINE[dimensionId];
  return rows.filter(
    (row) =>
      row.dimensionId === dimensionId &&
      row.metricId === headline.metricId &&
      (headline.subKey === undefined || row.subKey === headline.subKey)
  );
}

/** confidenceAverage stores the sum of confidences (0-1) in valueSum; everything else is passing/eligible. */
export function metricPercent(metricId: MetricId, totals: RowTotals): number | null {
  if (metricId === 'confidenceAverage') {
    return totals.eligible === 0 ? null : Math.round((totals.valueSum / totals.eligible) * 100);
  }
  return computeValuePercent(totals.passing, totals.eligible);
}

export function percentOfRows(metricId: MetricId, rows: readonly AggregateRow[]): number | null {
  return metricPercent(metricId, sumRows(rows));
}

export function metricDirection(metricId: MetricId): 'critical' | 'informational' {
  return metricId.includes('Rate') ||
    metricId.includes('Coverage') ||
    metricId.includes('Completeness')
    ? 'critical'
    : 'informational';
}

export const ISSUE_KIND_BY_METRIC: Partial<Record<MetricId, QualityIssueKind>> = {
  orphansByTier: 'orphan-asset',
  depthOutliers: 'depth-outlier',
  typeCoverageGaps: 'type-gap',
  unmatchedWithCandidates: 'unmatched-equipment',
  orphansByPrefix: 'orphan-timeseries',
  staleTimeSeries: 'stale-timeseries',
  tier1CoverageGaps: 'tier1-ts-gap',
  woWithoutNotification: 'wo-without-notification',
  unlinkedMaintenance: 'unlinked-maintenance',
  lowConfidenceQueue: 'low-confidence-annotation',
  pendingReview: 'pending-review',
  unlinkedTier1: 'unlinked-3d',
  bboxGaps: 'bbox-gap',
  unlinkedByCategory: 'unlinked-file',
  activeChildrenUnderRetiredParents: 'active-child-retired-parent',
};
