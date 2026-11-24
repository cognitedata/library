import type { AggregateMetricRowDto } from '../../types';

/**
 * What the two parts of a metric's bar are called. A link metric (equipment linked to an asset) is about contextualisation
 * and keeps "Contextualised" and "Remaining"; a metric about something else says what it counts instead (approved,
 * reviewed, complete, completed, fresh). The words are translations under `dimension.wording.<metric>[.<field>]`.
 */
const OWN_WORDING = new Set([
  'approvalMix',
  'lowConfidenceQueue',
  'pendingReview',
  'serialMfrCompleteness/serialNumber',
  'serialMfrCompleteness/manufacturer',
  'metadataCompleteness/category',
  'metadataCompleteness/description',
  'metadataCompleteness/sourceId',
  'woCompletionRate',
  'freshness',
  'bboxCompleteness',
]);

export type MetricWording = {
  /** Name of the part that passes, the one that does not, and the badge with the share that passes. */
  passingKey: string;
  remainingKey: string;
  badgeKey: string;
};

function wordingId(metric: AggregateMetricRowDto): string {
  return metric.subKey === null || metric.subKey === undefined ? metric.metricId : `${metric.metricId}/${metric.subKey}`;
}

export function metricWording(metric: AggregateMetricRowDto): MetricWording | null {
  const id = wordingId(metric);
  if (!OWN_WORDING.has(id)) {
    return null;
  }
  const base = `dimension.wording.${id.replace('/', '.')}`;
  return { passingKey: `${base}.passing`, remainingKey: `${base}.remaining`, badgeKey: `${base}.badge` };
}

/** The average confidence is a value, not a share of a population: it is shown as a number and has no bar. */
export function isAverageValue(metric: AggregateMetricRowDto): boolean {
  return metric.metricId === 'confidenceAverage';
}
