import type { DimensionId, MetricId } from './ids';
import type { Tier } from './tier';

export type QualityMetric = {
  id: MetricId;
  dimensionId: DimensionId;
  labelKey: string;
  direction: 'critical' | 'informational';
  /** Percentage 0–100 when applicable; null for count-only metrics. */
  valuePercent: number | null;
  count: number | null;
  tier: Tier | 'all';
};

export type QualityIssueKind =
  | 'orphan-asset'
  | 'depth-outlier'
  | 'type-gap'
  | 'unmatched-equipment'
  | 'manufacturer-gap'
  | 'orphan-timeseries'
  | 'stale-timeseries'
  | 'tier1-ts-gap'
  | 'inferred-tag-gap'
  | 'unlinked-maintenance'
  | 'wo-without-notification'
  | 'tier1-maintenance-gap'
  | 'low-confidence-annotation'
  | 'unprocessed-file'
  | 'pending-review'
  | 'pattern-annotation-gap'
  | 'unlinked-3d'
  | 'bbox-gap'
  | 'unlinked-file'
  | 'missing-metadata'
  | 'upload-gap'
  | 'stale-annotation-link'
  | 'uncontextualized-revision'
  | 'active-child-retired-parent'
  | 'broken-cross-view-link';

export type QualityIssue = {
  id: string;
  kind: QualityIssueKind;
  dimensionId: DimensionId;
  metricId: MetricId;
  entityId: string;
  entityLabel: string;
  tier: Tier;
  facilityId: string;
  divisionId: string;
  disciplineId: string;
  ownerId: string;
  detail: string;
};

export type SuggestedActionKind =
  | 'export-list'
  | 'notify-owner'
  | 'rerun-metric'
  | 'review-queue'
  | 'escalate-steward';

export type SuggestedAction = {
  id: string;
  issueId: string;
  kind: SuggestedActionKind;
  actionKey: string;
  ownerId: string | null;
};

export type AlertSeverity = 'critical' | 'warning' | 'info';

export type AlertType =
  | 'tier-threshold'
  | 'pending-review'
  | 'recurrence'
  | 'workflow-failure';

export type Alert = {
  id: string;
  type: AlertType;
  severity: AlertSeverity;
  titleKey: string;
  bodyKey: string;
  dimensionId: DimensionId;
  metricId: MetricId | null;
  facilityId: string | null;
  ownerId: string | null;
  status: 'open' | 'acknowledged';
  createdAt: string;
};

export type TrendSnapshot = {
  metricId: MetricId;
  capturedAt: string;
  valuePercent: number;
  windowLabel: string;
  tier?: Tier;
};

export type InsightSource = 'rule' | 'ai';

export type Insight = {
  id: string;
  dimensionId: DimensionId;
  source: InsightSource;
  messageKey: string;
  mentionsTier: Tier | 'all';
  facilityId: string | null;
};

export type ExclusionBucketKind =
  | 'missing-upstream-target'
  | 'invalid-ocr'
  | 'synthetic-timeseries'
  | 'historical-retired'
  | 'location-centric-files'
  | 'user-file-extension'
  | 'user-tag-pattern'
  | 'user-decommissioned-equipment';

export type ExclusionRuleKind = 'file-extension' | 'tag-pattern' | 'lifecycle-status' | 'id-prefix';

export type ExclusionRule = {
  id: string;
  kind: ExclusionRuleKind;
  label: string;
  pattern: string;
  enabled: boolean;
  source: 'system' | 'user';
  excludedCount: number;
};

export type ExclusionBucket = {
  kind: ExclusionBucketKind;
  labelKey: string;
  count: number;
  source: 'system' | 'user';
};
