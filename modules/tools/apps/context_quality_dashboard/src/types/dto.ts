import type { SliceFilters } from './filters';
import type { DimensionId, MetricId, TrendWindow } from './ids';
import type {
  ExclusionBucket,
  ExclusionRule,
  ExclusionRuleKind,
  InsightSource,
  QualityIssueKind,
} from './quality';
import type { Tier } from './tier';

export type TierScore = {
  tier: Tier;
  /** Null when the tier has no eligible entities in the active slice. */
  scorePercent: number | null;
  targetPercent: number;
  scored: boolean;
};

/**
 * `not-applicable`: the dimension has rows, but nothing was eligible to measure (or a needed mapping is missing), so it has
 * no score and must not read as healthy. `disabled` means the dimension has no rows at all.
 */
export type DimensionQualityStatus = 'good' | 'weak' | 'critical' | 'disabled' | 'not-applicable';

/** Entities with no tier assigned (the backend leaves the tier out when it cannot tell). Never counted as tier 3. */
export type UntieredScoreDto = {
  /** Null when no untiered entity was eligible in the active slice. */
  scorePercent: number | null;
  eligibleCount: number;
  passingCount: number;
};

/**
 * What a dimension's overall score is made of. `tiered`: the scored tiers (1 and 2), averaged. `pooled`: tiering is switched
 * off, so every entity is scored together. `pooled-fallback`: tiering is on but no tier 1 or 2 entity was eligible (for
 * example none has a tier assigned), so the score pools all entities rather than inventing a per-tier result.
 */
export type ScoreBasis = 'tiered' | 'pooled' | 'pooled-fallback';

export type NotApplicableReasonKind = 'mapping-empty' | 'all-excluded' | 'no-eligible-entities';

/** Why a metric could not be measured, taken from the run's `metric-not-applicable` warning. */
export type NotApplicableReasonDto = {
  kind: NotApplicableReasonKind;
  /** The configuration key that is empty, for `mapping-empty`. */
  mapping: string | null;
};

/** One run warning, reduced to what the dashboard shows: counts and codes, never entity ids. */
export type RunWarningDto = {
  code: string;
  message: string;
  subject: string | null;
  reason: string | null;
  counts: Record<string, number>;
  /** Type codes the warning lists (up to 10); not entity ids. */
  codes: string[];
};

export type RunWarningsDto = {
  runTimestamp: string;
  warnings: RunWarningDto[];
};

export type DimensionCardDto = {
  dimensionId: DimensionId;
  nameKey: string;
  enabled: boolean;
  optional: boolean;
  overallPercent: number | null;
  tierScores: TierScore[];
  /** Entities with no tier; null when none were eligible or tiering is off. */
  untiered: UntieredScoreDto | null;
  scoreBasis: ScoreBasis;
  issueCount: number | null;
  status: DimensionQualityStatus;
  weakestTier: Tier | null;
  /** Entities the dimension's headline metric measured, and how many of them pass it. Absent from older producers. */
  eligibleCount?: number;
  passingCount?: number;
  /** When the run this card comes from finished. A run can measure some dimensions only, so cards can differ. */
  measuredAt?: string;
};

export type AlertSummaryDto = {
  alertId: string;
  type: string;
  severity: string;
  titleKey: string;
  dimensionId: DimensionId;
  createdAt: string;
  currentPercent: number | null;
  targetPercent: number | null;
};

export type OverviewTrendDto = {
  currentPercent: number | null;
  previousPercent: number | null;
  changePts: number | null;
};

export type InsightSummaryDto = {
  id: string;
  dimensionId: DimensionId;
  source: InsightSource;
  messageKey: string;
  actionKey: string;
  mentionsTier: Tier | 'all';
};

export type UserExclusionSummaryDto = {
  id: string;
  label: string;
  count: number;
};

export type OverviewScopeDto = {
  entityCount: number;
  facilityId?: string;
  divisionId?: string;
  disciplineId?: string;
  ownerId?: string;
};

export type HeadlineMetricDto = {
  metricId: MetricId;
  labelKey: string;
};

export type OverviewDto = {
  overallAggregatePercent: number | null;
  overallTrend: OverviewTrendDto | null;
  tierScores: TierScore[];
  /** Entities with no tier, across the enabled dimensions; null when there are none or tiering is off. */
  untiered: UntieredScoreDto | null;
  scope: OverviewScopeDto;
  dimensionCards: DimensionCardDto[];
  excludedSummary: ExclusionBucket[];
  userExclusionSummary: UserExclusionSummaryDto[];
  insights: InsightSummaryDto[];
  insightIds: string[];
  alertSummaries: AlertSummaryDto[];
  headlineMetrics: HeadlineMetricDto[];
  headlineSnapshots: SnapshotPointDto[];
};

export type AggregateMetricRowDto = {
  metricId: MetricId;
  /** Set for a metric measured once per field (e.g. serial number, manufacturer): each field is its own card. */
  subKey?: string | null;
  /** The drill-down metric the card opens, when it is not the card's own metric (a per-field card lists its field). */
  drilldownMetricId?: MetricId | null;
  labelKey: string;
  direction: 'critical' | 'informational';
  eligibleCount: number;
  passingCount: number;
  issueCount: number;
  /** Null when nothing was eligible: never 0% and never a made-up value. A measured zero is 0. */
  valuePercent: number | null;
  tier: Tier | 'all';
  extended: boolean;
  hasDrilldown: boolean;
  actionKey: string;
  status: 'good' | 'weak' | 'critical' | 'not-applicable';
  /** True when no entity was eligible for this metric. */
  notApplicable: boolean;
  /** Why, when the run's warnings say; null otherwise (older runs carry no warnings). */
  notApplicableReason: NotApplicableReasonDto | null;
};

export type SnapshotScopeDto = {
  facilityId?: string;
  divisionId?: string;
  disciplineId?: string;
  ownerId?: string;
  tier?: Tier;
};

export type DrilldownSummaryDto = {
  metricId: MetricId;
  labelKey: string;
  count: number;
  /** True when the list was cut at the read limit, so `count` is a lower bound. */
  atLeast?: boolean;
  actionKey: string;
};

export type DimensionDto = {
  dimensionId: DimensionId;
  nameKey: string;
  enabled: boolean;
  optional: boolean;
  status: DimensionQualityStatus;
  entityCount: number | null;
  tierScores: TierScore[];
  untiered: UntieredScoreDto | null;
  scoreBasis: ScoreBasis;
  overallPercent: number | null;
  aggregateRows: AggregateMetricRowDto[];
  excludedBuckets: ExclusionBucket[];
  userExclusionSummary: UserExclusionSummaryDto[];
  insightIds: string[];
  extendedMetricsAvailable: boolean;
};

export type DrilldownRowDto = {
  id: string;
  cells: Record<string, string>;
  /** Classification label per kind, e.g. { assetType: 'Pump' }; empty when the project has none. */
  classes: Record<string, string>;
  /** Null when the backend assigned no tier. */
  tier: Tier | null;
  ownerId: string;
};

export type DrilldownDto = {
  metricId: MetricId;
  dimensionId: DimensionId;
  labelKey: string;
  actionKey: string;
  ownerId: string | null;
  columnKeys: string[];
  rows: DrilldownRowDto[];
  /** Set when the list holds only the first `limit` items: there are more. */
  partial: { limit: number } | null;
};

export type SnapshotPointDto = {
  metricId: MetricId;
  capturedAt: string;
  valuePercent: number;
  scope: SnapshotScopeDto;
  tier?: Tier;
};

export type AlertDto = {
  id: string;
  type: string;
  severity: string;
  status: string;
  titleKey: string;
  bodyKey: string;
  actionKey: string;
  dimensionId: DimensionId;
  metricId: MetricId | null;
  facilityId: string | null;
  ownerId: string | null;
  affectedCount: number | null;
  createdAt: string;
};

export type AlertSummaryCountsDto = {
  total: number;
  critical: number;
  warning: number;
  informational: number;
};

export type AlertsDto = {
  alerts: AlertDto[];
  summary: AlertSummaryCountsDto;
};

export type IssueDto = {
  id: string;
  kind: QualityIssueKind;
  dimensionId: DimensionId;
  metricId: MetricId;
  entityId: string;
  entityLabel: string;
  /** Null when the backend assigned no tier. */
  tier: Tier | null;
  facilityId: string;
  ownerId: string;
  detail: string;
  actionKey: string;
  /** Classification label per kind, e.g. { assetType: 'Pump' }; empty when the project has none. */
  classes: Record<string, string>;
};

export type IssuesSummaryDto = {
  total: number;
  critical: number;
  important: number;
  standard: number;
  /** Issues whose entity has no tier. */
  unassigned: number;
};

/** The issues of one metric: one of the parts the Issues page loads, so it can show each as it arrives. */
export type MetricIssuesDto = {
  issues: IssueDto[];
  /** True when the metric had more items than `limit`, so `issues` holds only the first `limit`. */
  cut: boolean;
  limit: number;
};

export type IssuesDto = {
  issues: IssueDto[];
  summary: IssuesSummaryDto;
  /**
   * Set when the list is not complete: it holds at most `limitPerMetric` items of each of `metricCount` metrics, and
   * `cutMetricIds` are the metrics that had more than that, so their counts are lower bounds.
   */
  partial: { limitPerMetric: number; metricCount: number; cutMetricIds: MetricId[] } | null;
};

export type ClassificationRowDto = {
  id: string;
  /** Display name taken from the data (type, class or category node). */
  label: string;
  /** Translation keys, set only for demo rows that have curated copy. */
  labelKey?: string;
  descriptionKey?: string;
  totalCount: number;
  contextualisedCount: number;
  coveragePercent: number | null;
};

export type ClassificationGroupDto = {
  /** Which classification this is, e.g. assetType, equipmentType, fileCategory. */
  kind: string;
  rows: ClassificationRowDto[];
};

export type ClassificationsDto = {
  groups: ClassificationGroupDto[];
};

/** Identifies one configured metric across runs (data model + view + property). */
export type ConfiguredMetricKey = {
  /** `space:externalId:version` of the data model. */
  modelId: string;
  view: string;
  property: string;
};

/** One metric the user named in the config file: a property of a view, counted over the view's instances. */
export type ConfiguredMetricDto = {
  /** The property name, as written in the config file. */
  label: string;
  modelId: string;
  view: string;
  totalCount: number;
  linkedCount: number;
  /** Null when no instance was counted: nothing to measure is not 0%. */
  coveragePercent: number | null;
};

/** One historical coverage point for a configured metric at a completed run. */
export type ConfiguredMetricTrendPointDto = {
  capturedAt: string;
  valuePercent: number;
  linkedCount: number;
  totalCount: number;
};

/** The metrics of one view of one data model. Names come from the run, so any config file produces any groups. */
export type ConfiguredGroupDto = {
  id: string;
  /** `space:externalId:version` of the data model this view belongs to. */
  modelId: string;
  modelLabel: string;
  viewLabel: string;
  metrics: ConfiguredMetricDto[];
};

/** Configured metrics for one data model from the run/config file. */
export type ConfiguredDataModelDto = {
  /** `space:externalId:version`. */
  id: string;
  label: string;
  groups: ConfiguredGroupDto[];
};

export type ConfiguredMetricsDto = {
  models: ConfiguredDataModelDto[];
};

/** One instance of a configured view whose link is not set. */
export type ConfiguredDrilldownRowDto = {
  id: string;
  /** The instance's name when its view has one, otherwise its external id. */
  entity: string;
  entityId: string;
  /** The instance space. */
  space: string;
  facilityId: string | null;
  detail: string;
};

/** The instances behind one configured metric's "not linked" count, as far as the run listed them. */
export type ConfiguredDrilldownDto = {
  key: ConfiguredMetricKey;
  rows: ConfiguredDrilldownRowDto[];
  /** Set when the run cut the list: the most rows listed per facility, and how many were left out. */
  cutAt: number | null;
  dropped: number;
};

/** The models, views and properties the latest run measured because a config file named them. */
export type RunConfigurationDto = {
  runTimestamp: string;
  /** Empty when the run was not started with a config file. */
  models: { model: string; views: { view: string; properties: string[] }[] }[];
};

export type RunScope =
  | { kind: 'all' }
  | { kind: 'dimension'; dimensionId: DimensionId }
  | { kind: 'metric'; metricId: MetricId };

export type RunStatusDto = {
  /** When the latest completed run finished, whatever it measured. */
  lastSuccessAt: string;
  inProgress: boolean;
  scope: RunScope;
  /** When each dimension was last measured: a run can measure some dimensions only. Absent for a producer that says no. */
  dimensionRuns?: Partial<Record<DimensionId, string>>;
  /** When the metrics of a config file were last measured; absent when they never were. */
  configuredRunAt?: string;
};

export type ViewBindingDto = {
  objectType: string;
  space: string;
  externalId: string;
  version: string;
};

export type OwnerMappingDto = {
  id: string;
  prefix: string;
  ownerId: string;
};

export type AppConfigDto = {
  maintenanceViewsAvailable: boolean;
  failureTaxonomyAvailable: boolean;
  fileAnnotationAcceleratorAvailable: boolean;
  lifecycleMetadataAvailable: boolean;
  model3dAvailable: boolean;
  fileAnnotationDimensionEnabled: boolean;
  maintenanceDimensionEnabled: boolean;
  /** When false, scores are pooled across all entities and tier breakdowns are hidden. */
  tieringEnabled: boolean;
  viewBindings: ViewBindingDto[];
  ownerMappings: OwnerMappingDto[];
  exclusionRules: ExclusionRule[];
  scheduleLabel: string;
  notificationRecipients: string[];
};

export type ExclusionRuleDraft = {
  kind: ExclusionRuleKind;
  pattern: string;
};

export type NotifyOwnerInput = {
  metricId: MetricId;
  ownerId: string;
  entityIds: string[];
};

export type NotifyOwnerResult = {
  ok: true;
};

export type { SliceFilters, TrendWindow };
