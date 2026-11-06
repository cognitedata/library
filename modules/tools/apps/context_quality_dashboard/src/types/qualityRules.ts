import type { DrilldownRowDto } from './dto';
import type { AlertSeverity } from './quality';
import type { Tier } from './tier';

/** Customer-defined rule types. Only relationship-coverage is implemented. */
export type QualityRuleType =
  | 'relationship-coverage'
  | 'property-completeness'
  | 'value-validity'
  | 'custom-condition';

export type ViewRef = {
  space: string;
  externalId: string;
};

export type DiscoveredDataModel = {
  space: string;
  displayName: string;
};

export type DiscoveredView = {
  space: string;
  externalId: string;
  displayName: string;
  instanceCount: number;
};

/** Relationship target is discovered data — not a hardcoded enum. */
export type DiscoveredRelationship = {
  relationshipId: string;
  sourceView: ViewRef;
  targetView: ViewRef;
  targetDisplayName: string;
  description?: string;
  discoveredCount?: number;
};

export type QualityRuleConditionOperator = 'minCount';

export type QualityRuleCondition = {
  operator: QualityRuleConditionOperator;
  value: number;
};

export type QualityRuleValidationError =
  | 'name-required'
  | 'source-required'
  | 'relationship-required'
  | 'min-count-invalid'
  | 'owner-required'
  | 'owner-invalid'
  | 'duplicate-rule';

export type QualityRule = {
  ruleId: string;
  name: string;
  type: QualityRuleType;
  sourceView: ViewRef;
  relationship: DiscoveredRelationship;
  condition: QualityRuleCondition;
  severity: AlertSeverity;
  ownerId: string;
  tier: Tier;
  enabled: boolean;
};

export type QualityRuleDraft = {
  ruleId?: string;
  name: string;
  type: QualityRuleType;
  sourceView: ViewRef;
  relationship: DiscoveredRelationship | null;
  condition: QualityRuleCondition;
  severity: AlertSeverity;
  ownerId: string;
  tier: Tier;
  enabled: boolean;
};

export type QualityRulePreviewRow = {
  entityId: string;
  entityLabel: string;
  relatedCount: number;
  status: 'pass' | 'issue';
};

export type QualityRulePreviewResult = {
  eligibleCount: number;
  passingCount: number;
  issueCount: number;
  valuePercent: number | null;
  sampleRows: readonly QualityRulePreviewRow[];
};

/** Live result for a saved custom rule — parallel to standard AggregateMetricRowDto, keyed by ruleId. */
export type CustomQualityMetricDto = {
  ruleId: string;
  name: string;
  sourceView: ViewRef;
  relationshipTargetDisplayName: string;
  eligibleCount: number;
  passingCount: number;
  issueCount: number;
  valuePercent: number | null;
  severity: AlertSeverity;
  tier: Tier;
  ownerId: string;
  actionKey: string;
  enabled: boolean;
};

export type CustomRuleDrilldownDto = {
  ruleId: string;
  labelKey: string;
  actionKey: string;
  ownerId: string | null;
  columnKeys: string[];
  rows: DrilldownRowDto[];
};
