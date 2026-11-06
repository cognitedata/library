import type {
  DiscoveredRelationship,
  QualityRule,
  QualityRuleCondition,
  QualityRulePreviewResult,
  QualityRulePreviewRow,
  ViewRef,
} from '../../types';

import { computeValuePercent } from './metricComputation';

type CoverageSeed = {
  eligibleCount: number;
  sampleRows: readonly QualityRulePreviewRow[];
};

const COVERAGE_BY_RELATIONSHIP: Readonly<Record<string, CoverageSeed>> = {
  'rmdm:IDI_BOM->cdf_cdm:CogniteAsset': {
    eligibleCount: 1000,
    sampleRows: [
      { entityId: 'bom-001', entityLabel: 'BOM-001', relatedCount: 2, status: 'pass' },
      { entityId: 'bom-002', entityLabel: 'BOM-002', relatedCount: 1, status: 'pass' },
      { entityId: 'bom-003', entityLabel: 'BOM-003', relatedCount: 0, status: 'issue' },
      { entityId: 'bom-004', entityLabel: 'BOM-004', relatedCount: 0, status: 'issue' },
    ],
  },
  'rmdm:IDI_BOM->cdf_cdm:CogniteEquipment': {
    eligibleCount: 1000,
    sampleRows: [
      { entityId: 'bom-001', entityLabel: 'BOM-001', relatedCount: 1, status: 'pass' },
      { entityId: 'bom-002', entityLabel: 'BOM-002', relatedCount: 0, status: 'issue' },
      { entityId: 'bom-003', entityLabel: 'BOM-003', relatedCount: 2, status: 'pass' },
      { entityId: 'bom-004', entityLabel: 'BOM-004', relatedCount: 0, status: 'issue' },
    ],
  },
  'rmdm:IDI_BOM->cdf_idm:CogniteMaintenanceOrder': {
    eligibleCount: 1000,
    sampleRows: [
      { entityId: 'bom-001', entityLabel: 'BOM-001', relatedCount: 2, status: 'pass' },
      { entityId: 'bom-002', entityLabel: 'BOM-002', relatedCount: 0, status: 'issue' },
      { entityId: 'bom-003', entityLabel: 'BOM-003', relatedCount: 1, status: 'pass' },
      { entityId: 'bom-004', entityLabel: 'BOM-004', relatedCount: 0, status: 'issue' },
    ],
  },
  'rmdm:IDI_BOM->cdf_cdm:CogniteLocation': {
    eligibleCount: 1000,
    sampleRows: [
      { entityId: 'bom-001', entityLabel: 'BOM-001', relatedCount: 1, status: 'pass' },
      { entityId: 'bom-002', entityLabel: 'BOM-002', relatedCount: 0, status: 'issue' },
      { entityId: 'bom-003', entityLabel: 'BOM-003', relatedCount: 0, status: 'issue' },
      { entityId: 'bom-004', entityLabel: 'BOM-004', relatedCount: 2, status: 'pass' },
    ],
  },
  'rmdm:Equipment->cdf_cdm:CogniteAsset': {
    eligibleCount: 420,
    sampleRows: [
      { entityId: 'eq-001', entityLabel: 'EQ-001', relatedCount: 1, status: 'pass' },
      { entityId: 'eq-002', entityLabel: 'EQ-002', relatedCount: 0, status: 'issue' },
      { entityId: 'eq-003', entityLabel: 'EQ-003', relatedCount: 2, status: 'pass' },
    ],
  },
  'rmdm:Equipment->cdf_idm:CogniteMaintenanceOrder': {
    eligibleCount: 420,
    sampleRows: [
      { entityId: 'eq-001', entityLabel: 'EQ-001', relatedCount: 0, status: 'issue' },
      { entityId: 'eq-002', entityLabel: 'EQ-002', relatedCount: 1, status: 'pass' },
      { entityId: 'eq-003', entityLabel: 'EQ-003', relatedCount: 0, status: 'issue' },
    ],
  },
  'rmdm:Equipment->cdf_cdm:CogniteLocation': {
    eligibleCount: 420,
    sampleRows: [
      { entityId: 'eq-001', entityLabel: 'EQ-001', relatedCount: 0, status: 'issue' },
      { entityId: 'eq-002', entityLabel: 'EQ-002', relatedCount: 0, status: 'issue' },
      { entityId: 'eq-003', entityLabel: 'EQ-003', relatedCount: 1, status: 'pass' },
    ],
  },
};

/** Deterministic passing counts for minCount=1 (demo data). */
const PASSING_AT_MIN_ONE: Readonly<Record<string, number>> = {
  'rmdm:IDI_BOM->cdf_cdm:CogniteAsset': 850,
  'rmdm:IDI_BOM->cdf_cdm:CogniteEquipment': 695,
  'rmdm:IDI_BOM->cdf_idm:CogniteMaintenanceOrder': 740,
  'rmdm:IDI_BOM->cdf_cdm:CogniteLocation': 520,
  'rmdm:Equipment->cdf_cdm:CogniteAsset': 360,
  'rmdm:Equipment->cdf_idm:CogniteMaintenanceOrder': 290,
  'rmdm:Equipment->cdf_cdm:CogniteLocation': 180,
};

function defaultSeed(relationship: DiscoveredRelationship): CoverageSeed {
  const eligibleCount = relationship.discoveredCount ?? 100;
  return {
    eligibleCount,
    sampleRows: [
      { entityId: 'entity-001', entityLabel: 'ENTITY-001', relatedCount: 1, status: 'pass' },
      { entityId: 'entity-002', entityLabel: 'ENTITY-002', relatedCount: 0, status: 'issue' },
    ],
  };
}

function getSeed(relationship: DiscoveredRelationship): CoverageSeed {
  return COVERAGE_BY_RELATIONSHIP[relationship.relationshipId] ?? defaultSeed(relationship);
}

function countPassingRows(
  rows: readonly QualityRulePreviewRow[],
  condition: QualityRuleCondition
): number {
  return rows.filter((row) => row.relatedCount >= condition.value).length;
}

function computePassingCount(
  relationship: DiscoveredRelationship,
  seed: CoverageSeed,
  condition: QualityRuleCondition
): number {
  if (seed.eligibleCount === 0) {
    return 0;
  }

  if (condition.operator === 'minCount' && condition.value === 1) {
    const preset = PASSING_AT_MIN_ONE[relationship.relationshipId];
    if (preset !== undefined) {
      return preset;
    }
  }

  const passingRows = countPassingRows(seed.sampleRows, condition);
  if (seed.sampleRows.length === 0) {
    return 0;
  }

  return Math.min(
    seed.eligibleCount,
    Math.round((passingRows / seed.sampleRows.length) * seed.eligibleCount)
  );
}

export function computeQualityRulePreview(
  relationship: DiscoveredRelationship,
  condition: QualityRuleCondition
): QualityRulePreviewResult {
  const seed = getSeed(relationship);
  const eligibleCount = seed.eligibleCount;
  const passingCount = computePassingCount(relationship, seed, condition);
  const issueCount = eligibleCount - passingCount;

  const sampleRows: QualityRulePreviewRow[] = seed.sampleRows.map((row) => ({
    ...row,
    status: row.relatedCount >= condition.value ? 'pass' : 'issue',
  }));

  return {
    eligibleCount,
    passingCount,
    issueCount,
    valuePercent: computeValuePercent(passingCount, eligibleCount),
    sampleRows,
  };
}

export function computeQualityRuleMetrics(rule: QualityRule): {
  eligibleCount: number;
  passingCount: number;
  issueCount: number;
  valuePercent: number | null;
} {
  const preview = computeQualityRulePreview(rule.relationship, rule.condition);
  return {
    eligibleCount: preview.eligibleCount,
    passingCount: preview.passingCount,
    issueCount: preview.issueCount,
    valuePercent: preview.valuePercent,
  };
}

export function buildCustomRuleDrilldownRows(
  relationship: DiscoveredRelationship,
  condition: QualityRuleCondition,
  sourceView: ViewRef
): QualityRulePreviewRow[] {
  const preview = computeQualityRulePreview(relationship, condition);
  return preview.sampleRows
    .filter((row) => row.status === 'issue')
    .map((row) => ({
      ...row,
      entityLabel: `${sourceView.externalId} ${row.entityLabel}`,
    }));
}
