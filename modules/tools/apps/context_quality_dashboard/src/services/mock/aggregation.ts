import type { MockDataset } from '../../mock/dataset';
import type { AppConfigDto, DimensionId, MetricId, QualityIssue, SliceFilters, Tier } from '../../types';
import { matchesSlice } from '../../types';

import {
  isEligibleEquipment,
  isEligibleFile,
  isEligibleTimeSeries,
  percent,
} from './exclusionLogic';

const DEPTH_THRESHOLD = 10;

function sliceFilter<T extends {
  facilityId: string;
  divisionId: string;
  disciplineId: string;
  ownerId: string;
}>(items: readonly T[], filters: SliceFilters): T[] {
  return items.filter((item) => matchesSlice(item, filters));
}

function tierFilter<T extends { tier: Tier }>(items: readonly T[], tier: Tier): T[] {
  return items.filter((item) => item.tier === tier);
}

export function buildQualityIssues(dataset: MockDataset, config: AppConfigDto): QualityIssue[] {
  const issues: QualityIssue[] = [];

  for (const asset of dataset.assets) {
    if (asset.isOrphan) {
      issues.push({
        id: `issue-orphan-${asset.id}`,
        kind: 'orphan-asset',
        dimensionId: 'asset-hierarchy',
        metricId: 'orphansByTier',
        entityId: asset.id,
        entityLabel: asset.externalId,
        tier: asset.tier,
        facilityId: asset.facilityId,
        divisionId: asset.divisionId,
        disciplineId: asset.disciplineId,
        ownerId: asset.ownerId,
        detail: 'Asset has no parent in the hierarchy.',
      });
    }
    if (asset.hierarchyDepth > DEPTH_THRESHOLD) {
      issues.push({
        id: `issue-depth-${asset.id}`,
        kind: 'depth-outlier',
        dimensionId: 'asset-hierarchy',
        metricId: 'depthOutliers',
        entityId: asset.id,
        entityLabel: asset.externalId,
        tier: asset.tier,
        facilityId: asset.facilityId,
        divisionId: asset.divisionId,
        disciplineId: asset.disciplineId,
        ownerId: asset.ownerId,
        detail: `Hierarchy depth ${asset.hierarchyDepth} exceeds threshold.`,
      });
    }
    if (asset.assetType === null && (asset.tier === 1 || asset.tier === 2)) {
      issues.push({
        id: `issue-typegap-${asset.id}`,
        kind: 'type-gap',
        dimensionId: 'asset-hierarchy',
        metricId: 'typeCoverageGaps',
        entityId: asset.id,
        entityLabel: asset.externalId,
        tier: asset.tier,
        facilityId: asset.facilityId,
        divisionId: asset.divisionId,
        disciplineId: asset.disciplineId,
        ownerId: asset.ownerId,
        detail: 'Asset is missing type classification.',
      });
    }
    if (asset.status === 'retired') {
      const activeChildren = dataset.assets.filter(
        (child) => child.parentId === asset.id && child.status === 'active'
      );
      for (const child of activeChildren) {
        issues.push({
          id: `issue-retired-child-${child.id}`,
          kind: 'active-child-retired-parent',
          dimensionId: 'asset-hierarchy',
          metricId: 'activeChildrenUnderRetiredParents',
          entityId: child.id,
          entityLabel: child.externalId,
          tier: child.tier,
          facilityId: child.facilityId,
          divisionId: child.divisionId,
          disciplineId: child.disciplineId,
          ownerId: child.ownerId,
          detail: `Active child under retired parent ${asset.externalId}.`,
        });
      }
    }
  }

  for (const eq of dataset.equipment) {
    if (!isEligibleEquipment(eq, config)) {
      continue;
    }
    if (eq.linkedAssetId === null && eq.candidateAssetId !== null) {
      issues.push({
        id: `issue-unmatched-${eq.id}`,
        kind: 'unmatched-equipment',
        dimensionId: 'equipment-asset',
        metricId: 'unmatchedWithCandidates',
        entityId: eq.id,
        entityLabel: eq.externalId,
        tier: eq.tier,
        facilityId: eq.facilityId,
        divisionId: eq.divisionId,
        disciplineId: eq.disciplineId,
        ownerId: eq.ownerId,
        detail: `Candidate match ${eq.candidateMatchPercent ?? 0}% available.`,
      });
    }
    if (eq.manufacturer === 'Emerson' && eq.serialNumber === null) {
      issues.push({
        id: `issue-mfr-${eq.id}`,
        kind: 'manufacturer-gap',
        dimensionId: 'equipment-asset',
        metricId: 'completenessByManufacturer',
        entityId: eq.id,
        entityLabel: eq.externalId,
        tier: eq.tier,
        facilityId: eq.facilityId,
        divisionId: eq.divisionId,
        disciplineId: eq.disciplineId,
        ownerId: eq.ownerId,
        detail: 'Emerson equipment missing serial number.',
      });
    }
  }

  for (const ts of dataset.timeSeries) {
    if (!isEligibleTimeSeries(ts, config)) {
      continue;
    }
    if (ts.linkedAssetId === null && ts.tagPrefix === 'PI-COMP3-') {
      issues.push({
        id: `issue-ts-orphan-${ts.id}`,
        kind: 'orphan-timeseries',
        dimensionId: 'time-series',
        metricId: 'orphansByPrefix',
        entityId: ts.id,
        entityLabel: ts.externalId,
        tier: ts.tier,
        facilityId: ts.facilityId,
        divisionId: ts.divisionId,
        disciplineId: ts.disciplineId,
        ownerId: ts.ownerId,
        detail: `Orphan time series with prefix ${ts.tagPrefix}`,
      });
    }
    const daysSinceUpdate = Math.floor(
      (Date.parse('2026-09-01T00:00:00.000Z') - Date.parse(ts.lastUpdatedAt)) / 86_400_000
    );
    if (daysSinceUpdate > 30) {
      issues.push({
        id: `issue-ts-stale-${ts.id}`,
        kind: 'stale-timeseries',
        dimensionId: 'time-series',
        metricId: 'staleTimeSeries',
        entityId: ts.id,
        entityLabel: ts.externalId,
        tier: ts.tier,
        facilityId: ts.facilityId,
        divisionId: ts.divisionId,
        disciplineId: ts.disciplineId,
        ownerId: ts.ownerId,
        detail: `No update for ${daysSinceUpdate} days.`,
      });
    }
    if (ts.tier === 1 && ts.linkedAssetId === null && ts.inferredTag === null) {
      const asset = dataset.assets.find((item) => item.id === 'asset-001');
      if (asset !== undefined && asset.tier === 1) {
        issues.push({
          id: `issue-ts-tier1-gap-${asset.id}`,
          kind: 'tier1-ts-gap',
          dimensionId: 'time-series',
          metricId: 'tier1CoverageGaps',
          entityId: asset.id,
          entityLabel: asset.externalId,
          tier: asset.tier,
          facilityId: asset.facilityId,
          divisionId: asset.divisionId,
          disciplineId: asset.disciplineId,
          ownerId: asset.ownerId,
          detail: 'Critical asset has zero linked time series.',
        });
      }
    }
    if (ts.inferredTag !== null && ts.linkedAssetId === null) {
      const hierarchyMatch = dataset.assets.some(
        (asset) => asset.externalId === ts.inferredTag
      );
      if (!hierarchyMatch) {
        issues.push({
          id: `issue-ts-inferred-${ts.id}`,
          kind: 'inferred-tag-gap',
          dimensionId: 'time-series',
          metricId: 'inferredTagsWithoutMatch',
          entityId: ts.id,
          entityLabel: ts.externalId,
          tier: ts.tier,
          facilityId: ts.facilityId,
          divisionId: ts.divisionId,
          disciplineId: ts.disciplineId,
          ownerId: ts.ownerId,
          detail: `Inferred tag ${ts.inferredTag} has no hierarchy match.`,
        });
      }
    }
  }

  for (const record of dataset.maintenance) {
    if (record.linkedAssetId === null) {
      issues.push({
        id: `issue-maint-unlinked-${record.id}`,
        kind: 'unlinked-maintenance',
        dimensionId: 'maintenance',
        metricId: 'unlinkedMaintenance',
        entityId: record.id,
        entityLabel: record.externalId,
        tier: record.tier,
        facilityId: record.facilityId,
        divisionId: record.divisionId,
        disciplineId: record.disciplineId,
        ownerId: record.ownerId,
        detail: 'Maintenance record missing asset link.',
      });
    }
    if (record.kind === 'work-order' && record.linkedNotificationId === null && record.tier <= 2) {
      issues.push({
        id: `issue-wo-nonotif-${record.id}`,
        kind: 'wo-without-notification',
        dimensionId: 'maintenance',
        metricId: 'woWithoutNotification',
        entityId: record.id,
        entityLabel: record.externalId,
        tier: record.tier,
        facilityId: record.facilityId,
        divisionId: record.divisionId,
        disciplineId: record.disciplineId,
        ownerId: record.ownerId,
        detail: 'Work order has no linked notification.',
      });
    }
    if (record.tier === 1 && record.linkedAssetId === null) {
      issues.push({
        id: `issue-maint-tier1-${record.id}`,
        kind: 'tier1-maintenance-gap',
        dimensionId: 'maintenance',
        metricId: 'tier1MaintenanceGaps',
        entityId: record.id,
        entityLabel: record.externalId,
        tier: record.tier,
        facilityId: record.facilityId,
        divisionId: record.divisionId,
        disciplineId: record.disciplineId,
        ownerId: record.ownerId,
        detail: 'Tier 1 maintenance record missing asset coverage.',
      });
    }
  }

  for (const ann of dataset.fileAnnotations) {
    if (ann.confidence < 0.5) {
      issues.push({
        id: `issue-ann-lowconf-${ann.id}`,
        kind: 'low-confidence-annotation',
        dimensionId: 'file-annotation',
        metricId: 'lowConfidenceQueue',
        entityId: ann.id,
        entityLabel: ann.fileName,
        tier: ann.tier,
        facilityId: ann.facilityId,
        divisionId: ann.divisionId,
        disciplineId: ann.disciplineId,
        ownerId: ann.ownerId,
        detail: `Confidence ${Math.round(ann.confidence * 100)}%.`,
      });
    }
    if (ann.status === 'pending-review' || ann.status === 'suggested') {
      issues.push({
        id: `issue-ann-pending-${ann.id}`,
        kind: 'pending-review',
        dimensionId: 'file-annotation',
        metricId: 'pendingReview',
        entityId: ann.id,
        entityLabel: ann.fileName,
        tier: ann.tier,
        facilityId: ann.facilityId,
        divisionId: ann.divisionId,
        disciplineId: ann.disciplineId,
        ownerId: ann.ownerId,
        detail: `Pending review for ${ann.ageDays} days.`,
      });
    }
    if (
      config.fileAnnotationAcceleratorAvailable &&
      ann.patternDetectedTag !== null &&
      ann.linkedAssetId === null
    ) {
      issues.push({
        id: `issue-ann-pattern-${ann.id}`,
        kind: 'pattern-annotation-gap',
        dimensionId: 'file-annotation',
        metricId: 'patternVsAnnotation',
        entityId: ann.id,
        entityLabel: ann.fileName,
        tier: ann.tier,
        facilityId: ann.facilityId,
        divisionId: ann.divisionId,
        disciplineId: ann.disciplineId,
        ownerId: ann.ownerId,
        detail: `Pattern detected ${ann.patternDetectedTag} without annotation link.`,
      });
    }
  }

  for (const obj of dataset.model3d) {
    if (obj.tier === 1 && obj.linkedAssetId === null) {
      issues.push({
        id: `issue-3d-unlinked-${obj.id}`,
        kind: 'unlinked-3d',
        dimensionId: 'model-3d',
        metricId: 'unlinkedTier1',
        entityId: obj.id,
        entityLabel: obj.externalId,
        tier: obj.tier,
        facilityId: obj.facilityId,
        divisionId: obj.divisionId,
        disciplineId: obj.disciplineId,
        ownerId: obj.ownerId,
        detail: 'Tier 1 asset missing 3D object.',
      });
    }
    if (!obj.bboxComplete) {
      issues.push({
        id: `issue-3d-bbox-${obj.id}`,
        kind: 'bbox-gap',
        dimensionId: 'model-3d',
        metricId: 'bboxGaps',
        entityId: obj.id,
        entityLabel: obj.externalId,
        tier: obj.tier,
        facilityId: obj.facilityId,
        divisionId: obj.divisionId,
        disciplineId: obj.disciplineId,
        ownerId: obj.ownerId,
        detail: 'Bounding box incomplete.',
      });
    }
  }

  for (const file of dataset.files) {
    if (!isEligibleFile(file, config)) {
      continue;
    }
    if (file.linkedAssetId === null) {
      issues.push({
        id: `issue-file-unlinked-${file.id}`,
        kind: 'unlinked-file',
        dimensionId: 'files',
        metricId: 'unlinkedByCategory',
        entityId: file.id,
        entityLabel: file.name,
        tier: file.tier,
        facilityId: file.facilityId,
        divisionId: file.divisionId,
        disciplineId: file.disciplineId,
        ownerId: file.ownerId,
        detail: `Unlinked file in category ${file.category ?? 'unknown'}.`,
      });
    }
    if (file.category === null) {
      issues.push({
        id: `issue-file-meta-${file.id}`,
        kind: 'missing-metadata',
        dimensionId: 'files',
        metricId: 'missingMetadata',
        entityId: file.id,
        entityLabel: file.name,
        tier: file.tier,
        facilityId: file.facilityId,
        divisionId: file.divisionId,
        disciplineId: file.disciplineId,
        ownerId: file.ownerId,
        detail: 'Missing file category metadata.',
      });
    }
    if (!file.isUploaded) {
      issues.push({
        id: `issue-file-upload-${file.id}`,
        kind: 'upload-gap',
        dimensionId: 'files',
        metricId: 'uploadStatusGaps',
        entityId: file.id,
        entityLabel: file.name,
        tier: file.tier,
        facilityId: file.facilityId,
        divisionId: file.divisionId,
        disciplineId: file.disciplineId,
        ownerId: file.ownerId,
        detail: 'File metadata exists without uploaded content.',
      });
    }
  }

  if (config.lifecycleMetadataAvailable) {
    issues.push({
      id: 'issue-lifecycle-stale-ann',
      kind: 'stale-annotation-link',
      dimensionId: 'file-annotation',
      metricId: 'staleAnnotationLinkages',
      entityId: 'ann-001',
      entityLabel: 'P&ID-1104.pdf',
      tier: 2,
      facilityId: 'demo-north',
      divisionId: 'prod-a',
      disciplineId: 'mechanical',
      ownerId: 'pid-review-lead',
      detail: 'Annotation references retired asset.',
    });
  }

  return issues;
}

export function filterIssues(
  issues: readonly QualityIssue[],
  filters: SliceFilters,
  dimensionId?: DimensionId,
  metricId?: MetricId
): QualityIssue[] {
  return issues.filter((issue) => {
    if (dimensionId !== undefined && issue.dimensionId !== dimensionId) {
      return false;
    }
    if (metricId !== undefined && issue.metricId !== metricId) {
      return false;
    }
    return matchesSlice(issue, filters);
  });
}

export function computeTierScores(
  dataset: MockDataset,
  config: AppConfigDto,
  dimensionId: DimensionId,
  filters: SliceFilters
): {
  tierScores: { tier: Tier; scorePercent: number | null; targetPercent: number; scored: boolean }[];
  overallPercent: number | null;
} {
  const tierScores = ([1, 2, 3] as Tier[]).map((tier) => {
    const scorePercent = computeDimensionTierScore(dataset, config, dimensionId, filters, tier);
    const targetPercent = tier === 1 ? 95 : tier === 2 ? 60 : 0;
    const scored = tier !== 3 && scorePercent !== null;
    return {
      tier,
      scorePercent,
      targetPercent,
      scored,
    };
  });

  const scored = tierScores.filter((entry) => entry.scored && entry.scorePercent !== null);
  const overallPercent =
    scored.length === 0
      ? null
      : Math.round(
          scored.reduce((sum, entry) => sum + (entry.scorePercent ?? 0), 0) / scored.length
        );

  return { tierScores, overallPercent };
}

function computeDimensionTierScore(
  dataset: MockDataset,
  config: AppConfigDto,
  dimensionId: DimensionId,
  filters: SliceFilters,
  tier: Tier
): number | null {
  switch (dimensionId) {
    case 'asset-hierarchy': {
      const assets = sliceFilter(dataset.assets, filters).filter((asset) => asset.tier === tier);
      if (assets.length === 0) {
        return null;
      }
      const linked = assets.filter((asset) => !asset.isOrphan).length;
      return percent(linked, assets.length);
    }
    case 'equipment-asset': {
      const equipment = sliceFilter(dataset.equipment, filters)
        .filter((eq) => eq.tier === tier)
        .filter((eq) => isEligibleEquipment(eq, config));
      if (equipment.length === 0) {
        return null;
      }
      const linked = equipment.filter((eq) => eq.linkedAssetId !== null).length;
      return percent(linked, equipment.length);
    }
    case 'time-series': {
      const series = sliceFilter(dataset.timeSeries, filters)
        .filter((ts) => ts.tier === tier)
        .filter((ts) => isEligibleTimeSeries(ts, config));
      if (series.length === 0) {
        return null;
      }
      const linked = series.filter((ts) => ts.linkedAssetId !== null).length;
      return percent(linked, series.length);
    }
    case 'maintenance': {
      const records = sliceFilter(dataset.maintenance, filters).filter((record) => record.tier === tier);
      if (records.length === 0) {
        return null;
      }
      const linked = records.filter((record) => record.linkedAssetId !== null).length;
      return percent(linked, records.length);
    }
    case 'file-annotation': {
      const annotations = sliceFilter(dataset.fileAnnotations, filters).filter((ann) => ann.tier === tier);
      if (annotations.length === 0) {
        return null;
      }
      const approved = annotations.filter((ann) => ann.status === 'approved').length;
      return percent(approved, annotations.length);
    }
    case 'model-3d': {
      const objects = sliceFilter(dataset.model3d, filters).filter((obj) => obj.tier === tier);
      if (objects.length === 0) {
        return null;
      }
      const linked = objects.filter((obj) => obj.linkedAssetId !== null).length;
      return percent(linked, objects.length);
    }
    case 'files': {
      const files = sliceFilter(dataset.files, filters)
        .filter((file) => file.tier === tier)
        .filter((file) => isEligibleFile(file, config));
      if (files.length === 0) {
        return null;
      }
      const linked = files.filter((file) => file.linkedAssetId !== null).length;
      return percent(linked, files.length);
    }
    default:
      return null;
  }
}

export function isDimensionEnabled(config: AppConfigDto, dimensionId: DimensionId): boolean {
  if (dimensionId === 'maintenance') {
    return config.maintenanceDimensionEnabled && config.maintenanceViewsAvailable;
  }
  if (dimensionId === 'file-annotation') {
    return config.fileAnnotationDimensionEnabled;
  }
  if (dimensionId === 'model-3d') {
    return config.model3dAvailable;
  }
  return true;
}

export function getEnabledDimensions(config: AppConfigDto): DimensionId[] {
  const all: DimensionId[] = [
    'asset-hierarchy',
    'equipment-asset',
    'time-series',
    'maintenance',
    'file-annotation',
    'model-3d',
    'files',
  ];
  return all.filter((dimensionId) => isDimensionEnabled(config, dimensionId));
}

export { tierFilter, sliceFilter };
