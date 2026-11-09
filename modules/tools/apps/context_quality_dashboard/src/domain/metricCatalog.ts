import type {
  AppConfigDto,
  DimensionId,
  DrilldownDto,
  MetricId,
  QualityIssue,
} from '../types';

const METRIC_ACTION_KEYS: Record<MetricId, string> = {
  hierarchyCompletionRate: 'action.reviewHierarchy',
  orphanRate: 'action.exportOrphans',
  orphansByTier: 'action.exportOrphans',
  depthOutliers: 'action.reviewDepth',
  typeCoverageGaps: 'action.classifyAssets',
  associationRate: 'action.reviewEquipmentLinks',
  serialMfrCompleteness: 'action.completeSerials',
  unmatchedWithCandidates: 'action.confirmCandidateLinks',
  completenessByManufacturer: 'action.escalateManufacturer',
  tsToAssetRate: 'action.mapTimeSeries',
  monitoringCoverage: 'action.reviewMonitoring',
  freshness: 'action.reviewFreshness',
  orphansByPrefix: 'action.bulkMapPrefix',
  staleTimeSeries: 'action.checkSensors',
  tier1CoverageGaps: 'action.flagInstrumentation',
  inferredTagsWithoutMatch: 'action.escalateMasterData',
  woToAssetRate: 'action.linkWorkOrders',
  woToNotificationRate: 'action.linkNotifications',
  notificationToAssetRate: 'action.linkNotifications',
  woCompletionRate: 'action.reviewCompletion',
  notificationToWoRate: 'action.reviewNotificationFlow',
  assetMaintenanceCoverage: 'action.reviewMaintenanceCoverage',
  notificationToEquipmentRate: 'action.linkEquipmentNotifications',
  woToEquipmentRate: 'action.linkEquipmentWorkOrders',
  failureDocumentationRate: 'action.reviewFailureDocs',
  equipmentMaintenanceCoverage: 'action.reviewEquipmentMaintenance',
  woWithoutNotification: 'action.reviewWorkOrders',
  unlinkedMaintenance: 'action.linkMaintenance',
  notificationsWithoutWorkOrder: 'action.reviewNotificationFlow',
  tier1MaintenanceGaps: 'action.reviewTier1Maintenance',
  annotationCount: 'action.reviewAnnotations',
  confidenceAverage: 'action.reviewConfidence',
  approvalMix: 'action.reviewApprovals',
  lowConfidenceQueue: 'action.routeLowConfidence',
  unprocessedBacklog: 'action.processBacklog',
  rejectionByType: 'action.reviewRejections',
  pendingReview: 'action.assignReviewer',
  unapprovedAnnotations: 'action.assignReviewer',
  patternVsAnnotation: 'action.reconcilePatternTags',
  threeDToAssetRate: 'action.link3dObjects',
  asset3dCoverage: 'action.review3dCoverage',
  bboxCompleteness: 'action.completeBbox',
  unlinkedTier1: 'action.upload3dModels',
  unlinked3dObjects: 'action.link3dObjects',
  bboxGaps: 'action.rerunSpatialExtraction',
  equipmentMissingSerial: 'action.completeSerials',
  equipmentMissingManufacturer: 'action.completeSerials',
  filesMissingCategory: 'action.completeMetadata',
  filesMissingDescription: 'action.completeMetadata',
  filesMissingSourceId: 'action.completeMetadata',
  incompleteWorkOrders: 'action.reviewCompletion',
  modelTypeCoverageByTier: 'action.reviewModelTypes',
  fileToAssetRate: 'action.linkFiles',
  metadataCompleteness: 'action.completeMetadata',
  unlinkedByCategory: 'action.matchFilesByCategory',
  missingMetadata: 'action.setCategories',
  uploadStatusGaps: 'action.retryUploads',
  staleAnnotationLinkages: 'action.refreshAnnotations',
  uncontextualizedRevisions: 'action.propagateAnnotations',
  activeChildrenUnderRetiredParents: 'action.reparentAssets',
  brokenCrossViewLinks: 'action.repairCrossViewLinks',
};

const METRIC_DIMENSION: Record<MetricId, DimensionId> = {
  hierarchyCompletionRate: 'asset-hierarchy',
  orphanRate: 'asset-hierarchy',
  orphansByTier: 'asset-hierarchy',
  depthOutliers: 'asset-hierarchy',
  typeCoverageGaps: 'asset-hierarchy',
  associationRate: 'equipment-asset',
  serialMfrCompleteness: 'equipment-asset',
  unmatchedWithCandidates: 'equipment-asset',
  completenessByManufacturer: 'equipment-asset',
  tsToAssetRate: 'time-series',
  monitoringCoverage: 'time-series',
  freshness: 'time-series',
  orphansByPrefix: 'time-series',
  staleTimeSeries: 'time-series',
  tier1CoverageGaps: 'time-series',
  inferredTagsWithoutMatch: 'time-series',
  woToAssetRate: 'maintenance',
  woToNotificationRate: 'maintenance',
  notificationToAssetRate: 'maintenance',
  woCompletionRate: 'maintenance',
  notificationToWoRate: 'maintenance',
  assetMaintenanceCoverage: 'maintenance',
  notificationToEquipmentRate: 'maintenance',
  woToEquipmentRate: 'maintenance',
  failureDocumentationRate: 'maintenance',
  equipmentMaintenanceCoverage: 'maintenance',
  woWithoutNotification: 'maintenance',
  unlinkedMaintenance: 'maintenance',
  notificationsWithoutWorkOrder: 'maintenance',
  tier1MaintenanceGaps: 'maintenance',
  annotationCount: 'file-annotation',
  confidenceAverage: 'file-annotation',
  approvalMix: 'file-annotation',
  lowConfidenceQueue: 'file-annotation',
  unprocessedBacklog: 'file-annotation',
  rejectionByType: 'file-annotation',
  pendingReview: 'file-annotation',
  unapprovedAnnotations: 'file-annotation',
  patternVsAnnotation: 'file-annotation',
  threeDToAssetRate: 'model-3d',
  asset3dCoverage: 'model-3d',
  bboxCompleteness: 'model-3d',
  unlinkedTier1: 'model-3d',
  unlinked3dObjects: 'model-3d',
  bboxGaps: 'model-3d',
  equipmentMissingSerial: 'equipment-asset',
  equipmentMissingManufacturer: 'equipment-asset',
  filesMissingCategory: 'files',
  filesMissingDescription: 'files',
  filesMissingSourceId: 'files',
  incompleteWorkOrders: 'maintenance',
  modelTypeCoverageByTier: 'model-3d',
  fileToAssetRate: 'files',
  metadataCompleteness: 'files',
  unlinkedByCategory: 'files',
  missingMetadata: 'files',
  uploadStatusGaps: 'files',
  staleAnnotationLinkages: 'file-annotation',
  uncontextualizedRevisions: 'files',
  activeChildrenUnderRetiredParents: 'asset-hierarchy',
  brokenCrossViewLinks: 'equipment-asset',
};

export function isMetricId(value: string): value is MetricId {
  return Object.prototype.hasOwnProperty.call(METRIC_DIMENSION, value);
}

export function getMetricDimension(metricId: MetricId): DimensionId {
  return METRIC_DIMENSION[metricId];
}

export function getMetricActionKey(metricId: MetricId): string {
  return METRIC_ACTION_KEYS[metricId];
}

export const DIMENSION_PRIMARY_ACTION_KEYS: Record<DimensionId, string> = {
  'asset-hierarchy': METRIC_ACTION_KEYS.orphansByTier,
  'equipment-asset': METRIC_ACTION_KEYS.unmatchedWithCandidates,
  'time-series': METRIC_ACTION_KEYS.inferredTagsWithoutMatch,
  maintenance: METRIC_ACTION_KEYS.unlinkedMaintenance,
  'file-annotation': METRIC_ACTION_KEYS.pendingReview,
  'model-3d': METRIC_ACTION_KEYS.unlinkedTier1,
  files: METRIC_ACTION_KEYS.unlinkedByCategory,
};

export function getDimensionPrimaryActionKey(dimensionId: DimensionId): string {
  return DIMENSION_PRIMARY_ACTION_KEYS[dimensionId];
}

export const DRILLDOWN_METRIC_IDS: readonly MetricId[] = [
  'orphansByTier',
  'depthOutliers',
  'typeCoverageGaps',
  'unmatchedWithCandidates',
  'completenessByManufacturer',
  'orphansByPrefix',
  'staleTimeSeries',
  'tier1CoverageGaps',
  'inferredTagsWithoutMatch',
  'woWithoutNotification',
  'unlinkedMaintenance',
  'notificationsWithoutWorkOrder',
  'tier1MaintenanceGaps',
  'lowConfidenceQueue',
  'pendingReview',
  'unapprovedAnnotations',
  'patternVsAnnotation',
  'unlinkedTier1',
  'unlinked3dObjects',
  'bboxGaps',
  'unlinkedByCategory',
  'equipmentMissingSerial',
  'equipmentMissingManufacturer',
  'filesMissingCategory',
  'filesMissingDescription',
  'filesMissingSourceId',
  'incompleteWorkOrders',
  'missingMetadata',
  'uploadStatusGaps',
  'activeChildrenUnderRetiredParents',
  'staleAnnotationLinkages',
  'unprocessedBacklog',
  'rejectionByType',
  'brokenCrossViewLinks',
  'uncontextualizedRevisions',
];

export const LIFECYCLE_METRIC_IDS: readonly MetricId[] = [
  'staleAnnotationLinkages',
  'uncontextualizedRevisions',
  'activeChildrenUnderRetiredParents',
  'brokenCrossViewLinks',
];

/** The drill-down metric that lists the entities a link metric failed on. */
export const UNLINKED_DRILLDOWN_BY_METRIC: Readonly<Partial<Record<MetricId, MetricId>>> = {
  orphanRate: 'orphansByTier',
  associationRate: 'unmatchedWithCandidates',
  tsToAssetRate: 'orphansByPrefix',
  freshness: 'staleTimeSeries',
  monitoringCoverage: 'tier1CoverageGaps',
  woToAssetRate: 'unlinkedMaintenance',
  notificationToAssetRate: 'unlinkedMaintenance',
  woToNotificationRate: 'woWithoutNotification',
  notificationToWoRate: 'notificationsWithoutWorkOrder',
  fileToAssetRate: 'unlinkedByCategory',
  threeDToAssetRate: 'unlinked3dObjects',
  asset3dCoverage: 'unlinkedTier1',
  bboxCompleteness: 'bboxGaps',
  woCompletionRate: 'incompleteWorkOrders',
  approvalMix: 'unapprovedAnnotations',
};

/** The metric whose rows list the entities behind `metricId`: itself for a drill-down metric, null when there is none. */
/** A metric measured once per field lists the entities missing that field under its own drill-down metric. */
const DRILLDOWN_BY_FIELD: Readonly<Record<string, MetricId>> = {
  'serialMfrCompleteness/serialNumber': 'equipmentMissingSerial',
  'serialMfrCompleteness/manufacturer': 'equipmentMissingManufacturer',
  'metadataCompleteness/category': 'filesMissingCategory',
  'metadataCompleteness/description': 'filesMissingDescription',
  'metadataCompleteness/sourceId': 'filesMissingSourceId',
};

export function drilldownMetricForField(metricId: MetricId, subKey: string): MetricId | null {
  return DRILLDOWN_BY_FIELD[`${metricId}/${subKey}`] ?? null;
}

export function drilldownMetricFor(metricId: MetricId): MetricId | null {
  if (DRILLDOWN_METRIC_IDS.includes(metricId)) {
    return metricId;
  }
  return UNLINKED_DRILLDOWN_BY_METRIC[metricId] ?? null;
}

export function isDrilldownMetric(metricId: MetricId): boolean {
  return DRILLDOWN_METRIC_IDS.includes(metricId);
}

/** Whether opening the metric lists entities: it is a drill-down metric, or a link metric with one. */
export function hasDrilldownList(metricId: MetricId): boolean {
  return drilldownMetricFor(metricId) !== null;
}

export function isLifecycleMetric(metricId: MetricId): boolean {
  return LIFECYCLE_METRIC_IDS.includes(metricId);
}

export function computeMetricStatus(
  direction: 'critical' | 'informational',
  valuePercent: number | null,
  issueCount: number
): 'good' | 'weak' | 'critical' {
  if (issueCount === 0) {
    return 'good';
  }
  if (direction === 'critical' && valuePercent !== null && valuePercent < 60) {
    return 'critical';
  }
  return 'weak';
}

export function buildDrilldownDto(metricId: MetricId, issues: readonly QualityIssue[]): DrilldownDto {
  const dimensionId = getMetricDimension(metricId);
  const actionKey = getMetricActionKey(metricId);
  const ownerId = issues[0]?.ownerId ?? null;

  return {
    metricId,
    dimensionId,
    labelKey: `metric.${metricId}`,
    actionKey,
    ownerId,
    columnKeys: ['entity', 'tier', 'detail', 'owner'],
    partial: null,
    rows: issues.map((issue) => ({
      id: issue.id,
      tier: issue.tier,
      ownerId: issue.ownerId,
      classes: {},
      cells: {
        entity: issue.entityLabel,
        tier: String(issue.tier),
        detail: issue.detail,
        owner: issue.ownerId,
      },
    })),
  };
}

export function isExtendedMetric(metricId: MetricId): boolean {
  const extendedMaintenance: MetricId[] = [
    'notificationToEquipmentRate',
    'woToEquipmentRate',
    'failureDocumentationRate',
    'equipmentMaintenanceCoverage',
  ];
  if (extendedMaintenance.includes(metricId)) {
    return true;
  }
  if (metricId === 'patternVsAnnotation') {
    return true;
  }
  const lifecycleMetrics: MetricId[] = [
    'staleAnnotationLinkages',
    'uncontextualizedRevisions',
    'activeChildrenUnderRetiredParents',
    'brokenCrossViewLinks',
  ];
  return lifecycleMetrics.includes(metricId);
}

export function shouldShowMetric(metricId: MetricId, config: AppConfigDto): boolean {
  if (isExtendedMetric(metricId)) {
    if (
      metricId === 'failureDocumentationRate' ||
      metricId === 'notificationToEquipmentRate' ||
      metricId === 'woToEquipmentRate' ||
      metricId === 'equipmentMaintenanceCoverage'
    ) {
      return config.failureTaxonomyAvailable;
    }
    if (metricId === 'patternVsAnnotation') {
      return config.fileAnnotationAcceleratorAvailable;
    }
    return config.lifecycleMetadataAvailable;
  }
  return true;
}
