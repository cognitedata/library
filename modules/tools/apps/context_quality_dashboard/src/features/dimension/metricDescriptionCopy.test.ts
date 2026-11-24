import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';
import type { MetricId } from '../../types';

import { metricDescriptionKey } from './dimensionMetricPresentation';

const METRIC_IDS: MetricId[] = [
  'hierarchyCompletionRate',
  'orphanRate',
  'orphansByTier',
  'depthOutliers',
  'typeCoverageGaps',
  'associationRate',
  'serialMfrCompleteness',
  'unmatchedWithCandidates',
  'completenessByManufacturer',
  'tsToAssetRate',
  'monitoringCoverage',
  'freshness',
  'orphansByPrefix',
  'staleTimeSeries',
  'tier1CoverageGaps',
  'inferredTagsWithoutMatch',
  'woToAssetRate',
  'woToNotificationRate',
  'notificationToAssetRate',
  'woCompletionRate',
  'notificationToWoRate',
  'assetMaintenanceCoverage',
  'notificationToEquipmentRate',
  'woToEquipmentRate',
  'failureDocumentationRate',
  'equipmentMaintenanceCoverage',
  'woWithoutNotification',
  'unlinkedMaintenance',
  'notificationsWithoutWorkOrder',
  'tier1MaintenanceGaps',
  'annotationCount',
  'confidenceAverage',
  'approvalMix',
  'lowConfidenceQueue',
  'unprocessedBacklog',
  'rejectionByType',
  'pendingReview',
  'unapprovedAnnotations',
  'patternVsAnnotation',
  'threeDToAssetRate',
  'asset3dCoverage',
  'bboxCompleteness',
  'unlinkedTier1',
  'unlinked3dObjects',
  'bboxGaps',
  'equipmentMissingSerial',
  'equipmentMissingManufacturer',
  'filesMissingCategory',
  'filesMissingDescription',
  'filesMissingSourceId',
  'incompleteWorkOrders',
  'modelTypeCoverageByTier',
  'fileToAssetRate',
  'metadataCompleteness',
  'unlinkedByCategory',
  'missingMetadata',
  'uploadStatusGaps',
  'staleAnnotationLinkages',
  'uncontextualizedRevisions',
  'activeChildrenUnderRetiredParents',
  'brokenCrossViewLinks',
];

describe('dimension metric description copy', () => {
  it.each(METRIC_IDS)('should define English tooltip copy for %s', (metricId) => {
    const key = metricDescriptionKey(metricId);
    expect(key).toBe(`dimension.metric.description.${metricId}`);
    const text = translate(key, 'en');
    expect(text).not.toBe(key);
    expect(text.length).toBeGreaterThan(10);
  });
});
