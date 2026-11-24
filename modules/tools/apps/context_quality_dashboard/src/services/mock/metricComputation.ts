import { computeValuePercent } from '../../domain/metricMath';
import type { MockDataset } from '../../mock/dataset';
import type {
  AppConfigDto,
  MetricId,
  QualityIssue,
  SliceFilters,
} from '../../types';


import { filterIssues, sliceFilter } from './aggregation';
import {
  isEligibleEquipment,
  isEligibleFile,
  isEligibleTimeSeries,
} from './exclusionLogic';
import { getMetricDimension } from './metricCatalog';

export type MetricCounts = {
  eligibleCount: number;
  passingCount: number;
  issueCount: number;
  valuePercent: number | null;
};

export { computeValuePercent };

type PoolContext = {
  dataset: MockDataset;
  config: AppConfigDto;
  filters: SliceFilters;
};

type PoolResult = {
  eligibleCount: number;
  passingCount: number;
};

function defaultPoolForDimension(
  dimensionId: ReturnType<typeof getMetricDimension>,
  { dataset, config, filters }: PoolContext
): PoolResult {
  switch (dimensionId) {
    case 'asset-hierarchy': {
      const assets = sliceFilter(dataset.assets, filters);
      const passing = assets.filter((asset) => !asset.isOrphan).length;
      return { eligibleCount: assets.length, passingCount: passing };
    }
    case 'equipment-asset': {
      const equipment = sliceFilter(dataset.equipment, filters).filter((eq) =>
        isEligibleEquipment(eq, config)
      );
      const passing = equipment.filter((eq) => eq.linkedAssetId !== null).length;
      return { eligibleCount: equipment.length, passingCount: passing };
    }
    case 'time-series': {
      const series = sliceFilter(dataset.timeSeries, filters).filter((ts) =>
        isEligibleTimeSeries(ts, config)
      );
      const passing = series.filter((ts) => ts.linkedAssetId !== null).length;
      return { eligibleCount: series.length, passingCount: passing };
    }
    case 'maintenance': {
      const records = sliceFilter(dataset.maintenance, filters);
      const passing = records.filter((record) => record.linkedAssetId !== null).length;
      return { eligibleCount: records.length, passingCount: passing };
    }
    case 'file-annotation': {
      const annotations = sliceFilter(dataset.fileAnnotations, filters);
      const passing = annotations.filter((ann) => ann.status === 'approved').length;
      return { eligibleCount: annotations.length, passingCount: passing };
    }
    case 'model-3d': {
      const objects = sliceFilter(dataset.model3d, filters);
      const passing = objects.filter((obj) => obj.linkedAssetId !== null).length;
      return { eligibleCount: objects.length, passingCount: passing };
    }
    case 'files': {
      const files = sliceFilter(dataset.files, filters).filter((file) =>
        isEligibleFile(file, config)
      );
      const passing = files.filter((file) => file.linkedAssetId !== null).length;
      return { eligibleCount: files.length, passingCount: passing };
    }
    default:
      return { eligibleCount: 0, passingCount: 0 };
  }
}

const RATE_METRIC_IDS: ReadonlySet<MetricId> = new Set([
  'hierarchyCompletionRate',
  'orphanRate',
  'associationRate',
  'serialMfrCompleteness',
  'tsToAssetRate',
  'monitoringCoverage',
  'freshness',
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
  'annotationCount',
  'confidenceAverage',
  'approvalMix',
  'threeDToAssetRate',
  'asset3dCoverage',
  'bboxCompleteness',
  'fileToAssetRate',
  'metadataCompleteness',
]);

function computeRateMetricPool(metricId: MetricId, context: PoolContext): PoolResult {
  const { dataset, config, filters } = context;

  switch (metricId) {
    case 'serialMfrCompleteness': {
      const equipment = sliceFilter(dataset.equipment, filters).filter((eq) =>
        isEligibleEquipment(eq, config)
      );
      const emerson = equipment.filter((eq) => eq.manufacturer === 'Emerson');
      const passing = emerson.filter((eq) => eq.serialNumber !== null).length;
      return { eligibleCount: emerson.length, passingCount: passing };
    }
    case 'woToNotificationRate': {
      const workOrders = sliceFilter(dataset.maintenance, filters).filter(
        (record) => record.kind === 'work-order'
      );
      const passing = workOrders.filter((record) => record.linkedNotificationId !== null).length;
      return { eligibleCount: workOrders.length, passingCount: passing };
    }
    case 'notificationToWoRate': {
      const notifications = sliceFilter(dataset.maintenance, filters).filter(
        (record) => record.kind === 'notification'
      );
      const workOrders = sliceFilter(dataset.maintenance, filters).filter(
        (record) => record.kind === 'work-order'
      );
      const passing = notifications.filter((notification) =>
        workOrders.some((workOrder) => workOrder.linkedNotificationId === notification.id)
      ).length;
      return { eligibleCount: notifications.length, passingCount: passing };
    }
    case 'woCompletionRate': {
      const workOrders = sliceFilter(dataset.maintenance, filters).filter(
        (record) => record.kind === 'work-order'
      );
      const passing = workOrders.filter((record) => record.completed).length;
      return { eligibleCount: workOrders.length, passingCount: passing };
    }
    case 'confidenceAverage': {
      const annotations = sliceFilter(dataset.fileAnnotations, filters);
      const passing = annotations.filter((ann) => ann.confidence >= 0.5).length;
      return { eligibleCount: annotations.length, passingCount: passing };
    }
    case 'bboxCompleteness': {
      const objects = sliceFilter(dataset.model3d, filters);
      const passing = objects.filter((obj) => obj.bboxComplete).length;
      return { eligibleCount: objects.length, passingCount: passing };
    }
    case 'metadataCompleteness': {
      const files = sliceFilter(dataset.files, filters).filter((file) =>
        isEligibleFile(file, config)
      );
      const passing = files.filter((file) => file.category !== null).length;
      return { eligibleCount: files.length, passingCount: passing };
    }
    case 'freshness': {
      const series = sliceFilter(dataset.timeSeries, filters).filter((ts) =>
        isEligibleTimeSeries(ts, config)
      );
      const referenceMs = Date.parse('2026-09-01T00:00:00.000Z');
      const passing = series.filter((ts) => {
        const daysSinceUpdate = Math.floor(
          (referenceMs - Date.parse(ts.lastUpdatedAt)) / 86_400_000
        );
        return daysSinceUpdate <= 30;
      }).length;
      return { eligibleCount: series.length, passingCount: passing };
    }
    default:
      return defaultPoolForDimension(getMetricDimension(metricId), context);
  }
}

export function computeMetricCounts(
  metricId: MetricId,
  dataset: MockDataset,
  config: AppConfigDto,
  filters: SliceFilters,
  issues: readonly QualityIssue[]
): MetricCounts {
  const dimensionId = getMetricDimension(metricId);
  const issueCount = filterIssues(issues, filters, dimensionId, metricId).length;
  const context: PoolContext = { dataset, config, filters };

  if (RATE_METRIC_IDS.has(metricId)) {
    const { eligibleCount, passingCount } = computeRateMetricPool(metricId, context);
    return {
      eligibleCount,
      passingCount,
      issueCount,
      valuePercent: computeValuePercent(passingCount, eligibleCount),
    };
  }

  const { eligibleCount } = defaultPoolForDimension(dimensionId, context);
  if (eligibleCount === 0) {
    return {
      eligibleCount: 0,
      passingCount: 0,
      issueCount,
      valuePercent: null,
    };
  }

  const passingCount = Math.max(0, eligibleCount - issueCount);
  return {
    eligibleCount,
    passingCount,
    issueCount,
    valuePercent: computeValuePercent(passingCount, eligibleCount),
  };
}
