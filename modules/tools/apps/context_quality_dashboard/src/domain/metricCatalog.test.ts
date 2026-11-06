import { describe, expect, it } from 'vitest';

import {
  DRILLDOWN_METRIC_IDS,
  UNLINKED_DRILLDOWN_BY_METRIC,
  drilldownMetricFor,
  getMetricDimension,
  hasDrilldownList,
} from './metricCatalog';

describe('listing the unlinked items of a link metric', () => {
  it.each([
    ['tsToAssetRate', 'orphansByPrefix'],
    ['associationRate', 'unmatchedWithCandidates'],
    ['woToAssetRate', 'unlinkedMaintenance'],
    ['notificationToAssetRate', 'unlinkedMaintenance'],
    ['woToNotificationRate', 'woWithoutNotification'],
    ['notificationToWoRate', 'notificationsWithoutWorkOrder'],
    ['fileToAssetRate', 'unlinkedByCategory'],
    ['woCompletionRate', 'incompleteWorkOrders'],
    ['approvalMix', 'unapprovedAnnotations'],
    ['threeDToAssetRate', 'unlinked3dObjects'],
    ['orphanRate', 'orphansByTier'],
  ] as const)('opens %s as %s', (metricId, drilldown) => {
    expect(drilldownMetricFor(metricId)).toBe(drilldown);
    expect(hasDrilldownList(metricId)).toBe(true);
  });

  it('opens a drill-down metric as itself', () => {
    expect(drilldownMetricFor('orphansByPrefix')).toBe('orphansByPrefix');
  });

  it('has nothing to list for a metric that does not count links', () => {
    expect(drilldownMetricFor('hierarchyCompletionRate')).toBeNull();
    expect(hasDrilldownList('hierarchyCompletionRate')).toBe(false);
  });

  it('only lists failures from drill-down metrics of the same dimension', () => {
    for (const [metricId, drilldown] of Object.entries(UNLINKED_DRILLDOWN_BY_METRIC)) {
      expect(DRILLDOWN_METRIC_IDS).toContain(drilldown);
      expect(getMetricDimension(drilldown)).toBe(
        getMetricDimension(metricId as keyof typeof UNLINKED_DRILLDOWN_BY_METRIC)
      );
    }
  });
});
