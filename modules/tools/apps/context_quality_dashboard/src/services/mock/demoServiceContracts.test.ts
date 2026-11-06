import { describe, expect, it } from 'vitest';

import { FACILITIES } from '../../mock/catalog';

import { createMockServices } from './createMockServices';

describe('Demo service contracts', () => {
  const { qualityDashboardService, configurationService } = createMockServices({ latency: 0 });

  describe('snapshot contract', () => {
    it('identifies metric and preserves filter scope on each point', async () => {
      const points = await qualityDashboardService.getSnapshots('hierarchyCompletionRate', '30d', {
        facilityId: 'demo-north',
        divisionId: 'prod-a',
      });

      expect(points.length).toBeGreaterThan(0);
      for (const point of points) {
        expect(point.metricId).toBe('hierarchyCompletionRate');
        expect(point.scope.facilityId).toBe('demo-north');
        expect(point.scope.divisionId).toBe('prod-a');
      }
    });

    it('returns different values when facility filter changes', async () => {
      const north = await qualityDashboardService.getSnapshots('hierarchyCompletionRate', '90d', {
        facilityId: 'demo-north',
      });
      const central = await qualityDashboardService.getSnapshots('hierarchyCompletionRate', '90d', {
        facilityId: 'demo-central',
      });

      expect(north.length).toBeGreaterThan(0);
      expect(central.length).toBeGreaterThan(0);
      expect(north[north.length - 1]?.valuePercent).not.toBe(central[central.length - 1]?.valuePercent);
    });

    it('respects trend window size', async () => {
      const shortWindow = await qualityDashboardService.getSnapshots('failureDocumentationRate', '7d', {});
      const longWindow = await qualityDashboardService.getSnapshots('failureDocumentationRate', '90d', {});

      expect(shortWindow.length).toBeLessThanOrEqual(longWindow.length);
    });
  });

  describe('alerts contract', () => {
    it('returns AlertsDto with summary counts and optional metric linkage', async () => {
      const alertsPage = await qualityDashboardService.getAlerts({});

      expect(Array.isArray(alertsPage.alerts)).toBe(true);
      expect(alertsPage.summary.total).toBe(alertsPage.alerts.length);
      expect(alertsPage.summary.critical).toBe(
        alertsPage.alerts.filter((alert) => alert.severity === 'critical').length
      );

      const withMetric = alertsPage.alerts.find((alert) => alert.metricId !== null);
      expect(withMetric).toBeDefined();
      expect(withMetric?.dimensionId).toBeTruthy();
      expect(withMetric?.actionKey).toBeTruthy();
    });
  });

  describe('run scope contract', () => {
    it('handles refresh with all scope', async () => {
      const status = await qualityDashboardService.refresh();
      expect(status.scope).toEqual({ kind: 'all' });
      expect(status.inProgress).toBe(false);
    });

    it('handles rerun with dimension scope', async () => {
      const status = await qualityDashboardService.rerun({ dimensionId: 'time-series' });
      expect(status.scope).toEqual({ kind: 'dimension', dimensionId: 'time-series' });
    });

    it('handles rerun with metric scope', async () => {
      const status = await qualityDashboardService.rerun({ metricId: 'orphansByPrefix' });
      expect(status.scope).toEqual({ kind: 'metric', metricId: 'orphansByPrefix' });
    });
  });

  describe('metric aggregate contract', () => {
    it('returns eligible, passing, issue, and derived valuePercent on dimension rows', async () => {
      const dimension = await qualityDashboardService.getDimension('asset-hierarchy', {});
      const row = dimension.aggregateRows.find(
        (metric) => metric.metricId === 'hierarchyCompletionRate'
      );

      expect(row).toBeDefined();
      expect(row?.eligibleCount).toBeGreaterThan(0);
      expect(row?.passingCount).toBeLessThanOrEqual(row?.eligibleCount ?? 0);
      expect(row?.issueCount).toBeGreaterThanOrEqual(0);
      expect(row?.valuePercent).toBe(
        row?.eligibleCount === 0
          ? null
          : Math.round(((row?.passingCount ?? 0) / (row?.eligibleCount ?? 1)) * 100)
      );
      expect(row?.actionKey).toBeTruthy();
    });
  });

  describe('optional disabled dimension contract', () => {
    it('returns disabled DTO without aggregate rows', async () => {
      await configurationService.updateConfig({ model3dAvailable: false });
      const dimension = await qualityDashboardService.getDimension('model-3d', {});

      expect(dimension.enabled).toBe(false);
      expect(dimension.status).toBe('disabled');
      expect(dimension.aggregateRows).toEqual([]);
      expect(dimension.entityCount).toBeNull();

      await configurationService.updateConfig({ model3dAvailable: true });
    });

    it('rejects drilldown for disabled dimension metrics', async () => {
      await configurationService.updateConfig({ model3dAvailable: false });

      await expect(
        qualityDashboardService.getDrilldown('unlinkedTier1', {})
      ).rejects.toThrow(/unavailable/i);

      await configurationService.updateConfig({ model3dAvailable: true });
    });
  });

  describe('facility catalog policy', () => {
    it('uses fictional demo facility IDs and display names only', () => {
      expect(FACILITIES.map((facility) => facility.id)).toEqual([
        'demo-north',
        'demo-central',
        'demo-south',
      ]);
      expect(FACILITIES.map((facility) => facility.name)).toEqual([
        'Demo North',
        'Demo Central',
        'Demo South',
      ]);
    });
  });
});
