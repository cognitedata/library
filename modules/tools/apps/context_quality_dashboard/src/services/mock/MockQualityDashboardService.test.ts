import { describe, expect, it } from 'vitest';

import { createMockServices } from './createMockServices';

describe('MockQualityDashboardService', () => {
  const { qualityDashboardService, configurationService } = createMockServices({ latency: 0 });

  it('returns deterministic overview dimension cards for default filters', async () => {
    const first = await qualityDashboardService.getOverview({});
    const second = await qualityDashboardService.getOverview({});

    expect(first.dimensionCards.length).toBe(second.dimensionCards.length);
    expect(first.dimensionCards.map((card) => card.dimensionId)).toEqual(
      second.dimensionCards.map((card) => card.dimensionId)
    );
    expect(first.dimensionCards.length).toBe(7);
  });

  it('returns different overview scope counts for different facility filters', async () => {
    const all = await qualityDashboardService.getOverview({});
    const north = await qualityDashboardService.getOverview({ facilityId: 'demo-north' });
    const south = await qualityDashboardService.getOverview({ facilityId: 'demo-south' });

    expect(north.scope.entityCount).toBeLessThan(all.scope.entityCount);
    expect(south.scope.entityCount).toBeLessThan(all.scope.entityCount);
    expect(north.scope.entityCount).not.toBe(south.scope.entityCount);
  });

  it('returns drill-down rows that shrink when facility filter is applied', async () => {
    const allRows = await qualityDashboardService.getDrilldown('orphansByPrefix', {});
    const habshanRows = await qualityDashboardService.getDrilldown('orphansByPrefix', {
      facilityId: 'demo-north',
    });

    expect(habshanRows.rows.length).toBeLessThanOrEqual(allRows.rows.length);
  });

  it('marks 3D dimension as disabled on overview when model3dAvailable is false', async () => {
    await configurationService.updateConfig({ model3dAvailable: false });
    const overview = await qualityDashboardService.getOverview({});

    const modelCard = overview.dimensionCards.find((card) => card.dimensionId === 'model-3d');
    expect(modelCard).toBeDefined();
    expect(modelCard?.enabled).toBe(false);
    expect(modelCard?.status).toBe('disabled');
    expect(modelCard?.overallPercent).toBeNull();

    await configurationService.updateConfig({ model3dAvailable: true });
  });

  it('updates run status on refresh without changing overview scores', async () => {
    const before = await qualityDashboardService.getOverview({});
    const status = await qualityDashboardService.refresh();
    const after = await qualityDashboardService.getOverview({});

    expect(status.inProgress).toBe(false);
    expect(before.dimensionCards).toEqual(after.dimensionCards);
  });

  it('returns issues with summary counts from the service layer', async () => {
    const issuesPage = await qualityDashboardService.getIssues({});

    expect(issuesPage.summary.total).toBe(issuesPage.issues.length);
    expect(issuesPage.summary.critical).toBe(
      issuesPage.issues.filter((issue) => issue.tier === 1).length
    );
    expect(issuesPage.issues[0]?.actionKey).toBeTruthy();
    expect(issuesPage.issues[0]?.entityLabel).toBeTruthy();
  });

  it('returns fewer issues when facility filter is applied', async () => {
    const all = await qualityDashboardService.getIssues({});
    const north = await qualityDashboardService.getIssues({ facilityId: 'demo-north' });

    expect(north.issues.length).toBeLessThan(all.issues.length);
    expect(north.issues.every((issue) => issue.facilityId === 'demo-north')).toBe(true);
  });

  it('returns alerts with summary counts from the service layer', async () => {
    const alertsPage = await qualityDashboardService.getAlerts({});

    expect(alertsPage.summary.total).toBe(alertsPage.alerts.length);
    expect(alertsPage.summary.critical).toBe(
      alertsPage.alerts.filter((alert) => alert.severity === 'critical').length
    );
    expect(alertsPage.alerts[0]?.actionKey).toBeTruthy();
    expect(alertsPage.alerts[0]?.affectedCount).not.toBeNull();
  });
});

describe('MockConfigurationService', () => {
  const { configurationService } = createMockServices({ latency: 0 });

  it('returns canned preview delta for exclusion rules', async () => {
    const preview = await configurationService.previewExclusion({
      kind: 'file-extension',
      pattern: '.png',
    });

    expect(preview.additionalExcluded).toBe(4);
  });

  it('persists configuration patches in memory', async () => {
    await configurationService.updateConfig({ maintenanceDimensionEnabled: false });
    const config = await configurationService.getConfig();

    expect(config.maintenanceDimensionEnabled).toBe(false);

    await configurationService.updateConfig({ maintenanceDimensionEnabled: true });
  });
});
