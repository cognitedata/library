// @vitest-environment node
import type { HostAppAPI } from '@cognite/app-sdk';
import { describe, expect, it } from 'vitest';

import { CdfCatalogService } from './CdfCatalogService';
import { CdfQualityDashboardService } from './CdfQualityDashboardService';
import { QualityStreamReader } from './QualityStreamReader';
import { HttpRecordsClient } from './recordsClient';
import { resolveBackendConfig } from './streamConfig';

/**
 * Opt-in check against a real metrics stream. Skipped unless these are set:
 *   VITE_CQ_LIVE_BASE_URL, VITE_CQ_LIVE_PROJECT, VITE_CQ_LIVE_TOKEN
 * The stream is named by the same variables the app uses (VITE_CQ_STREAM_ID, VITE_CQ_SCHEMA_SPACE); there are no defaults.
 */
function readEnv(key: string): string | undefined {
  const value: unknown = import.meta.env[key];
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

const baseUrl = readEnv('VITE_CQ_LIVE_BASE_URL');
const project = readEnv('VITE_CQ_LIVE_PROJECT');
const token = readEnv('VITE_CQ_LIVE_TOKEN');
const resolved = resolveBackendConfig({
  VITE_QUALITY_BACKEND: 'cdf',
  VITE_CQ_STREAM_ID: readEnv('VITE_CQ_STREAM_ID'),
  VITE_CQ_SCHEMA_SPACE: readEnv('VITE_CQ_SCHEMA_SPACE'),
  VITE_CQ_RELATIONSHIP_CONTAINER: readEnv('VITE_CQ_RELATIONSHIP_CONTAINER'),
  VITE_CQ_WINDOW_DAYS: readEnv('VITE_CQ_WINDOW_DAYS'),
});
const streamConfig =
  resolved.ok && resolved.config.backend === 'cdf' ? resolved.config.stream : undefined;
const live =
  baseUrl !== undefined && project !== undefined && token !== undefined && streamConfig !== undefined;

function hostApi(): HostAppAPI {
  return {
    getBaseUrl: async () => baseUrl ?? '',
    getProject: async () => project ?? '',
    getAccessToken: async () => token ?? '',
  } as Partial<HostAppAPI> as HostAppAPI;
}

describe.skipIf(!live)('CdfQualityDashboardService against a live stream', () => {
  const reader = new QualityStreamReader(
    new HttpRecordsClient({
      getHostApi: hostApi,
      fetchFn: (input, init) => fetch(input, init),
      config: streamConfig ?? { streamId: '', schemaSpace: '', relationshipContainer: '', windowDays: null },
      now: () => Date.now(),
    }),
    () => Date.now()
  );
  const service = new CdfQualityDashboardService({ reader, now: () => Date.now() });

  it('reads the overview and prints what the dashboard would show', async () => {
    const overview = await service.getOverview({});
    const summary = overview.dimensionCards.map((card) => ({
      dimension: card.dimensionId,
      overall: card.overallPercent,
      tiers: card.tierScores.map((tier) => tier.scorePercent),
      issues: card.issueCount,
      status: card.status,
    }));
    console.log('OVERVIEW', JSON.stringify({ entityCount: overview.scope.entityCount, aggregate: overview.overallAggregatePercent, trend: overview.overallTrend, cards: summary, excluded: overview.excludedSummary.map((b) => `${b.kind}:${b.count}`), user: overview.userExclusionSummary.map((u) => `${u.id}:${u.count}`) }));
    expect(overview.scope.entityCount).toBeGreaterThan(0);
  });

  it('reads a dimension, a drill-down, issues, trends, run status and the catalog', async () => {
    const dimension = await service.getDimension('time-series', { facilityId: 'north' });
    console.log('DIMENSION time-series/north', JSON.stringify(dimension.aggregateRows.map((r) => `${r.metricId}:${r.passingCount}/${r.eligibleCount}=${r.valuePercent}`)));
    const drilldown = await service.getDrilldown('orphansByTier', {});
    console.log('DRILLDOWN orphansByTier rows', drilldown.rows.length, JSON.stringify(drilldown.rows[0]));
    const issues = await service.getIssues({});
    console.log('ISSUES', JSON.stringify(issues.summary));
    const trend = await service.getSnapshots('woToNotificationRate', '90d', {});
    console.log('TREND woToNotificationRate', JSON.stringify(trend.map((p) => `${p.capturedAt.slice(0, 10)}=${p.valuePercent}`)));
    console.log('RUN', JSON.stringify(await service.getRunStatus()));
    const catalog = new CdfCatalogService({ reader });
    console.log('CATALOG', JSON.stringify({ facilities: (await catalog.getFacilities()).map((f) => f.id), divisions: (await catalog.getDivisions()).map((f) => f.id), disciplines: (await catalog.getDisciplines()).map((f) => f.id), owners: (await catalog.getDataOwners()).map((f) => f.id) }));
    expect(dimension.enabled).toBe(true);
    expect(drilldown.rows.length).toBeGreaterThan(0);
  });

  it('reads the auto-discovered classifications', async () => {
    const { groups } = await service.getClassifications({});
    console.log('CLASSIFICATIONS', JSON.stringify(groups.map((g) => ({ kind: g.kind, rows: g.rows.map((r) => `${r.label}:${r.contextualisedCount}/${r.totalCount}`) }))));
    expect(groups.length).toBeGreaterThan(0);
  });

  it('tags drilldown rows and issues with their classifications', async () => {
    const drilldown = await service.getDrilldown('unmatchedWithCandidates', {});
    const { issues } = await service.getIssues({});
    const classified = issues.filter((issue) => Object.keys(issue.classes).length > 0);
    console.log('DRILLDOWN_CLASSES', JSON.stringify({ columns: drilldown.columnKeys, first: drilldown.rows[0]?.cells, issues: issues.length, classifiedIssues: classified.length, sample: classified[0]?.classes }));
    expect(classified.length).toBeGreaterThan(0);
  });
});
