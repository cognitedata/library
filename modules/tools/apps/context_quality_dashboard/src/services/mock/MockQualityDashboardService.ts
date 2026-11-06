import { computeValuePercent } from '../../domain/metricMath';
import type { MockDataset } from '../../mock/dataset';
import { HEADLINE_METRIC_IDS } from '../../mock/snapshots';
import type {
  AggregateMetricRowDto,
  AlertsDto,
  AlertDto,
  ClassificationsDto,
  ConfiguredDrilldownDto,
  ConfiguredMetricKey,
  ConfiguredMetricTrendPointDto,
  ConfiguredMetricsDto,
  RunConfigurationDto,
  AppConfigDto,
  CustomQualityMetricDto,
  CustomRuleDrilldownDto,
  DimensionDto,
  DimensionId,
  DrilldownDto,
  DrilldownSummaryDto,
  IssueDto,
  IssuesDto,
  MetricIssuesDto,
  MetricId,
  NotifyOwnerInput,
  NotifyOwnerResult,
  OverviewDto,
  QualityIssue,
  QualityRule,
  RunStatusDto,
  RunWarningsDto,
  SliceFilters,
  SnapshotPointDto,
  TrendWindow,
} from '../../types';
import type { QualityDashboardService } from '../QualityDashboardService';

import {
  buildQualityIssues,
  computeTierScores,
  filterIssues,
  getEnabledDimensions,
  isDimensionEnabled,
} from './aggregation';
import { withLatency, type LatencyMode } from './latency';
import {
  buildDrilldownDto,
  drilldownMetricFor,
  computeMetricStatus,
  DRILLDOWN_METRIC_IDS,
  getDimensionPrimaryActionKey,
  getMetricActionKey,
  getMetricDimension,
  hasDrilldownList,
  isLifecycleMetric,
  shouldShowMetric,
} from './metricCatalog';
import { computeMetricCounts } from './metricComputation';
import { buildMockClassificationGroups } from './mockClassifications';
import {
  ALL_DIMENSION_IDS,
  computeDimensionStatus,
  computeOverallTrend,
  computePortfolioAggregatePercent,
  computePortfolioTierScores,
  countDimensionEntities,
  countScopedEntities,
  findWeakestTier,
  isOptionalDimension,
} from './overviewAggregation';
import {
  buildCustomRuleDrilldownRows,
  computeQualityRuleMetrics,
} from './qualityRuleComputation';
import { buildBreakdown, untieredPool } from './tierBreakdown';
import { buildTrendPoints, filterAlertsBySlice } from './trends';

const CUSTOM_RULE_ACTION_KEY = 'action.reviewCustomRule';

type MockQualityDashboardServiceOptions = {
  dataset: MockDataset;
  getConfig: () => AppConfigDto;
  getQualityRules: () => readonly QualityRule[];
  latency: LatencyMode;
};

const DIMENSION_METRICS: Record<DimensionId, MetricId[]> = {
  'asset-hierarchy': [
    'hierarchyCompletionRate',
    'orphanRate',
    'orphansByTier',
    'depthOutliers',
    'typeCoverageGaps',
    'activeChildrenUnderRetiredParents',
  ],
  'equipment-asset': [
    'associationRate',
    'serialMfrCompleteness',
    'unmatchedWithCandidates',
    'completenessByManufacturer',
    'brokenCrossViewLinks',
  ],
  'time-series': [
    'tsToAssetRate',
    'monitoringCoverage',
    'freshness',
    'orphansByPrefix',
    'staleTimeSeries',
    'tier1CoverageGaps',
    'inferredTagsWithoutMatch',
  ],
  maintenance: [
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
    'tier1MaintenanceGaps',
  ],
  'file-annotation': [
    'annotationCount',
    'confidenceAverage',
    'approvalMix',
    'lowConfidenceQueue',
    'unprocessedBacklog',
    'rejectionByType',
    'pendingReview',
    'patternVsAnnotation',
    'staleAnnotationLinkages',
  ],
  'model-3d': [
    'threeDToAssetRate',
    'asset3dCoverage',
    'bboxCompleteness',
    'unlinkedTier1',
    'bboxGaps',
    'modelTypeCoverageByTier',
  ],
  files: [
    'fileToAssetRate',
    'metadataCompleteness',
    'unlinkedByCategory',
    'missingMetadata',
    'uploadStatusGaps',
    'uncontextualizedRevisions',
  ],
};

const DRILLDOWN_METRICS = [...DRILLDOWN_METRIC_IDS] as MetricId[];

/** The metric whose pass rate stands for each dimension; its counts are the dimension's all-tier population. */
const HEADLINE_METRIC: Record<DimensionId, MetricId> = {
  'asset-hierarchy': 'hierarchyCompletionRate',
  'equipment-asset': 'associationRate',
  'time-series': 'tsToAssetRate',
  maintenance: 'woToAssetRate',
  'file-annotation': 'approvalMix',
  'model-3d': 'threeDToAssetRate',
  files: 'fileToAssetRate',
};

export class MockQualityDashboardService implements QualityDashboardService {
  private readonly dataset: MockDataset;
  private readonly getConfig: () => AppConfigDto;
  private readonly getQualityRules: () => readonly QualityRule[];
  private readonly latency: LatencyMode;
  private runStatus: RunStatusDto;

  constructor(options: MockQualityDashboardServiceOptions) {
    this.dataset = options.dataset;
    this.getConfig = options.getConfig;
    this.getQualityRules = options.getQualityRules;
    this.latency = options.latency;
    this.runStatus = {
      lastSuccessAt: '2026-08-31T22:00:00.000Z',
      inProgress: false,
      scope: { kind: 'all' },
    };
  }

  getOverview(filters: SliceFilters): Promise<OverviewDto> {
    return withLatency(() => {
      const config = this.getConfig();
      const issues = buildQualityIssues(this.dataset, config);
      const enabledDimensions = getEnabledDimensions(config);
      const entityCount =
        countScopedEntities(this.dataset, filters) +
        ALL_DIMENSION_IDS.reduce(
          (sum, dimensionId) =>
            sum + (isDimensionEnabled(config, dimensionId) ? (untieredPool(this.dataset, dimensionId, filters)?.eligibleCount ?? 0) : 0),
          0
        );

      const dimensionCards = ALL_DIMENSION_IDS.map((dimensionId) => {
        const optional = isOptionalDimension(dimensionId);
        const enabled = isDimensionEnabled(config, dimensionId);

        if (!enabled) {
          return {
            dimensionId,
            nameKey: `dimension.${dimensionId}`,
            enabled: false,
            optional,
            overallPercent: null,
            tierScores: [],
            untiered: null,
            scoreBasis: config.tieringEnabled ? ('tiered' as const) : ('pooled' as const),
            issueCount: null,
            status: 'disabled' as const,
            weakestTier: null,
          };
        }

        const pool = untieredPool(this.dataset, dimensionId, filters);
        if (countDimensionEntities(this.dataset, dimensionId, filters) + (pool?.eligibleCount ?? 0) === 0) {
          // Nothing to measure here: no score, no issue count, and not "good".
          return {
            dimensionId,
            nameKey: `dimension.${dimensionId}`,
            enabled: true,
            optional,
            overallPercent: null,
            tierScores: [],
            untiered: null,
            scoreBasis: config.tieringEnabled ? ('tiered' as const) : ('pooled' as const),
            issueCount: null,
            status: 'not-applicable' as const,
            weakestTier: null,
          };
        }

        const { tierScores, breakdown } = this.breakdownFor(config, dimensionId, filters, pool);
        const issueCount = filterIssues(issues, filters, dimensionId).length;
        const status = computeDimensionStatus(breakdown.statusScores);
        const weakestTier = findWeakestTier(tierScores);

        return {
          dimensionId,
          nameKey: `dimension.${dimensionId}`,
          enabled: true,
          optional,
          overallPercent: breakdown.overallPercent,
          tierScores,
          untiered: breakdown.untiered,
          scoreBasis: breakdown.scoreBasis,
          issueCount,
          status,
          weakestTier,
        };
      });

      const overallAggregatePercent =
        entityCount === 0 ? null : computePortfolioAggregatePercent(dimensionCards);
      const tierScores =
        entityCount === 0 ? [] : computePortfolioTierScores(this.dataset, config, filters);
      const scoredCards = dimensionCards.filter((card) => card.enabled && card.status !== 'not-applicable');
      const untieredEligible = scoredCards.reduce((sum, card) => sum + (card.untiered?.eligibleCount ?? 0), 0);
      const untieredPassing = scoredCards.reduce((sum, card) => sum + (card.untiered?.passingCount ?? 0), 0);

      const insights = this.dataset.insights
        .filter((insight) => enabledDimensions.includes(insight.dimensionId))
        .map((insight) => ({
          id: insight.id,
          dimensionId: insight.dimensionId,
          source: insight.source,
          messageKey: insight.messageKey,
          actionKey: getDimensionPrimaryActionKey(insight.dimensionId),
          mentionsTier: insight.mentionsTier,
        }));

      const insightIds = insights.map((insight) => insight.id);

      const alertSummaries = filterAlertsBySlice(this.dataset.alerts, filters).map((alert) => {
        const card = dimensionCards.find((entry) => entry.dimensionId === alert.dimensionId);
        const criticalScore = card?.tierScores.find((entry) => entry.tier === 1);
        const hasTarget =
          criticalScore !== undefined && criticalScore.scored && criticalScore.targetPercent > 0;
        return {
          alertId: alert.id,
          type: alert.type,
          severity: alert.severity,
          titleKey: alert.titleKey,
          dimensionId: alert.dimensionId,
          createdAt: alert.createdAt,
          currentPercent: card?.overallPercent ?? null,
          targetPercent: hasTarget ? criticalScore.targetPercent : null,
        };
      });

      const userExclusionSummary = config.exclusionRules
        .filter((rule) => rule.enabled)
        .map((rule) => ({
          id: rule.id,
          label: rule.label,
          count: rule.excludedCount,
        }));

      const headlineMetrics = HEADLINE_METRIC_IDS.map((metricId) => ({
        metricId,
        labelKey: `metric.${metricId}`,
      }));

      const headlineSnapshots = HEADLINE_METRIC_IDS.flatMap((metricId) =>
        buildTrendPoints(this.dataset.snapshots, metricId, '30d', filters).slice(-1)
      );

      const overallTrend =
        entityCount === 0
          ? null
          : computeOverallTrend(this.dataset.snapshots, HEADLINE_METRIC_IDS, filters);

      return {
        overallAggregatePercent,
        overallTrend,
        tierScores,
        untiered:
          config.tieringEnabled && untieredEligible > 0
            ? {
                scorePercent: computeValuePercent(untieredPassing, untieredEligible),
                eligibleCount: untieredEligible,
                passingCount: untieredPassing,
              }
            : null,
        scope: {
          entityCount,
          facilityId: filters.facilityId,
          divisionId: filters.divisionId,
          disciplineId: filters.disciplineId,
          ownerId: filters.ownerId,
        },
        dimensionCards,
        excludedSummary: [...this.dataset.systemExclusionBuckets],
        userExclusionSummary,
        insights,
        insightIds,
        alertSummaries,
        headlineMetrics,
        headlineSnapshots,
      };
    }, this.latency);
  }

  getDimension(dimensionId: DimensionId, filters: SliceFilters): Promise<DimensionDto> {
    return withLatency(() => {
      const config = this.getConfig();
      const optional = isOptionalDimension(dimensionId);
      const userExclusionSummary = config.exclusionRules
        .filter((rule) => rule.enabled)
        .map((rule) => ({
          id: rule.id,
          label: rule.label,
          count: rule.excludedCount,
        }));

      if (!isDimensionEnabled(config, dimensionId)) {
        return {
          dimensionId,
          nameKey: `dimension.${dimensionId}`,
          enabled: false,
          optional,
          status: 'disabled',
          entityCount: null,
          tierScores: [],
          untiered: null,
          scoreBasis: config.tieringEnabled ? 'tiered' : 'pooled',
          overallPercent: null,
          aggregateRows: [],
          excludedBuckets: [...this.dataset.systemExclusionBuckets],
          userExclusionSummary,
          insightIds: [],
          extendedMetricsAvailable: false,
        };
      }

      const pool = untieredPool(this.dataset, dimensionId, filters);
      const entityCount = countDimensionEntities(this.dataset, dimensionId, filters) + (pool?.eligibleCount ?? 0);
      if (entityCount === 0) {
        return {
          dimensionId,
          nameKey: `dimension.${dimensionId}`,
          enabled: true,
          optional,
          status: 'not-applicable',
          entityCount: 0,
          tierScores: [],
          untiered: null,
          scoreBasis: config.tieringEnabled ? 'tiered' : 'pooled',
          overallPercent: null,
          aggregateRows: [],
          drilldownSummaries: [],
          excludedBuckets: [...this.dataset.systemExclusionBuckets],
          userExclusionSummary,
          lifecycleWarnings: [],
          insightIds: [],
          extendedMetricsAvailable: config.failureTaxonomyAvailable,
        };
      }

      const issues = buildQualityIssues(this.dataset, config);
      const { tierScores, breakdown } = this.breakdownFor(config, dimensionId, filters, pool);
      const status = computeDimensionStatus(breakdown.statusScores);

      const aggregateRows = this.buildAggregateRows(dimensionId, issues, filters, config);
      const insightIds = this.dataset.insights
        .filter((insight) => insight.dimensionId === dimensionId)
        .map((insight) => insight.id);

      return {
        dimensionId,
        nameKey: `dimension.${dimensionId}`,
        enabled: true,
        optional,
        status,
        entityCount,
        tierScores,
        untiered: breakdown.untiered,
        scoreBasis: breakdown.scoreBasis,
        overallPercent: breakdown.overallPercent,
        aggregateRows,
        excludedBuckets: [...this.dataset.systemExclusionBuckets],
        userExclusionSummary,
        insightIds,
        extendedMetricsAvailable: config.failureTaxonomyAvailable,
      };
    }, this.latency);
  }

  getDrilldown(metricId: MetricId, filters: SliceFilters): Promise<DrilldownDto> {
    return withLatency(() => {
      const config = this.getConfig();
      const dimensionId = getMetricDimension(metricId);
      if (!isDimensionEnabled(config, dimensionId)) {
        throw new Error(`Metric ${metricId} is unavailable`);
      }
      if (!shouldShowMetric(metricId, config)) {
        throw new Error(`Metric ${metricId} is hidden by configuration`);
      }

      const source = drilldownMetricFor(metricId) ?? metricId;
      const issues = filterIssues(
        buildQualityIssues(this.dataset, config),
        filters,
        dimensionId,
        source
      );
      // A link metric lists its unlinked entities from its drill-down metric; the list stays under the metric opened.
      const opened = {
        ...buildDrilldownDto(source, issues),
        metricId,
        labelKey: `metric.${metricId}`,
        actionKey: getMetricActionKey(metricId),
      };
      return this.withUntieredRows(opened, filters);
    }, this.latency);
  }

  getSnapshots(
    metricId: MetricId,
    window: TrendWindow,
    filters: SliceFilters
  ): Promise<SnapshotPointDto[]> {
    return withLatency(
      () => buildTrendPoints(this.dataset.snapshots, metricId, window, filters),
      this.latency
    );
  }

  getAlerts(filters: SliceFilters): Promise<AlertsDto> {
    return withLatency(() => {
      const config = this.getConfig();
      const enabledDimensions = new Set(getEnabledDimensions(config));
      const issues = buildQualityIssues(this.dataset, config);

      const alerts = filterAlertsBySlice(this.dataset.alerts, filters)
        .filter((alert) => enabledDimensions.has(alert.dimensionId))
        .map((alert) => this.mapAlertDto(alert, issues, filters));

      const summary = {
        total: alerts.length,
        critical: alerts.filter((alert) => alert.severity === 'critical').length,
        warning: alerts.filter((alert) => alert.severity === 'warning').length,
        informational: alerts.filter((alert) => alert.severity === 'info').length,
      };

      return { alerts, summary };
    }, this.latency);
  }

  /** The demo data has no config file, so it has no configured metrics. */
  getConfiguredMetrics(): Promise<ConfiguredMetricsDto> {
    return withLatency(() => ({ models: [] }), this.latency);
  }

  /** The demo data has no config file, so no configured metric lists instances. */
  getConfiguredDrilldown(key: ConfiguredMetricKey): Promise<ConfiguredDrilldownDto> {
    return withLatency(() => ({ key, rows: [], cutAt: null, dropped: 0 }), this.latency);
  }

  getConfiguredMetricTrends(): Promise<ConfiguredMetricTrendPointDto[]> {
    return withLatency(() => [], this.latency);
  }

  /** The demo data has no config file, so no run was started with one. */
  getLatestRunConfiguration(): Promise<RunConfigurationDto | null> {
    return withLatency(() => null, this.latency);
  }

  getClassifications(filters: SliceFilters): Promise<ClassificationsDto> {
    return withLatency(
      () => ({ groups: buildMockClassificationGroups(countScopedEntities(this.dataset, filters)) }),
      this.latency
    );
  }

  getIssues(filters: SliceFilters): Promise<IssuesDto> {
    return withLatency(() => {
      const config = this.getConfig();
      const enabledDimensions = new Set(getEnabledDimensions(config));
      const tiered = filterIssues(buildQualityIssues(this.dataset, config), filters)
        .filter((issue) => enabledDimensions.has(issue.dimensionId))
        .map((issue) => this.mapIssueDto(issue));
      const issues = [
        ...tiered,
        ...this.untieredIssues(filters).filter((issue) => enabledDimensions.has(issue.dimensionId)),
      ];

      const summary = {
        total: issues.length,
        critical: issues.filter((issue) => issue.tier === 1).length,
        important: issues.filter((issue) => issue.tier === 2).length,
        standard: issues.filter((issue) => issue.tier === 3).length,
        unassigned: issues.filter((issue) => issue.tier === null).length,
      };

      return { issues, summary, partial: null };
    }, this.latency);
  }

  getLifecycleWarnings(dimensionId: DimensionId, filters: SliceFilters): Promise<DrilldownSummaryDto[]> {
    return withLatency(() => {
      const issues = buildQualityIssues(this.dataset, this.getConfig());
      return this.buildDrilldownSummaries(dimensionId, issues, filters).filter((summary) =>
        isLifecycleMetric(summary.metricId)
      );
    }, this.latency);
  }

  getIssueMetricIds(): readonly MetricId[] {
    return DRILLDOWN_METRIC_IDS;
  }

  async getIssuesOfMetric(metricId: MetricId, filters: SliceFilters): Promise<MetricIssuesDto> {
    const { issues } = await this.getIssues(filters);
    return { issues: issues.filter((issue) => issue.metricId === metricId), cut: false, limit: Number.MAX_SAFE_INTEGER };
  }

  getRunStatus(): Promise<RunStatusDto> {
    return withLatency(() => this.runStatus, this.latency);
  }

  getRunWarnings(): Promise<RunWarningsDto> {
    return withLatency(() => this.dataset.states.runWarnings, this.latency);
  }

  refresh(): Promise<RunStatusDto> {
    return this.simulateRun({ kind: 'all' });
  }

  rerun(scope: { dimensionId: DimensionId } | { metricId: MetricId }): Promise<RunStatusDto> {
    if ('dimensionId' in scope) {
      return this.simulateRun({ kind: 'dimension', dimensionId: scope.dimensionId });
    }
    return this.simulateRun({ kind: 'metric', metricId: scope.metricId });
  }

  notifyOwner(_input: NotifyOwnerInput): Promise<NotifyOwnerResult> {
    return withLatency(() => ({ ok: true }), this.latency);
  }

  getCustomQualityMetrics(_filters: SliceFilters): Promise<readonly CustomQualityMetricDto[]> {
    return withLatency(() => {
      return this.getQualityRules()
        .filter((rule) => rule.enabled)
        .map((rule) => {
          const metrics = computeQualityRuleMetrics(rule);
          return {
            ruleId: rule.ruleId,
            name: rule.name,
            sourceView: rule.sourceView,
            relationshipTargetDisplayName: rule.relationship.targetDisplayName,
            eligibleCount: metrics.eligibleCount,
            passingCount: metrics.passingCount,
            issueCount: metrics.issueCount,
            valuePercent: metrics.valuePercent,
            severity: rule.severity,
            tier: rule.tier,
            ownerId: rule.ownerId,
            actionKey: CUSTOM_RULE_ACTION_KEY,
            enabled: rule.enabled,
          };
        });
    }, this.latency);
  }

  getCustomRuleDrilldown(ruleId: string, _filters: SliceFilters): Promise<CustomRuleDrilldownDto> {
    return withLatency(() => {
      const rule = this.getQualityRules().find((entry) => entry.ruleId === ruleId);
      if (rule === undefined) {
        throw new Error(`Custom quality rule ${ruleId} was not found`);
      }

      const rows = buildCustomRuleDrilldownRows(
        rule.relationship,
        rule.condition,
        rule.sourceView
      );

      return {
        ruleId: rule.ruleId,
        labelKey: `qualityRule.${rule.ruleId}`,
        actionKey: CUSTOM_RULE_ACTION_KEY,
        ownerId: rule.ownerId,
        columnKeys: ['entity', 'relatedCount', 'status', 'owner'],
        rows: rows.map((row) => ({
          id: row.entityId,
          tier: rule.tier,
          ownerId: rule.ownerId,
          classes: {},
          cells: {
            entity: row.entityLabel,
            relatedCount: String(row.relatedCount),
            status: row.status === 'pass' ? 'PASS' : 'ISSUE',
            owner: rule.ownerId,
          },
        })),
      };
    }, this.latency);
  }

  private buildAggregateRows(
    dimensionId: DimensionId,
    issues: readonly QualityIssue[],
    filters: SliceFilters,
    config: AppConfigDto
  ): AggregateMetricRowDto[] {
    const metricIds = DIMENSION_METRICS[dimensionId].filter((metricId) =>
      shouldShowMetric(metricId, config)
    );

    return metricIds.map((metricId) => {
      const counts = computeMetricCounts(metricId, this.dataset, config, filters, issues);
      const fixtureReason = this.dataset.states.notApplicableMetrics[metricId] ?? null;
      const notApplicable = fixtureReason !== null || counts.eligibleCount === 0;
      const direction =
        metricId.includes('Rate') || metricId.includes('Coverage') || metricId.includes('Completeness')
          ? 'critical'
          : 'informational';
      return {
        metricId,
        labelKey: `metric.${metricId}`,
        direction,
        eligibleCount: fixtureReason === null ? counts.eligibleCount : 0,
        passingCount: fixtureReason === null ? counts.passingCount : 0,
        issueCount: fixtureReason === null ? counts.issueCount : 0,
        valuePercent: notApplicable ? null : counts.valuePercent,
        tier: 'all',
        extended: metricId === 'failureDocumentationRate' || metricId === 'patternVsAnnotation',
        hasDrilldown: hasDrilldownList(metricId),
        actionKey: getMetricActionKey(metricId),
        status: notApplicable ? 'not-applicable' : computeMetricStatus(direction, counts.valuePercent, counts.issueCount),
        notApplicable,
        notApplicableReason: fixtureReason,
      };
    });
  }

  /** Tier scores as before, plus the untiered pool and the score basis, by the same rules as CDF mode. */
  private breakdownFor(
    config: AppConfigDto,
    dimensionId: DimensionId,
    filters: SliceFilters,
    pool: ReturnType<typeof untieredPool>
  ) {
    const { tierScores, overallPercent } = computeTierScores(this.dataset, config, dimensionId, filters);
    const headline = computeMetricCounts(HEADLINE_METRIC[dimensionId], this.dataset, config, filters, []);
    const breakdown = buildBreakdown({
      tierScores,
      overallPercent,
      untiered: pool,
      allTiers: { eligible: headline.eligibleCount, passing: headline.passingCount },
      tieringEnabled: config.tieringEnabled,
    });
    return { tierScores, breakdown };
  }

  /** Untiered entities have no division, discipline or tier; a facility or owner filter can still select them. */
  private untieredIssues(filters: SliceFilters): IssueDto[] {
    if (filters.divisionId !== undefined || filters.disciplineId !== undefined) {
      return [];
    }
    return this.dataset.states.untieredIssues
      .filter(
        (issue) =>
          (filters.facilityId === undefined || issue.facilityId === filters.facilityId) &&
          (filters.ownerId === undefined || issue.ownerId === filters.ownerId)
      )
      .map((issue) => ({
        id: issue.id,
        kind: issue.kind,
        dimensionId: issue.dimensionId,
        metricId: issue.metricId,
        entityId: issue.entityId,
        entityLabel: issue.entityLabel,
        tier: null,
        facilityId: issue.facilityId,
        ownerId: issue.ownerId,
        detail: issue.detail,
        actionKey: getMetricActionKey(issue.metricId),
        classes: {},
      }));
  }

  private withUntieredRows(drilldown: DrilldownDto, filters: SliceFilters): DrilldownDto {
    const extra = this.untieredIssues(filters).filter((issue) => issue.metricId === drilldown.metricId);
    return {
      ...drilldown,
      rows: [
        ...drilldown.rows,
        ...extra.map((issue) => ({
          id: issue.id,
          tier: null,
          ownerId: issue.ownerId,
          classes: {},
          cells: { entity: issue.entityLabel, tier: '', detail: issue.detail, owner: issue.ownerId },
        })),
      ],
    };
  }

  private buildDrilldownSummaries(
    dimensionId: DimensionId,
    issues: readonly QualityIssue[],
    filters: SliceFilters
  ): DrilldownSummaryDto[] {
    return DRILLDOWN_METRICS
      .filter((metricId) => getMetricDimension(metricId) === dimensionId)
      .map((metricId) => {
        const count = filterIssues(issues, filters, dimensionId, metricId).length;
        return {
          metricId,
          labelKey: `metric.${metricId}`,
          count,
          actionKey: getMetricActionKey(metricId),
        };
      })
      .filter((summary) => summary.count > 0);
  }

  private mapIssueDto(issue: QualityIssue): IssueDto {
    return {
      id: issue.id,
      kind: issue.kind,
      dimensionId: issue.dimensionId,
      metricId: issue.metricId,
      entityId: issue.entityId,
      entityLabel: issue.entityLabel,
      tier: issue.tier,
      facilityId: issue.facilityId,
      ownerId: issue.ownerId,
      detail: issue.detail,
      actionKey: getMetricActionKey(issue.metricId),
      classes: {},
    };
  }

  private mapAlertDto(
    alert: (typeof this.dataset.alerts)[number],
    issues: readonly QualityIssue[],
    filters: SliceFilters
  ): AlertDto {
    const affectedCount = alert.metricId
      ? filterIssues(issues, filters, alert.dimensionId, alert.metricId).length
      : filterIssues(issues, filters, alert.dimensionId).length;
    const actionKey = alert.metricId
      ? getMetricActionKey(alert.metricId)
      : getDimensionPrimaryActionKey(alert.dimensionId);

    return {
      id: alert.id,
      type: alert.type,
      severity: alert.severity,
      status: alert.status,
      titleKey: alert.titleKey,
      bodyKey: alert.bodyKey,
      actionKey,
      dimensionId: alert.dimensionId,
      metricId: alert.metricId,
      facilityId: alert.facilityId,
      ownerId: alert.ownerId,
      affectedCount,
      createdAt: alert.createdAt,
    };
  }

  private simulateRun(scope: RunStatusDto['scope']): Promise<RunStatusDto> {
    this.runStatus = {
      lastSuccessAt: this.runStatus.lastSuccessAt,
      inProgress: true,
      scope,
    };

    const completionDelay = this.latency === 'random' ? 0 : this.latency;

    return new Promise((resolve) => {
      setTimeout(() => {
        this.runStatus = {
          lastSuccessAt: new Date('2026-09-01T12:00:00.000Z').toISOString(),
          inProgress: false,
          scope,
        };
        resolve(this.runStatus);
      }, completionDelay);
    });
  }
}
