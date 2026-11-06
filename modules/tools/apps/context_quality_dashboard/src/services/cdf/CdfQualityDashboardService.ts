import { groupConfiguredMetricsByModel } from '../../domain/configuredMetricsByModel';
import {
  ALL_DIMENSION_IDS,
  computeDimensionStatus,
  computePortfolioAggregatePercent,
  findWeakestTier,
  isOptionalDimension,
} from '../../domain/dimensions';
import {
  DRILLDOWN_METRIC_IDS,
  computeMetricStatus,
  drilldownMetricFor,
  drilldownMetricForField,
  getMetricActionKey,
  getMetricDimension,
  hasDrilldownList,
  isExtendedMetric,
  isLifecycleMetric,
  isMetricId,
} from '../../domain/metricCatalog';
import { computeValuePercent } from '../../domain/metricMath';
import type {
  AggregateMetricRowDto,
  AlertsDto,
  ClassificationGroupDto,
  ClassificationRowDto,
  ClassificationsDto,
  ConfiguredGroupDto,
  ConfiguredMetricKey,
  ConfiguredMetricTrendPointDto,
  ConfiguredDrilldownDto,
  ConfiguredMetricsDto,
  RunConfigurationDto,
  CustomQualityMetricDto,
  CustomRuleDrilldownDto,
  DimensionCardDto,
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
  RunStatusDto,
  RunWarningDto,
  RunWarningsDto,
  ScoreBasis,
  SliceFilters,
  SnapshotPointDto,
  Tier,
  TierScore,
  TrendWindow,
  UntieredScoreDto,
} from '../../types';
import { TIER_TARGETS } from '../../types';
import type { QualityDashboardService } from '../QualityDashboardService';

import { BackendError, isBackendError } from './backendErrors';
import {
  ISSUE_KIND_BY_METRIC,
  TREND_HEADLINE_METRIC_IDS,
  metricDirection,
  metricPercent,
  selectHeadlineRows,
  isSplitBySubKey,
  splitMetricFields,
  selectMetricRows,
} from './dimensionMetrics';
import { buildExclusionBuckets, buildUserExclusionSummary } from './exclusionBuckets';
import {
  matchesRowSlice,
  sumRows,
  type AggregateRow,
  type ClassificationRow,
  type DrilldownRow,
  type ExclusionRow,
} from './metricRows';
import { notApplicableReasonFor } from './notApplicableReasons';
import { extraLinkKey, type CurrentRuns, type QualityStreamReader, type RunInfo } from './QualityStreamReader';

/** The dimensions each run is current for, one entry per run: a run is read once however many dimensions it serves. */
function dimensionsByRun(current: CurrentRuns): Map<string, { run: RunInfo; dimensions: Set<DimensionId> }> {
  const byRun = new Map<string, { run: RunInfo; dimensions: Set<DimensionId> }>();
  for (const [dimensionId, run] of current.byDimension) {
    const entry = byRun.get(run.runId) ?? { run, dimensions: new Set<DimensionId>() };
    entry.dimensions.add(dimensionId);
    byRun.set(run.runId, entry);
  }
  return byRun;
}

function isDimensionIn(dimensions: ReadonlySet<DimensionId>, dimensionId: string): boolean {
  return ALL_DIMENSION_IDS.some((id) => id === dimensionId && dimensions.has(id));
}

const DAY_MS = 86_400_000;
const WINDOW_DAYS: Record<TrendWindow, number> = { '7d': 7, '30d': 30, '90d': 90 };
const TIERS: readonly Tier[] = [1, 2, 3];

type Options = {
  reader: QualityStreamReader;
  now: () => number;
  /** Defaults to on. When off, rows are scored as one pool instead of per tier. */
  getTieringEnabled?: () => boolean;
};

function targetFor(tier: Tier): number {
  return TIER_TARGETS.find((entry) => entry.tier === tier)?.targetPercent ?? 0;
}

const CLASSIFICATION_KIND_ORDER: readonly string[] = ['assetClass', 'assetType', 'equipmentType', 'fileCategory'];
const CLASSIFICATION_ROW_CAP = 8;
export const OTHER_CLASSIFICATION_ID = '__other__';

/** Classification kinds present on any row, in the same order the overview lists them. */
function classificationKinds(rows: readonly DrilldownRow[]): string[] {
  const kinds = new Set(rows.flatMap((row) => Object.keys(row.classes)));
  return [...kinds].sort((left, right) => kindRank(left) - kindRank(right) || left.localeCompare(right));
}

function kindRank(kind: string): number {
  const index = CLASSIFICATION_KIND_ORDER.indexOf(kind);
  return index === -1 ? CLASSIFICATION_KIND_ORDER.length : index;
}

/** Keeps the largest rows and folds the rest into one "Other" row, so a project with hundreds of types stays readable. */
function capClassificationRows(rows: readonly ClassificationRowDto[]): ClassificationRowDto[] {
  if (rows.length <= CLASSIFICATION_ROW_CAP) {
    return [...rows];
  }
  const kept = rows.slice(0, CLASSIFICATION_ROW_CAP - 1);
  const rest = rows.slice(CLASSIFICATION_ROW_CAP - 1);
  const total = rest.reduce((sum, row) => sum + row.totalCount, 0);
  const contextualised = rest.reduce((sum, row) => sum + row.contextualisedCount, 0);
  return [
    ...kept,
    {
      id: OTHER_CLASSIFICATION_ID,
      label: 'Other',
      labelKey: 'overview.classification.other',
      totalCount: total,
      contextualisedCount: contextualised,
      coveragePercent: computeValuePercent(contextualised, total),
    },
  ];
}

/** The untiered entities of several dimensions, scored together. Null when there are none. */
function sumUntiered(parts: readonly (UntieredScoreDto | null)[]): UntieredScoreDto | null {
  const eligibleCount = parts.reduce((sum, part) => sum + (part?.eligibleCount ?? 0), 0);
  const passingCount = parts.reduce((sum, part) => sum + (part?.passingCount ?? 0), 0);
  return eligibleCount === 0
    ? null
    : { scorePercent: computeValuePercent(passingCount, eligibleCount), eligibleCount, passingCount };
}

function averageRounded(values: readonly number[]): number {
  return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
}

type TierBreakdown = {
  tierScores: TierScore[];
  overallPercent: number | null;
  statusScores: TierScore[];
  untiered: UntieredScoreDto | null;
  scoreBasis: ScoreBasis;
};

/** The one pooled score status is judged by. It reuses the tier 1 target, since there is no pooled target of its own. */
function pooledStatusScores(pooled: number | null): TierScore[] {
  return [{ tier: 1, scorePercent: pooled, targetPercent: targetFor(1), scored: pooled !== null }];
}

/** True when the rows measured nothing: every row is a not-applicable row, or there are none, so there is no score. */
function nothingEligible(rows: readonly AggregateRow[]): boolean {
  return sumRows(rows).eligible === 0;
}

function buildTierScores(rows: readonly AggregateRow[], tieringEnabled: boolean): TierBreakdown {
  if (!tieringEnabled) {
    const totals = sumRows(rows);
    const pooled = computeValuePercent(totals.passing, totals.eligible);
    return {
      tierScores: [],
      overallPercent: pooled,
      statusScores: pooledStatusScores(pooled),
      untiered: null,
      scoreBasis: 'pooled',
    };
  }
  const untieredTotals = sumRows(rows.filter((row) => row.tier === null));
  const untiered: UntieredScoreDto | null =
    untieredTotals.eligible === 0
      ? null
      : {
          scorePercent: computeValuePercent(untieredTotals.passing, untieredTotals.eligible),
          eligibleCount: untieredTotals.eligible,
          passingCount: untieredTotals.passing,
        };
  const tierScores = TIERS.map((tier) => {
    const totals = sumRows(rows.filter((row) => row.tier === tier));
    const scorePercent = computeValuePercent(totals.passing, totals.eligible);
    return {
      tier,
      scorePercent,
      targetPercent: targetFor(tier),
      scored: tier !== 3 && scorePercent !== null,
    };
  });
  const scored = tierScores.flatMap((entry) =>
    entry.scored && entry.scorePercent !== null ? [entry.scorePercent] : []
  );
  if (scored.length === 0) {
    // No tier 1 or 2 entity was measured (for example none has a tier). Rather than invent a per-tier result or drop the
    // rows, score everything together and say so (scoreBasis).
    const totals = sumRows(rows);
    const pooled = computeValuePercent(totals.passing, totals.eligible);
    return { tierScores, overallPercent: pooled, statusScores: pooledStatusScores(pooled), untiered, scoreBasis: 'pooled-fallback' };
  }
  return {
    tierScores,
    statusScores: tierScores,
    overallPercent: averageRounded(scored),
    untiered,
    scoreBasis: 'tiered',
  };
}

/** The most items of one metric a list reads (the Issues page, a dimension's lists, a drill-down). */
export const LIST_LIMIT_PER_METRIC = 200;
/** Up to this many facilities are asked for one by one; more are read together and filtered here. */
const MAX_FACILITY_QUERIES = 10;

/** The facilities to ask for one by one, or null to read all rows of a metric and filter them here. */
function issueFacilities(filters: SliceFilters): readonly string[] | null {
  const named = filters.facilityId === undefined ? filters.facilityIds : [filters.facilityId];
  if (named === undefined) {
    return null;
  }
  const inLocation = filters.facilityId === undefined ? named : named.filter((id) => filters.facilityIds?.includes(id) ?? true);
  return inLocation.length <= MAX_FACILITY_QUERIES ? inLocation : null;
}

/** A location's facilities share the limit, so the page reads about as many rows as for one facility. */
function perFacilityLimit(count: number): number {
  return Math.max(25, Math.ceil(LIST_LIMIT_PER_METRIC / Math.max(count, 1)));
}

export class CdfQualityDashboardService implements QualityDashboardService {
  private readonly reader: QualityStreamReader;
  private readonly now: () => number;
  private readonly getTieringEnabled: () => boolean;

  constructor(options: Options) {
    this.reader = options.reader;
    this.now = options.now;
    this.getTieringEnabled = options.getTieringEnabled ?? (() => true);
  }

  async getOverview(filters: SliceFilters): Promise<OverviewDto> {
    const { current, aggregates: allRows, exclusions: exclusionRows } = await this.currentRows();
    const rows = allRows.filter((row) => matchesRowSlice(row, filters));
    const tieringEnabled = this.getTieringEnabled();

    let entityCount = 0;
    const dimensionCards: DimensionCardDto[] = ALL_DIMENSION_IDS.map((dimensionId) => {
      const optional = isOptionalDimension(dimensionId);
      const headlineRows = selectHeadlineRows(rows, dimensionId);
      const hasData = allRows.some((row) => row.dimensionId === dimensionId);
      if (!hasData) {
        return {
          dimensionId,
          nameKey: `dimension.${dimensionId}`,
          enabled: false,
          optional,
          overallPercent: null,
          tierScores: [],
          untiered: null,
          scoreBasis: tieringEnabled ? 'tiered' : 'pooled',
          issueCount: null,
          status: 'disabled',
          weakestTier: null,
        };
      }
      const totals = sumRows(headlineRows);
      entityCount += totals.eligible;
      if (nothingEligible(headlineRows)) {
        // The dimension has rows but nothing was measured: no score, no issue count and, above all, not "good".
        return {
          dimensionId,
          nameKey: `dimension.${dimensionId}`,
          enabled: true,
          optional,
          overallPercent: null,
          tierScores: [],
          untiered: null,
          scoreBasis: tieringEnabled ? 'tiered' : 'pooled',
          issueCount: null,
          status: 'not-applicable',
          weakestTier: null,
        };
      }
      const { tierScores, statusScores, overallPercent, untiered, scoreBasis } = buildTierScores(
        headlineRows,
        tieringEnabled
      );
      return {
        dimensionId,
        nameKey: `dimension.${dimensionId}`,
        enabled: true,
        optional,
        overallPercent,
        tierScores,
        untiered,
        scoreBasis,
        issueCount: totals.issue,
        status: computeDimensionStatus(statusScores),
        weakestTier: findWeakestTier(tierScores),
        eligibleCount: totals.eligible,
        passingCount: totals.passing,
      };
    });

    const enabledCards = dimensionCards.filter((card) => card.enabled && card.status !== 'not-applicable');
    // each card says when it was measured: a dimension is as current as the latest run that measured it
    for (const card of dimensionCards) {
      const measured = current.byDimension.get(card.dimensionId);
      if (measured !== undefined) {
        card.measuredAt = measured.runTimestamp;
      }
    }
    const portfolioTierScores: TierScore[] = TIERS.map((tier) => {
      const scores = enabledCards.flatMap((card) => {
        const score = card.tierScores.find((entry) => entry.tier === tier)?.scorePercent;
        return score === null || score === undefined ? [] : [score];
      });
      return {
        tier,
        scorePercent: scores.length === 0 ? null : averageRounded(scores),
        targetPercent: targetFor(tier),
        scored: tier !== 3,
      };
    });

    const scopedExclusions = exclusionRows.filter((row) => matchesRowSlice(row, filters));

    return {
      overallAggregatePercent: entityCount === 0 ? null : computePortfolioAggregatePercent(dimensionCards),
      // The portfolio trend and the headline snapshots are not read here: nothing on the Overview shows them, and they
      // cost one query per headline metric. Each dimension card reads its own trend.
      overallTrend: null,
      tierScores: entityCount === 0 || !tieringEnabled ? [] : portfolioTierScores,
      untiered: tieringEnabled ? sumUntiered(enabledCards.map((card) => card.untiered)) : null,
      scope: {
        entityCount,
        facilityId: filters.facilityId,
        divisionId: filters.divisionId,
        disciplineId: filters.disciplineId,
        ownerId: filters.ownerId,
      },
      dimensionCards,
      excludedSummary: buildExclusionBuckets(scopedExclusions).filter((bucket) => bucket.source === 'system'),
      userExclusionSummary: buildUserExclusionSummary(scopedExclusions),
      // Insights and alerts are not produced by the metrics stream yet.
      insights: [],
      insightIds: [],
      alertSummaries: [],
      headlineMetrics: TREND_HEADLINE_METRIC_IDS.map((metricId) => ({
        metricId,
        labelKey: `metric.${metricId}`,
      })),
      headlineSnapshots: [],
    };
  }

  async getDimension(dimensionId: DimensionId, filters: SliceFilters): Promise<DimensionDto> {
    const run = await this.runOf(dimensionId);
    const [allRows, exclusionRows] =
      run === null
        ? [[], []]
        : await Promise.all([this.reader.aggregates(run.runId), this.reader.exclusions(run.runId)]);
    const optional = isOptionalDimension(dimensionId);
    const nameKey = `dimension.${dimensionId}`;
    const dimensionRows = allRows.filter((row) => row.dimensionId === dimensionId);
    const rows = dimensionRows.filter((row) => matchesRowSlice(row, filters));
    const scopedExclusions = exclusionRows.filter(
      (row) => row.dimensionId === dimensionId && matchesRowSlice(row, filters)
    );
    const excludedBuckets = buildExclusionBuckets(scopedExclusions);
    const userExclusionSummary = buildUserExclusionSummary(scopedExclusions);

    if (dimensionRows.length === 0) {
      return {
        dimensionId,
        nameKey,
        enabled: false,
        optional,
        status: 'disabled',
        entityCount: null,
        tierScores: [],
        untiered: null,
        scoreBasis: this.getTieringEnabled() ? 'tiered' : 'pooled',
        overallPercent: null,
        aggregateRows: [],
        excludedBuckets,
        userExclusionSummary,
        insightIds: [],
        extendedMetricsAvailable: false,
      };
    }

    const headlineRows = selectHeadlineRows(rows, dimensionId);
    const entityCount = sumRows(headlineRows).eligible;
    const notApplicable = nothingEligible(headlineRows);
    const { tierScores, statusScores, overallPercent, untiered, scoreBasis } = buildTierScores(
      headlineRows,
      this.getTieringEnabled()
    );

    return {
      dimensionId,
      nameKey,
      enabled: true,
      optional,
      status: notApplicable ? 'not-applicable' : computeDimensionStatus(statusScores),
      entityCount,
      tierScores: notApplicable ? [] : tierScores,
      untiered,
      scoreBasis,
      overallPercent: notApplicable ? null : overallPercent,
      aggregateRows: this.aggregateRows(rows, run?.warnings ?? []),
      excludedBuckets,
      userExclusionSummary,
      insightIds: [],
      extendedMetricsAvailable: dimensionRows.some(
        (row) => isMetricId(row.metricId) && isExtendedMetric(row.metricId)
      ),
    };
  }

  async getLifecycleWarnings(dimensionId: DimensionId, filters: SliceFilters): Promise<DrilldownSummaryDto[]> {
    const metricIds = DRILLDOWN_METRIC_IDS.filter(
      (metricId) => getMetricDimension(metricId) === dimensionId && isLifecycleMetric(metricId)
    );
    if (metricIds.length === 0) {
      return [];
    }
    const run = await this.runOf(dimensionId);
    if (run === null) {
      return [];
    }
    const sample = await this.sampleDrilldown(run.runId, metricIds, filters);
    return this.drilldownSummaries(sample.rows.filter((row) => matchesRowSlice(row, filters))).map((summary) => ({
      ...summary,
      atLeast: sample.cutMetricIds.includes(summary.metricId),
    }));
  }

  async getDrilldown(metricId: MetricId, filters: SliceFilters): Promise<DrilldownDto> {
    // A link metric lists its unlinked entities from its drill-down metric; the list stays under the metric opened.
    const source = drilldownMetricFor(metricId) ?? metricId;
    const run = await this.runOf(getMetricDimension(source));
    const sample =
      run === null
        ? { rows: [], cut: false, cutMetricIds: [] }
        : await this.sampleDrilldown(run.runId, [source], filters);
    const rows = sample.rows.filter((row) => matchesRowSlice(row, filters));
    return {
      partial: sample.cut ? { limit: LIST_LIMIT_PER_METRIC } : null,
      metricId,
      dimensionId: getMetricDimension(metricId),
      labelKey: `metric.${metricId}`,
      actionKey: getMetricActionKey(metricId),
      ownerId: rows[0]?.ownerId ?? null,
      columnKeys: ['entity', 'tier', ...classificationKinds(rows), 'detail', 'owner'],
      rows: rows.map((row) => ({
        id: `${metricId}:${row.entityId}`,
        tier: row.tier,
        ownerId: row.ownerId ?? '',
        classes: row.classes,
        cells: {
          entity: row.entityLabel,
          // Empty for an entity with no tier: the table says "Unassigned" itself, so no tier is invented here.
          tier: row.tier === null ? '' : String(row.tier),
          ...row.classes,
          detail: row.detail,
          owner: row.ownerId ?? '',
        },
      })),
    };
  }

  async getSnapshots(
    metricId: MetricId,
    window: TrendWindow,
    filters: SliceFilters,
    subKey?: string
  ): Promise<SnapshotPointDto[]> {
    const history = await this.reader.history(metricId);
    const cutoff = this.now() - WINDOW_DAYS[window] * DAY_MS;
    const byRun = new Map<string, AggregateRow[]>();
    for (const row of history) {
      if (!matchesRowSlice(row, filters)) {
        continue;
      }
      byRun.set(row.runId, [...(byRun.get(row.runId) ?? []), row]);
    }
    const points: SnapshotPointDto[] = [];
    for (const runRows of byRun.values()) {
      const first = runRows[0];
      const selected = selectMetricRows(runRows, metricId, subKey);
      const valuePercent = metricPercent(metricId, sumRows(selected));
      if (first === undefined || valuePercent === null || Date.parse(first.runTimestamp) < cutoff) {
        continue;
      }
      points.push({
        metricId,
        capturedAt: first.runTimestamp,
        valuePercent,
        scope: {
          facilityId: filters.facilityId,
          divisionId: filters.divisionId,
          disciplineId: filters.disciplineId,
          ownerId: filters.ownerId,
        },
      });
    }
    return points.sort((left, right) => Date.parse(left.capturedAt) - Date.parse(right.capturedAt));
  }

  async getAlerts(_filters: SliceFilters): Promise<AlertsDto> {
    // Alerts are not produced by the metrics stream yet.
    return { alerts: [], summary: { total: 0, critical: 0, warning: 0, informational: 0 } };
  }

  async getIssues(filters: SliceFilters): Promise<IssuesDto> {
    const metricIds = this.getIssueMetricIds();
    const parts = await Promise.all(metricIds.map((metricId) => this.getIssuesOfMetric(metricId, filters)));
    const issues = parts.flatMap((part) => part.issues);
    const cutMetricIds = metricIds.filter((_, index) => parts[index]?.cut === true);
    return {
      issues,
      summary: {
        total: issues.length,
        critical: issues.filter((issue) => issue.tier === 1).length,
        important: issues.filter((issue) => issue.tier === 2).length,
        standard: issues.filter((issue) => issue.tier === 3).length,
        unassigned: issues.filter((issue) => issue.tier === null).length,
      },
      partial:
        cutMetricIds.length > 0
          ? { limitPerMetric: LIST_LIMIT_PER_METRIC, metricCount: metricIds.length, cutMetricIds }
          : null,
    };
  }

  getIssueMetricIds(): readonly MetricId[] {
    return DRILLDOWN_METRIC_IDS.filter((metricId) => ISSUE_KIND_BY_METRIC[metricId] !== undefined);
  }

  async getIssuesOfMetric(metricId: MetricId, filters: SliceFilters): Promise<MetricIssuesDto> {
    const run = await this.runOf(getMetricDimension(metricId));
    if (run === null) {
      return { issues: [], cut: false, limit: LIST_LIMIT_PER_METRIC };
    }
    const sample = await this.sampleDrilldown(run.runId, [metricId], filters);
    const issues = sample.rows
      .filter((row) => matchesRowSlice(row, filters))
      .flatMap((row) => {
        const issue = this.toIssue(row);
        return issue === null ? [] : [issue];
      });
    return { issues, cut: sample.cut, limit: LIST_LIMIT_PER_METRIC };
  }

  async getClassifications(filters: SliceFilters): Promise<ClassificationsDto> {
    const rows = (await this.currentClassifications()).filter((row) => matchesRowSlice(row, filters));
    const byKind = new Map<string, Map<string, { name: string; eligible: number; passing: number }>>();
    for (const row of rows) {
      const kindRows = byKind.get(row.kind) ?? new Map();
      const entry = kindRows.get(row.classificationId) ?? { name: row.classificationName, eligible: 0, passing: 0 };
      entry.eligible += row.eligibleCount;
      entry.passing += row.passingCount;
      kindRows.set(row.classificationId, entry);
      byKind.set(row.kind, kindRows);
    }
    const groups = [...byKind.entries()].map(([kind, entries]): ClassificationGroupDto => {
      const all = [...entries.entries()]
        .map(([id, entry]): ClassificationRowDto => ({
          id,
          label: entry.name,
          totalCount: entry.eligible,
          contextualisedCount: entry.passing,
          coveragePercent: computeValuePercent(entry.passing, entry.eligible),
        }))
        .filter((row) => row.totalCount > 0)
        .sort((left, right) => right.totalCount - left.totalCount || left.label.localeCompare(right.label));
      return { kind, rows: capClassificationRows(all) };
    });
    return {
      groups: groups
        .filter((group) => group.rows.length > 0)
        .sort((left, right) => kindRank(left.kind) - kindRank(right.kind) || left.kind.localeCompare(right.kind)),
    };
  }

  async getConfiguredMetrics(filters: SliceFilters): Promise<ConfiguredMetricsDto> {
    const rows = (await this.reader.currentExtraLinks()).rows.filter((row) => matchesRowSlice(row, filters));
    const groups = new Map<string, ConfiguredGroupDto>();
    const totals = new Map<string, { total: number; linked: number }>();
    for (const row of rows) {
      const groupId = `${row.model}/${row.view}`;
      const group = groups.get(groupId) ?? {
        id: groupId,
        modelId: row.model,
        modelLabel: row.modelLabel,
        viewLabel: row.view,
        metrics: [],
      };
      const key = `${groupId}.${row.property}`;
      const sum = totals.get(key) ?? { total: 0, linked: 0 };
      sum.total += row.eligibleCount;
      sum.linked += row.passingCount;
      if (!totals.has(key)) {
        group.metrics.push({
          label: row.property,
          modelId: row.model,
          view: row.view,
          totalCount: 0,
          linkedCount: 0,
          coveragePercent: null,
        });
      }
      totals.set(key, sum);
      groups.set(groupId, group);
    }
    const builtGroups = [...groups.values()].map((group) => ({
      ...group,
      metrics: group.metrics.map((metric) => {
        const sum = totals.get(`${group.id}.${metric.label}`) ?? { total: 0, linked: 0 };
        return {
          ...metric,
          totalCount: sum.total,
          linkedCount: sum.linked,
          coveragePercent: computeValuePercent(sum.linked, sum.total),
        };
      }),
    }));
    return {
      models: groupConfiguredMetricsByModel(builtGroups),
    };
  }

  async getConfiguredDrilldown(key: ConfiguredMetricKey, filters: SliceFilters): Promise<ConfiguredDrilldownDto> {
    const link = { model: key.modelId, view: key.view, property: key.property };
    const current = await this.reader.currentExtraLinks();
    const run = current.runOf.get(extraLinkKey(link));
    if (run === undefined) {
      return { key, rows: [], cutAt: null, dropped: 0 };
    }
    const counted = current.rows.filter(
      (row) => row.model === link.model && row.view === link.view && row.property === link.property
    );
    const facilityIds = [...new Set(counted.map((row) => row.facilityId))];
    const rows = (await this.reader.extraLinkGaps(run.runId, link, facilityIds))
      .filter((row) => matchesRowSlice(row, filters))
      .map((row) => ({
        id: `${row.unit}:${row.entityId}`,
        entity: row.entityLabel,
        entityId: row.entityId,
        space: row.unit,
        facilityId: row.facilityId,
        detail: row.detail,
      }))
      .sort((left, right) => left.entity.localeCompare(right.entity) || left.id.localeCompare(right.id));
    const cut = run.warnings.filter(
      (warning) => warning.code === 'drilldown-truncated' && warning.subject === `${key.view}.${key.property}`
    );
    return {
      key,
      rows,
      cutAt: cut.length === 0 ? null : (cut[0]?.counts.limit ?? null),
      dropped: cut.reduce((sum, warning) => sum + (warning.counts.dropped ?? 0), 0),
    };
  }

  async getConfiguredMetricTrends(
    key: ConfiguredMetricKey,
    window: TrendWindow,
    filters: SliceFilters
  ): Promise<ConfiguredMetricTrendPointDto[]> {
    const history = await this.reader.extraLinkHistory();
    const cutoff = this.now() - WINDOW_DAYS[window] * DAY_MS;
    const byRun = new Map<string, { runTimestamp: string; linked: number; total: number }>();
    for (const row of history) {
      if (row.model !== key.modelId || row.view !== key.view || row.property !== key.property) {
        continue;
      }
      if (!matchesRowSlice(row, filters)) {
        continue;
      }
      const sum = byRun.get(row.runId) ?? { runTimestamp: row.runTimestamp, linked: 0, total: 0 };
      sum.linked += row.passingCount;
      sum.total += row.eligibleCount;
      byRun.set(row.runId, sum);
    }
    const points: ConfiguredMetricTrendPointDto[] = [];
    for (const sum of byRun.values()) {
      const valuePercent = computeValuePercent(sum.linked, sum.total);
      if (valuePercent === null || Date.parse(sum.runTimestamp) < cutoff) {
        continue;
      }
      points.push({
        capturedAt: sum.runTimestamp,
        valuePercent,
        linkedCount: sum.linked,
        totalCount: sum.total,
      });
    }
    return points.sort((left, right) => Date.parse(left.capturedAt) - Date.parse(right.capturedAt));
  }

  async getLatestRunConfiguration(): Promise<RunConfigurationDto | null> {
    const current = await this.reader.currentRuns().catch((error: unknown) => {
      if (isBackendError(error) && error.kind === 'no-completed-run') {
        return null;
      }
      throw error;
    });
    if (current === null) {
      return null;
    }
    // the configuration is every configured metric as it is current, even when later runs measured other things
    const { rows, runOf } = await this.reader.currentExtraLinks();
    const runTimestamp = [...runOf.values()].reduce(
      (latest, run) => (run.runTimestamp > latest ? run.runTimestamp : latest),
      (current.configured ?? current.latest).runTimestamp
    );
    const byModel = new Map<string, Map<string, Set<string>>>();
    for (const row of rows) {
      const views = byModel.get(row.model) ?? new Map<string, Set<string>>();
      views.set(row.view, (views.get(row.view) ?? new Set<string>()).add(row.property));
      byModel.set(row.model, views);
    }
    const sorted = (values: Iterable<string>): string[] => [...values].sort((a, b) => a.localeCompare(b));
    return {
      runTimestamp,
      models: sorted(byModel.keys()).map((model) => ({
        model,
        views: sorted(byModel.get(model)?.keys() ?? []).map((view) => ({
          view,
          properties: sorted(byModel.get(model)?.get(view) ?? []),
        })),
      })),
    };
  }

  async getRunStatus(): Promise<RunStatusDto> {
    const current = await this.reader.currentRuns();
    const dimensionRuns: Partial<Record<DimensionId, string>> = {};
    for (const [dimensionId, run] of current.byDimension) {
      dimensionRuns[dimensionId] = run.runTimestamp;
    }
    return {
      lastSuccessAt: current.latest.runTimestamp,
      inProgress: false,
      scope: { kind: 'all' },
      dimensionRuns,
      ...(current.configured === null ? {} : { configuredRunAt: current.configured.runTimestamp }),
    };
  }

  async getRunWarnings(): Promise<RunWarningsDto> {
    const run = await this.reader.latestRun();
    return { runTimestamp: run.runTimestamp, warnings: run.warnings };
  }

  async refresh(): Promise<RunStatusDto> {
    // This only re-reads the latest completed run. Starting a backend run is not supported (see rerun).
    this.reader.invalidate();
    return this.getRunStatus();
  }

  async rerun(_scope: { dimensionId: DimensionId } | { metricId: MetricId }): Promise<RunStatusDto> {
    throw new BackendError('unsupported-operation', 'Starting a metrics run is not available from the dashboard.', {
      subject: 'rerun',
    });
  }

  async notifyOwner(_input: NotifyOwnerInput): Promise<NotifyOwnerResult> {
    throw new BackendError('unsupported-operation', 'Notifying an owner is not available from the dashboard.', {
      subject: 'notifyOwner',
    });
  }

  async getCustomQualityMetrics(_filters: SliceFilters): Promise<readonly CustomQualityMetricDto[]> {
    return [];
  }

  async getCustomRuleDrilldown(ruleId: string, _filters: SliceFilters): Promise<CustomRuleDrilldownDto> {
    throw new BackendError(
      'unsupported-operation',
      `Custom quality rule ${ruleId} is not available from the metrics stream.`,
      { subject: 'customRule' }
    );
  }

  private aggregateRows(rows: readonly AggregateRow[], warnings: readonly RunWarningDto[]): AggregateMetricRowDto[] {
    const metricIds: MetricId[] = [];
    for (const row of rows) {
      if (isMetricId(row.metricId) && !metricIds.includes(row.metricId)) {
        metricIds.push(row.metricId);
      }
    }
    const entries = metricIds.flatMap((metricId): { metricId: MetricId; subKey: string | null }[] => {
      if (!isSplitBySubKey(metricId)) {
        return [{ metricId, subKey: null }];
      }
      const present = rows.filter((row) => row.metricId === metricId).map((row) => row.subKey);
      return splitMetricFields(metricId, present).map((subKey) => ({ metricId, subKey }));
    });
    return entries.map(({ metricId, subKey }) => {
      const metricRows = selectMetricRows(rows, metricId, subKey ?? undefined);
      const totals = sumRows(metricRows);
      const notApplicable = totals.eligible === 0;
      const valuePercent = metricPercent(metricId, totals);
      const direction = metricDirection(metricId);
      const fieldDrilldown = subKey === null ? null : drilldownMetricForField(metricId, subKey);
      return {
        metricId,
        subKey,
        drilldownMetricId: fieldDrilldown,
        labelKey: subKey === null ? `metric.${metricId}` : `metric.${metricId}.${subKey}`,
        direction,
        eligibleCount: totals.eligible,
        passingCount: totals.passing,
        issueCount: totals.issue,
        valuePercent,
        tier: 'all',
        extended: isExtendedMetric(metricId),
        hasDrilldown: subKey === null ? hasDrilldownList(metricId) : fieldDrilldown !== null,
        actionKey: getMetricActionKey(metricId),
        status: notApplicable ? 'not-applicable' : computeMetricStatus(direction, valuePercent, totals.issue),
        notApplicable,
        notApplicableReason: notApplicable
          ? notApplicableReasonFor(warnings, metricId, metricRows.find((row) => row.subKey !== null)?.subKey ?? undefined)
          : null,
      };
    });
  }

  /** The run a dimension is current as of: the latest completed run that measured it, or null when none did. */
  private async runOf(dimensionId: DimensionId): Promise<RunInfo | null> {
    return (await this.reader.currentRuns()).byDimension.get(dimensionId) ?? null;
  }

  /**
   * Every dimension's rows, each from the run that dimension is current as of. A run is read once, and only the rows of
   * the dimensions it is current for are kept: a later run of one dimension must not hide the others.
   */
  private async currentRows(): Promise<{ current: CurrentRuns; aggregates: AggregateRow[]; exclusions: ExclusionRow[] }> {
    const current = await this.reader.currentRuns();
    const dimensionsOfRun = dimensionsByRun(current);
    const parts = await Promise.all(
      [...dimensionsOfRun.values()].map(async ({ run, dimensions }) => {
        const [aggregates, exclusions] = await Promise.all([
          this.reader.aggregates(run.runId),
          this.reader.exclusions(run.runId),
        ]);
        return {
          aggregates: aggregates.filter((row) => isDimensionIn(dimensions, row.dimensionId)),
          exclusions: exclusions.filter((row) => isDimensionIn(dimensions, row.dimensionId)),
        };
      })
    );
    return {
      current,
      aggregates: parts.flatMap((part) => part.aggregates),
      exclusions: parts.flatMap((part) => part.exclusions),
    };
  }

  /** The classification rows of every dimension, each from the run its dimension is current as of. */
  private async currentClassifications(): Promise<ClassificationRow[]> {
    const current = await this.reader.currentRuns();
    const dimensionsOfRun = dimensionsByRun(current);
    const parts = await Promise.all(
      [...dimensionsOfRun.values()].map(async ({ run, dimensions }) =>
        (await this.reader.classifications(run.runId)).filter(
          // a row that does not name its dimension was written by a run that measured every dimension
          (row) => row.dimensionId === undefined || isDimensionIn(dimensions, row.dimensionId)
        )
      )
    );
    return parts.flat();
  }

  private drilldownSummaries(rows: readonly DrilldownRow[]): DrilldownSummaryDto[] {
    return DRILLDOWN_METRIC_IDS.flatMap((metricId) => {
      const count = rows.filter((row) => row.metricId === metricId).length;
      return count === 0
        ? []
        : [{ metricId, labelKey: `metric.${metricId}`, count, actionKey: getMetricActionKey(metricId) }];
    });
  }

  /**
   * The drill-down rows of some metrics, each up to `LIST_LIMIT_PER_METRIC`, and whether any metric had more. Every item
   * of every metric would be too much for one page (a metric holds up to the list cap per space), so lists are partial.
   * A facility, or the few facilities of a location, narrow the rows on the server so the limit is spent inside the scope.
   */
  private async sampleDrilldown(
    runId: string,
    metricIds: readonly MetricId[],
    filters: SliceFilters
  ): Promise<{ rows: DrilldownRow[]; cut: boolean; cutMetricIds: MetricId[] }> {
    const facilities = issueFacilities(filters);
    const limit = facilities === null ? LIST_LIMIT_PER_METRIC : perFacilityLimit(facilities.length);
    const samples = await Promise.all(
      metricIds.flatMap((metricId) =>
        (facilities ?? [undefined]).map(async (facilityId) => ({
          metricId,
          ...(await this.reader.drilldownSample(runId, metricId, limit, facilityId)),
        }))
      )
    );
    const cutMetricIds = [...new Set(samples.filter((sample) => sample.cut).map((sample) => sample.metricId))];
    return { rows: samples.flatMap((sample) => sample.rows), cut: cutMetricIds.length > 0, cutMetricIds };
  }

  private toIssue(row: DrilldownRow): IssueDto | null {
    if (!isMetricId(row.metricId)) {
      return null;
    }
    const kind = ISSUE_KIND_BY_METRIC[row.metricId];
    if (kind === undefined) {
      return null;
    }
    return {
      id: `${row.metricId}:${row.entityId}`,
      kind,
      dimensionId: getMetricDimension(row.metricId),
      metricId: row.metricId,
      entityId: row.entityId,
      entityLabel: row.entityLabel,
      tier: row.tier,
      facilityId: row.facilityId ?? '',
      ownerId: row.ownerId ?? '',
      detail: row.detail,
      actionKey: getMetricActionKey(row.metricId),
      classes: row.classes,
    };
  }
}
