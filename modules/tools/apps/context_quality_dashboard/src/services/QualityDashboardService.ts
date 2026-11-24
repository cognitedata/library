import type {
  AlertsDto,
  ClassificationsDto,
  ConfiguredMetricKey,
  ConfiguredDrilldownDto,
  ConfiguredMetricTrendPointDto,
  ConfiguredMetricsDto,
  RunConfigurationDto,
  IssuesDto,
  MetricIssuesDto,
  CustomQualityMetricDto,
  CustomRuleDrilldownDto,
  DimensionDto,
  DimensionId,
  DrilldownDto,
  DrilldownSummaryDto,
  MetricId,
  NotifyOwnerInput,
  NotifyOwnerResult,
  OverviewDto,
  RunStatusDto,
  RunWarningsDto,
  SliceFilters,
  SnapshotPointDto,
  TrendWindow,
} from '../types';

export interface QualityDashboardService {
  getOverview(filters: SliceFilters): Promise<OverviewDto>;
  getDimension(dimensionId: DimensionId, filters: SliceFilters): Promise<DimensionDto>;
  /**
   * The lifecycle lists (stale links, broken links and so on) of a dimension that have items. Loaded apart from the
   * dimension so the page does not wait for it.
   */
  getLifecycleWarnings(dimensionId: DimensionId, filters: SliceFilters): Promise<DrilldownSummaryDto[]>;
  getDrilldown(metricId: MetricId, filters: SliceFilters): Promise<DrilldownDto>;
  getSnapshots(
    metricId: MetricId,
    window: TrendWindow,
    filters: SliceFilters,
    /** For a metric reported per field: which field. */
    subKey?: string
  ): Promise<SnapshotPointDto[]>;
  getAlerts(filters: SliceFilters): Promise<AlertsDto>;
  getIssues(filters: SliceFilters): Promise<IssuesDto>;
  /** The metrics whose items are issues, in the order the Issues page lists them. */
  getIssueMetricIds(): readonly MetricId[];
  /** One metric's issues, so the Issues page can load the metrics separately and show each as it arrives. */
  getIssuesOfMetric(metricId: MetricId, filters: SliceFilters): Promise<MetricIssuesDto>;
  getClassifications(filters: SliceFilters): Promise<ClassificationsDto>;
  /** The metrics the latest run measured because a config file named them. No groups when it had none. */
  getConfiguredMetrics(filters: SliceFilters): Promise<ConfiguredMetricsDto>;
  /** Coverage history for one configured metric across completed runs. */
  /** The instances of a configured view whose link is not set, as listed by the latest run. */
  getConfiguredDrilldown(key: ConfiguredMetricKey, filters: SliceFilters): Promise<ConfiguredDrilldownDto>;
  getConfiguredMetricTrends(
    key: ConfiguredMetricKey,
    window: TrendWindow,
    filters: SliceFilters
  ): Promise<ConfiguredMetricTrendPointDto[]>;
  /** What the latest run was started with, read from the run itself. Null when no run has completed yet. */
  getLatestRunConfiguration(): Promise<RunConfigurationDto | null>;
  getRunStatus(): Promise<RunStatusDto>;
  /** What the latest completed run reported about itself. Empty when the run carries no warnings. */
  getRunWarnings(): Promise<RunWarningsDto>;
  refresh(): Promise<RunStatusDto>;
  rerun(
    scope: { dimensionId: DimensionId } | { metricId: MetricId }
  ): Promise<RunStatusDto>;
  notifyOwner(input: NotifyOwnerInput): Promise<NotifyOwnerResult>;
  getCustomQualityMetrics(filters: SliceFilters): Promise<readonly CustomQualityMetricDto[]>;
  getCustomRuleDrilldown(ruleId: string, filters: SliceFilters): Promise<CustomRuleDrilldownDto>;
}
