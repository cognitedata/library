import { interpolate } from '../../i18n/interpolate';
import type { AggregateMetricRowDto, DimensionDto, DimensionId, MetricId } from '../../types';
import { formatCount } from '../overview/contextualisationCoverageLabels';
import { primaryTrendMetricForDimension } from '../overview/trendMetricPresentation';

export function metricDescriptionKey(metricId: MetricId): string {
  return `dimension.metric.description.${metricId}`;
}

/** Metric cards omit the primary headline metric (already shown in the summary card). */
export function aggregateRowsForMetricCards(
  dimensionId: DimensionId,
  rows: readonly AggregateMetricRowDto[]
): AggregateMetricRowDto[] {
  const primaryId = primaryTrendMetricForDimension(dimensionId);
  return rows.filter((row) => row.metricId !== primaryId);
}

/** Headline readiness for a dimension: passing / eligible on its primary metric. */
export function headlineMetricRow(dimension: DimensionDto): AggregateMetricRowDto | undefined {
  const primaryId = primaryTrendMetricForDimension(dimension.dimensionId);
  const primary = dimension.aggregateRows.find(
    (row) => row.metricId === primaryId && row.tier === 'all' && !row.notApplicable
  );
  if (primary !== undefined) {
    return primary;
  }
  return dimension.aggregateRows.find((row) => row.tier === 'all' && !row.notApplicable);
}

export function formatMetricReadiness(
  metric: AggregateMetricRowDto,
  t: (key: string) => string
): string | null {
  if (metric.notApplicable) {
    return null;
  }
  const orphanText = orphanRateCountText(metric, t);
  if (orphanText !== null) {
    return orphanText;
  }
  return interpolate(t('overview.dimensionCard.population'), {
    contextualised: formatCount(metric.passingCount),
    total: formatCount(metric.eligibleCount),
  });
}

export function formatUntieredReadiness(
  passingCount: number,
  eligibleCount: number,
  t: (key: string) => string
): string {
  return interpolate(t('overview.dimensionCard.population'), {
    contextualised: formatCount(passingCount),
    total: formatCount(eligibleCount),
  });
}

function orphanRateCountText(metric: AggregateMetricRowDto, t: (key: string) => string): string | null {
  if (metric.metricId !== 'orphanRate') {
    return null;
  }
  const key =
    metric.issueCount === 1 ? 'dimension.metric.orphanCount.one' : 'dimension.metric.orphanCount.other';
  return interpolate(t(key), { count: String(metric.issueCount), total: String(metric.eligibleCount) });
}

export function canOpenMetricDrilldown(metric: AggregateMetricRowDto): boolean {
  return metric.hasDrilldown && !metric.notApplicable;
}

/** Subheading under `dimension.noDrilldown` when the metric is measured but has no entity list. */
export function metricDrilldownUnavailableDetail(
  metric: AggregateMetricRowDto,
  t: (key: string) => string
): string | null {
  if (metric.notApplicable || metric.hasDrilldown) {
    return null;
  }
  return t('dimension.openDrilldown.disabled.noEntityList');
}

export function showsMetricDrilldownUnavailableCallout(metric: AggregateMetricRowDto): boolean {
  return !metric.notApplicable && !metric.hasDrilldown;
}

export function metricDrilldownDisabledTooltipMessage(
  metric: AggregateMetricRowDto,
  t: (key: string) => string
): string | null {
  if (canOpenMetricDrilldown(metric)) {
    return null;
  }
  if (metric.notApplicable) {
    return (
      formatMetricNotApplicableReason(metric.notApplicableReason, t) ?? t('dimension.metric.notApplicable')
    );
  }
  return metricDrilldownUnavailableDetail(metric, t);
}

export function formatMetricNotApplicableReason(
  reason: AggregateMetricRowDto['notApplicableReason'],
  t: (key: string) => string
): string | null {
  if (reason === null) {
    return null;
  }
  if (reason.kind === 'mapping-empty') {
    return reason.mapping === null
      ? t('dimension.metric.notApplicable.mapping-empty.generic')
      : interpolate(t('dimension.metric.notApplicable.mapping-empty'), { mapping: reason.mapping });
  }
  return t(`dimension.metric.notApplicable.${reason.kind}`);
}

const metricCardDrilldownButtonBaseClassName = 'h-7 w-fit shrink-0 px-0';

export function metricCardDrilldownButtonClassName(canOpen: boolean): string {
  if (canOpen) {
    return `${metricCardDrilldownButtonBaseClassName} font-medium text-foreground hover:bg-secondary-background hover:text-foreground`;
  }
  return `${metricCardDrilldownButtonBaseClassName} text-muted-foreground hover:text-foreground disabled:opacity-50`;
}
