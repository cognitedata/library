
import { Badge } from '@cognite/aura/components/badge';
import { Button } from '@cognite/aura/components/button';
import { Card, CardContent } from '@cognite/aura/components/card';
import { IconChevronRight } from '@cognite/aura/icons';

import type { ConfiguredMetricDto, ConfiguredMetricTrendPointDto } from '../../types';
import { metricCardDrilldownButtonClassName } from '../dimension/dimensionMetricPresentation';

import { configuredMetricChartId } from './configuredMetricKey';
import { configuredMetricTrendToSnapshots } from './configuredMetricTrendSnapshots';
import {
  buildCoverageBreakdownAriaLabel,
  formatCount,
} from './contextualisationCoverageLabels';
import { DimensionCardTrendFooter } from './DimensionCardTrendFooter';
import { DimensionPopulationBar } from './DimensionPopulationBar';
import { DimensionPopulationLegend } from './DimensionPopulationLegend';
import { hasConfiguredMetricTrendHistory } from './useConfiguredMetricTrends';

export type ConfiguredMetricRowVariant = 'overview' | 'dimension';

type ConfiguredMetricRowProps = {
  groupId: string;
  viewLabel: string;
  metric: ConfiguredMetricDto;
  trendPoints: ConfiguredMetricTrendPointDto[];
  trendLoading: boolean;
  onOpenDrilldown: () => void;
  t: (key: string) => string;
  /** Overview dashboard: aside trend + view → property title. Dimensions tab: dimension-style stacked cards. */
  variant?: ConfiguredMetricRowVariant;
};

export function ConfiguredMetricRow({
  groupId,
  viewLabel,
  metric,
  trendPoints,
  trendLoading,
  onOpenDrilldown,
  t,
  variant = 'overview',
}: ConfiguredMetricRowProps) {
  const metricTitle = interpolate(t('overview.configured.metricTitle'), {
    view: viewLabel,
    property: metric.label,
  });
  const remaining =
    metric.totalCount > metric.linkedCount ? metric.totalCount - metric.linkedCount : 0;
  const coveragePercentLabel =
    metric.coveragePercent === null
      ? null
      : interpolate(t('overview.dimensionCard.coveragePercent'), {
          percent: String(metric.coveragePercent),
        });
  const chartId = configuredMetricChartId({
    modelId: metric.modelId,
    view: metric.view,
    property: metric.label,
  });
  const historyPoints = hasConfiguredMetricTrendHistory(trendPoints) ? trendPoints : [];
  const snapshotPoints = configuredMetricTrendToSnapshots(historyPoints);
  const canOpenDrilldown = metric.totalCount > 0 && remaining > 0;

  const cardClassName =
    variant === 'dimension'
      ? 'group h-full gap-0 border border-border bg-card-background p-0 shadow-none transition-colors hover:border-border/80'
      : 'gap-0 overflow-visible border border-border bg-card-background p-0 shadow-none';

  const contentClassName =
    variant === 'dimension'
      ? 'flex h-full w-full flex-col items-stretch gap-3 p-4'
      : 'flex w-full flex-col items-stretch p-4';

  return (
    <Card data-testid={`configured-metric-${groupId}.${metric.label}`} className={cardClassName}>
      <CardContent className={contentClassName}>
        {metric.totalCount === 0 ? (
          <div className="flex w-full flex-col gap-3">
            <ConfiguredMetricHeader
              metricTitle={metricTitle}
              coveragePercentLabel={coveragePercentLabel}
              variant={variant}
            />
            <p className="text-sm text-muted-foreground">{t('overview.configured.noInstances')}</p>
          </div>
        ) : variant === 'dimension' ? (
          <>
            <ConfiguredMetricHeader
              metricTitle={metricTitle}
              coveragePercentLabel={coveragePercentLabel}
              variant={variant}
            />
            <p className="text-xl font-semibold tabular-nums tracking-tight text-foreground">
              {interpolate(t('overview.dimensionCard.population'), {
                contextualised: formatCount(metric.linkedCount),
                total: formatCount(metric.totalCount),
              })}
            </p>

            <DimensionPopulationBar
              contextualised={metric.linkedCount}
              remaining={remaining}
              includeZeroSegments
              ariaLabel={buildCoverageBreakdownAriaLabel(t, {
                contextualised: metric.linkedCount,
                remaining,
              })}
            />

            <DimensionPopulationLegend
              contextualised={metric.linkedCount}
              remaining={remaining}
              total={metric.totalCount}
              includeZeroSegments
              t={t}
            />

            <DimensionCardTrendFooter
              chartId={chartId}
              points={snapshotPoints}
              totalIncludingExcluded={metric.totalCount}
              populationAtPoint={(index) => ({
                contextualised: historyPoints[index]?.linkedCount ?? metric.linkedCount,
                total: historyPoints[index]?.totalCount ?? metric.totalCount,
              })}
              isLoading={trendLoading}
              t={t}
            />

            <div className="mt-auto">
              <DrilldownButton
                groupId={groupId}
                metricLabel={metric.label}
                canOpen={canOpenDrilldown}
                onOpen={onOpenDrilldown}
                t={t}
              />
            </div>
          </>
        ) : (
          <div className="flex flex-col gap-4 lg:flex-row lg:items-stretch">
            <div className="flex min-w-0 flex-1 flex-col gap-3">
              <ConfiguredMetricHeader
                metricTitle={metricTitle}
                coveragePercentLabel={coveragePercentLabel}
                variant={variant}
              />
              <p className="text-xl font-semibold tabular-nums tracking-tight text-foreground">
                {interpolate(t('overview.dimensionCard.population'), {
                  contextualised: formatCount(metric.linkedCount),
                  total: formatCount(metric.totalCount),
                })}
              </p>

              <DimensionPopulationBar
                contextualised={metric.linkedCount}
                remaining={remaining}
                includeZeroSegments
                ariaLabel={buildCoverageBreakdownAriaLabel(t, {
                  contextualised: metric.linkedCount,
                  remaining,
                })}
              />

              <DimensionPopulationLegend
                contextualised={metric.linkedCount}
                remaining={remaining}
                total={metric.totalCount}
                includeZeroSegments
                t={t}
              />

              <div className="mt-auto">
                <DrilldownButton
                  groupId={groupId}
                  metricLabel={metric.label}
                  canOpen={remaining > 0}
                  onOpen={onOpenDrilldown}
                  t={t}
                />
              </div>
            </div>

            <div
              className="flex w-full min-w-0 flex-col justify-center overflow-visible border-border pr-1 lg:w-[42%] lg:max-w-xs lg:self-stretch lg:border-l lg:pl-4"
              data-testid="configured-metric-trend-aside"
            >
              <DimensionCardTrendFooter
                chartId={chartId}
                points={snapshotPoints}
                totalIncludingExcluded={metric.totalCount}
                populationAtPoint={(index) => ({
                  contextualised: historyPoints[index]?.linkedCount ?? metric.linkedCount,
                  total: historyPoints[index]?.totalCount ?? metric.totalCount,
                })}
                isLoading={trendLoading}
                layout="aside"
                t={t}
              />
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

type DrilldownButtonProps = {
  groupId: string;
  metricLabel: string;
  canOpen: boolean;
  onOpen: () => void;
  t: (key: string) => string;
};

function DrilldownButton({ groupId, metricLabel, canOpen, onOpen, t }: DrilldownButtonProps) {
  return (
    <Button
      variant="ghost"
      size="sm"
      disabled={!canOpen}
      className={metricCardDrilldownButtonClassName(canOpen)}
      data-testid={`configured-metric-open-drilldown-${groupId}.${metricLabel}`}
      onClick={onOpen}
    >
      {t('dimension.openDrilldown')}
      <IconChevronRight className="size-4" />
    </Button>
  );
}

type ConfiguredMetricHeaderProps = {
  metricTitle: string;
  coveragePercentLabel: string | null;
  variant: ConfiguredMetricRowVariant;
};

function ConfiguredMetricHeader({
  metricTitle,
  coveragePercentLabel,
  variant,
}: ConfiguredMetricHeaderProps) {
  if (variant === 'dimension') {
    return (
      <div className="flex w-full items-center gap-2" data-testid="configured-metric-header">
        <div className="flex min-w-0 flex-1 items-center gap-1.5">
          <h3 className="min-w-0 truncate text-sm font-medium text-foreground">{metricTitle}</h3>
        </div>
        {coveragePercentLabel !== null ? (
          <Badge
            variant="sky"
            className="shrink-0 text-xs font-medium tabular-nums"
            data-testid="configured-metric-coverage"
          >
            {coveragePercentLabel}
          </Badge>
        ) : null}
      </div>
    );
  }

  return (
    <div className="flex w-full flex-wrap items-center gap-3" data-testid="configured-metric-header">
      <h4 className="min-w-0 max-w-full truncate text-sm font-medium text-foreground">{metricTitle}</h4>
      {coveragePercentLabel !== null ? (
        <Badge
          variant="sky"
          className="shrink-0 text-xs font-medium tabular-nums"
          data-testid="configured-metric-coverage"
        >
          {coveragePercentLabel}
        </Badge>
      ) : null}
    </div>
  );
}

function interpolate(template: string, vars: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => vars[name] ?? '');
}
