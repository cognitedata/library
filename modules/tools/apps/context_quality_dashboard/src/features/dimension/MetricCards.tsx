
import { Badge } from '@cognite/aura/components/badge';
import { Button } from '@cognite/aura/components/button';
import { Card, CardContent } from '@cognite/aura/components/card';
import { Tooltip, TooltipContent, TooltipTrigger } from '@cognite/aura/components/tooltip';
import { IconChevronRight, IconInfoCircle } from '@cognite/aura/icons';
import { useId } from 'react';

import { interpolate } from '../../i18n/interpolate';
import type { AggregateMetricRowDto, MetricId } from '../../types';
import {
  buildCoverageBreakdownAriaLabel,
  formatCount,
} from '../overview/contextualisationCoverageLabels';
import { dimensionCardGridClassName } from '../overview/dimensionCardGrid';
import { DimensionCardTrendFooter } from '../overview/DimensionCardTrendFooter';
import { DimensionPopulationBar } from '../overview/DimensionPopulationBar';
import { DimensionPopulationLegend } from '../overview/DimensionPopulationLegend';
import { DIMENSION_TREND_CHART_EMPTY_CLASS } from '../overview/DimensionTrendAreaChart';
import { contextualisedCountAtPercent } from '../overview/trendChartFormat';
import { sectionHeadingSpacedClassName } from '../shell/sectionHeading';

import { AverageGauge } from './AverageGauge';
import {
  canOpenMetricDrilldown,
  formatMetricNotApplicableReason,
  formatMetricReadiness,
  metricDescriptionKey,
  metricCardDrilldownButtonClassName,
  metricDrilldownDisabledTooltipMessage,
} from './dimensionMetricPresentation';
import { isAverageValue, metricWording } from './metricWording';
import type { DimensionTrendViewModel, DimensionViewModel } from './useDimensionViewModel';

type MetricCardsProps = {
  viewModel: DimensionViewModel;
  metrics: readonly AggregateMetricRowDto[];
};

export function MetricCards({ viewModel, metrics }: MetricCardsProps) {
  return (
    <section aria-label={viewModel.t('dimension.metrics')}>
      <h2 className={sectionHeadingSpacedClassName}>{viewModel.t('dimension.metrics')}</h2>
      <div className={dimensionCardGridClassName}>
        {metrics.map((metric) => (
          <MetricCard
            key={metricCardId(metric)}
            metric={metric}
            viewModel={viewModel}
            trend={viewModel.trendMetrics.find(
              (entry) => entry.metricId === metric.metricId && entry.subKey === (metric.subKey ?? null)
            )}
          />
        ))}
      </div>
    </section>
  );
}

type MetricCardProps = {
  viewModel: DimensionViewModel;
  metric: AggregateMetricRowDto;
  trend: DimensionTrendViewModel | undefined;
};

function MetricCard({ viewModel, metric, trend }: MetricCardProps) {
  const t = viewModel.t;
  const canOpenDrilldown = canOpenMetricDrilldown(metric);
  const notApplicableBlockId = useId();
  const notApplicableDetailId = useId();
  const drilldownDisabledTooltip = metricDrilldownDisabledTooltipMessage(metric, t);
  const drilldownTarget = metric.drilldownMetricId ?? metric.metricId;
  const isSelected = viewModel.selectedMetricId === drilldownTarget;
  const contextualised = metric.passingCount;
  const remaining = metric.issueCount;
  const total = metric.eligibleCount;

  const wording = metricWording(metric);
  const averageValue = isAverageValue(metric);
  const coveragePercentLabel =
    !metric.notApplicable && total > 0 && !averageValue
      ? interpolate(t(wording?.badgeKey ?? 'overview.dimensionCard.coveragePercent'), {
          percent: String(Math.round((contextualised / total) * 100)),
        })
      : null;

  const trendPoints = trend?.points ?? [];
  const trendLoading = trend?.isLoading ?? false;
  const metricDescriptionI18nKey = metricDescriptionKey(metric.metricId);

  return (
    <Card
      data-testid={`metric-card-${metricCardId(metric)}`}
      className={`group h-full gap-0 border bg-card-background p-0 shadow-none transition-colors hover:border-border/80 ${
        isSelected ? 'border-border-active' : 'border-border'
      }`}
    >
      <CardContent className="flex h-full w-full flex-col items-stretch gap-3 p-4">
        <div className="flex w-full items-center gap-2">
          <div className="flex min-w-0 flex-1 items-center gap-1.5">
            <h3 className="min-w-0 truncate text-sm font-medium text-foreground">{t(metric.labelKey)}</h3>
            <Tooltip>
              <TooltipTrigger
                aria-label={t('dimension.metric.descriptionInfoLabel')}
                className="inline-flex shrink-0 items-center text-muted-foreground"
                data-testid={`metric-description-info-${metric.metricId}`}
              >
                <IconInfoCircle className="size-3.5" />
              </TooltipTrigger>
              <TooltipContent className="max-w-sm text-pretty">
                {t(metricDescriptionI18nKey)}
              </TooltipContent>
            </Tooltip>
          </div>
          {coveragePercentLabel !== null ? (
            <Badge
              variant="sky"
              className="shrink-0 text-xs font-medium tabular-nums"
              data-testid="metric-coverage-badge"
            >
              {coveragePercentLabel}
            </Badge>
          ) : null}
        </div>

        {metric.notApplicable ? (
          <div
            className={`${DIMENSION_TREND_CHART_EMPTY_CLASS} min-h-24 flex-1 gap-1`}
            data-testid={`metric-not-applicable-${metric.metricId}`}
            id={notApplicableBlockId}
          >
            <p className="text-center text-xs font-medium text-muted-foreground">
              {t('dimension.metric.notApplicable')}
            </p>
            {metric.notApplicableReason === null ? null : (
              <p
                id={notApplicableDetailId}
                className="max-w-full px-2 text-center text-xs text-pretty text-muted-foreground"
                data-testid={`metric-not-applicable-reason-${metric.metricId}`}
              >
                {formatMetricNotApplicableReason(metric.notApplicableReason, t)}
              </p>
            )}
          </div>
        ) : total <= 0 ? (
          <p className="text-sm text-muted-foreground">{t('overview.configured.noInstances')}</p>
        ) : (
          <>
            {averageValue ? (
              <>
                <div className="flex items-baseline gap-2">
                  <p
                    className="text-xl font-semibold tabular-nums tracking-tight text-foreground"
                    data-testid={`metric-count-${metric.metricId}`}
                  >
                    {interpolate(t('dimension.wording.confidenceAverage.value'), {
                      percent: String(metric.valuePercent ?? 0),
                    })}
                  </p>
                  <span className="text-xs text-muted-foreground">{t('dimension.wording.confidenceAverage.caption')}</span>
                </div>
                <AverageGauge
                  percent={metric.valuePercent ?? 0}
                  count={total}
                  ariaLabel={interpolate(t('dimension.wording.confidenceAverage.value'), {
                    percent: String(metric.valuePercent ?? 0),
                  })}
                  countLabel={t('dimension.wording.confidenceAverage.scored')}
                />
              </>
            ) : (
              <p
                className="text-xl font-semibold tabular-nums tracking-tight text-foreground"
                data-testid={`metric-count-${metric.metricId}`}
              >
                {metricHeadline(metric, t)}
              </p>
            )}

            {averageValue ? null : (
              <>
              <DimensionPopulationBar
                contextualised={contextualised}
                remaining={remaining}
                ariaLabel={buildCoverageBreakdownAriaLabel(t, {
                  contextualised,
                  remaining,
                })}
              />

              <DimensionPopulationLegend
                contextualised={contextualised}
                remaining={remaining}
                total={total}
                labelKeys={wording === null ? undefined : { contextualised: wording.passingKey, remaining: wording.remainingKey }}
                t={t}
              />
              </>
            )}

            <DimensionCardTrendFooter
              chartId={`dimension-metric-${metricCardId(metric)}`}
              points={trendPoints}
              totalIncludingExcluded={total}
              populationAtPoint={
                averageValue
                  ? undefined
                  : (index) => ({
                      contextualised: contextualisedCountAtPercent(total, trendPoints[index]?.valuePercent ?? 0),
                      total,
                    })
              }
              isLoading={trendLoading}
              t={t}
            />
          </>
        )}

        <div className="mt-auto">
          <MetricCardDrilldownButton
            canOpen={canOpenDrilldown}
            metricId={metric.metricId}
            tooltipMessage={drilldownDisabledTooltip}
            label={t('dimension.openDrilldown')}
            onOpen={() => {
              viewModel.selectMetric(drilldownTarget);
            }}
          />
        </div>
      </CardContent>
    </Card>
  );
}

type MetricCardDrilldownButtonProps = {
  canOpen: boolean;
  metricId: MetricId;
  label: string;
  tooltipMessage: string | null;
  onOpen: () => void;
};

function MetricCardDrilldownButton({
  canOpen,
  metricId,
  label,
  tooltipMessage,
  onOpen,
}: MetricCardDrilldownButtonProps) {
  const button = (
    <Button
      variant="ghost"
      size="sm"
      disabled={!canOpen}
      className={metricCardDrilldownButtonClassName(canOpen)}
      data-testid={`metric-card-open-${metricId}`}
      onClick={onOpen}
    >
      {label}
      <IconChevronRight className="size-4" />
    </Button>
  );

  if (tooltipMessage === null) {
    return button;
  }

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            className="inline-flex w-fit cursor-not-allowed"
            data-testid={`metric-card-open-drilldown-trigger-${metricId}`}
          />
        }
      >
        {button}
      </TooltipTrigger>
      <TooltipContent className="max-w-sm text-pretty">{tooltipMessage}</TooltipContent>
    </Tooltip>
  );
}

/** Unique per card: a metric reported per field has one card per field. */
function metricCardId(metric: AggregateMetricRowDto): string {
  return metric.subKey === null || metric.subKey === undefined ? metric.metricId : `${metric.metricId}.${metric.subKey}`;
}

function metricHeadline(metric: AggregateMetricRowDto, t: (key: string) => string): string {
  const readiness = formatMetricReadiness(metric, t);
  if (readiness !== null) {
    return readiness;
  }
  return interpolate(t('overview.dimensionCard.population'), {
    contextualised: formatCount(metric.passingCount),
    total: formatCount(metric.eligibleCount),
  });
}

