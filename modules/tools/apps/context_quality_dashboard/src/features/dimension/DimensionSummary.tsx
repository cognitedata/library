import { Badge } from '@cognite/aura/components/badge';
import { Button } from '@cognite/aura/components/button';
import { Card, CardContent } from '@cognite/aura/components/card';
import { Tooltip, TooltipContent, TooltipTrigger } from '@cognite/aura/components/tooltip';
import { IconChevronRight, IconInfoCircle } from '@cognite/aura/icons';

import { interpolate } from '../../i18n/interpolate';
import { formatCount } from '../overview/contextualisationCoverageLabels';

import {
  canOpenMetricDrilldown,
  formatMetricReadiness,
  headlineMetricRow,
} from './dimensionMetricPresentation';
import type { DimensionViewModel } from './useDimensionViewModel';

type DimensionSummaryProps = {
  viewModel: DimensionViewModel;
};

export function DimensionSummary({ viewModel }: DimensionSummaryProps) {
  const dimension = viewModel.dimension;
  if (dimension === undefined) {
    return null;
  }

  const headline = headlineMetricRow(dimension);
  const description = viewModel.t(`dimension.description.${dimension.dimensionId}`);
  const t = viewModel.t;

  // Annotations are counted, not measured against a population: there is nothing they could be "x of", so no share is shown
  const countsOnly = headline?.metricId === 'annotationCount';

  const coveragePercentLabel =
    headline !== undefined &&
    !headline.notApplicable &&
    headline.valuePercent !== null &&
    headline.metricId !== 'orphanRate' &&
    !countsOnly
      ? interpolate(t('overview.coverage.coveragePercent'), {
          percent: String(Math.round(headline.valuePercent)),
        })
      : null;

  const readinessHeadline =
    headline !== undefined && !headline.notApplicable
      ? dimensionReadinessHeadline(headline, t)
      : null;

  return (
    <section aria-labelledby="dimension-page-title">
      <Card
        className="gap-0 border border-border bg-card-background p-0 shadow-none"
        data-testid="dimension-summary-card"
      >
        <CardContent className="gap-2 p-4">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
              <h1
                id="dimension-page-title"
                className="text-xs font-medium tracking-wide text-muted-foreground"
              >
                {t(dimension.nameKey)}
              </h1>
              <Tooltip>
                <TooltipTrigger
                  aria-label={t('dimension.descriptionInfoLabel')}
                  className="inline-flex items-center text-muted-foreground"
                  data-testid="dimension-description-info"
                >
                  <IconInfoCircle className="size-3.5" />
                </TooltipTrigger>
                <TooltipContent className="max-w-sm text-pretty">{description}</TooltipContent>
              </Tooltip>
              {coveragePercentLabel !== null ? (
                <Badge
                  variant="sky"
                  className="text-sm font-medium tabular-nums"
                  data-testid="dimension-coverage-percent"
                >
                  {coveragePercentLabel}
                </Badge>
              ) : null}
            </div>
            {dimension.enabled && viewModel.canRerun ? (
              <Button
                variant="outline"
                size="sm"
                data-testid="rerun-dimension"
                disabled={viewModel.isRerunningDimension}
                onClick={viewModel.rerunDimension}
              >
                {viewModel.isRerunningDimension ? t('runStatus.rerunning') : t('runStatus.rerun')}
              </Button>
            ) : null}
          </div>

          {dimension.enabled && dimension.status === 'not-applicable' ? (
            <p className="text-sm text-muted-foreground" data-testid="dimension-not-applicable">
              {t('dimension.notApplicableSummary')}
            </p>
          ) : dimension.enabled && readinessHeadline !== null ? (
            <div className="max-w-full overflow-x-auto">
              <p
                className="whitespace-nowrap text-2xl font-semibold tabular-nums tracking-tight text-foreground sm:text-3xl lg:text-4xl"
                data-testid="dimension-readiness"
              >
                {readinessHeadline}
              </p>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">{t('dimension.unavailable')}</p>
          )}

          {dimension.enabled && headline !== undefined && canOpenMetricDrilldown(headline) ? (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-fit px-0 font-medium text-foreground hover:bg-secondary-background hover:text-foreground"
              data-testid="dimension-summary-open-drilldown"
              onClick={() => {
                viewModel.selectMetric(headline.metricId);
              }}
            >
              {t('dimension.openDrilldown')}
              <IconChevronRight className="size-4" />
            </Button>
          ) : null}

          {dimension.scoreBasis === 'pooled-fallback' ? (
            <p className="text-xs text-muted-foreground" data-testid="dimension-pooled-note">
              {t('dimension.scoreBasis.pooled-fallback')}
            </p>
          ) : null}
        </CardContent>
      </Card>
    </section>
  );
}

function dimensionReadinessHeadline(
  headline: NonNullable<ReturnType<typeof headlineMetricRow>>,
  t: (key: string) => string
): string {
  if (headline.metricId === 'annotationCount') {
    return interpolate(t(headline.passingCount === 1 ? 'dimension.annotationCount.one' : 'dimension.annotationCount.other'), {
      count: formatCount(headline.passingCount),
    });
  }
  if (headline.metricId === 'orphanRate') {
    const orphanText = formatMetricReadiness(headline, t);
    return orphanText ?? t('dimension.unavailable');
  }
  return interpolate(t('overview.coverage.primaryMetric'), {
    contextualised: formatCount(headline.passingCount),
    total: formatCount(headline.eligibleCount),
  });
}
