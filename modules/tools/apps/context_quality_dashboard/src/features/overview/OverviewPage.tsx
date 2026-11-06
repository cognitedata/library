import { Button } from '@cognite/aura/components/button';
import {
  EmptyState,
  EmptyStateDescription,
  EmptyStateTitle,
} from '@cognite/aura/components/empty-state';
import { Skeleton } from '@cognite/aura/components/skeleton';

import { interpolate } from '../../i18n/interpolate';
import { useLanguage } from '../../i18n/useLanguage';
import { useBackend, useRunService } from '../../services/useServices';
import type { TrendWindow } from '../../types';
import { formatLastSuccessfulRun } from '../shell/formatLastSuccessfulRun';
import {
  sectionHeadingClassName,
  sectionHeadingDestructiveClassName,
} from '../shell/sectionHeading';

import { ConfiguredMetricsSection } from './ConfiguredMetricsSection';
import { buildDimensionCardCounts } from './dimensionCardCounts';
import { dimensionCardGridClassName } from './dimensionCardGrid';
import { DimensionOverviewCard } from './DimensionOverviewCard';
import type { OverviewPresentation } from './overviewPresentation';
import { useDimensionOverviewTrends } from './useDimensionOverviewTrends';
import type { OverviewViewModel } from './useOverviewViewModel';
import { useOverviewViewModel } from './useOverviewViewModel';

export function OverviewPage() {
  const viewModel = useOverviewViewModel();
  const backend = useBackend();
  const canStartRun = useRunService() !== null;

  if (viewModel.isSliceLoading) {
    return (
      <section aria-label={viewModel.t('overview.loading')} className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-32 w-full" />
        <div className="grid gap-6 lg:grid-cols-[minmax(0,1.6fr)_minmax(0,1fr)]">
          <Skeleton className="h-64 w-full" />
          <Skeleton className="h-48 w-full" />
        </div>
      </section>
    );
  }

  if (
    viewModel.isError ||
    viewModel.overview === undefined ||
    viewModel.presentation === undefined
  ) {
    return (
      <section className="rounded-md border border-destructive-background bg-destructive-muted-background p-4">
        <h2 className={sectionHeadingDestructiveClassName}>{viewModel.t('overview.error')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('overview.errorHint')}</p>
      </section>
    );
  }

  if (viewModel.isEmpty) {
    return (
      <EmptyState>
        <EmptyStateTitle>{viewModel.t('overview.noData')}</EmptyStateTitle>
        <EmptyStateDescription>{viewModel.t('overview.noDataHint')}</EmptyStateDescription>
      </EmptyState>
    );
  }

  const presentation = viewModel.presentation;

  return (
    <div className="space-y-8">
      <DimensionsSection viewModel={viewModel} presentation={presentation} />
      <ConfiguredMetricsSection
        models={viewModel.configuredModels}
        filters={viewModel.filters}
        isConfiguredLoaded={viewModel.isConfiguredLoaded}
        showNotConfiguredEmpty={
          backend.kind === 'cdf' &&
          viewModel.isConfiguredLoaded &&
          viewModel.configuredModels.length === 0
        }
        canStartRun={canStartRun}
        onOpenConfiguration={viewModel.navigateToConfiguration}
        t={viewModel.t}
      />
    </div>
  );
}

type DimensionsSectionProps = {
  viewModel: OverviewViewModel;
  presentation: OverviewPresentation;
};

const TREND_WINDOWS: readonly TrendWindow[] = ['7d', '30d', '90d'];

function DimensionsSection({ viewModel, presentation }: DimensionsSectionProps) {
  // one trend window for the Overview cards and the dimension pages, as set here or in the filter bar
  const dimensionTrendWindow = viewModel.trendWindow;
  const setDimensionTrendWindow = viewModel.setTrendWindow;
  const trendSeriesByDimension = useDimensionOverviewTrends(
    presentation.dimensions,
    viewModel.filters,
    dimensionTrendWindow
  );

  const counts = buildDimensionCardCounts(presentation.dimensions);
  const { language } = useLanguage();
  // a card from an earlier run than the freshest says when it was measured
  const freshest = presentation.dimensions
    .map((dimension) => dimension.measuredAt)
    .reduce<string | undefined>((latest, at) => (at !== undefined && (latest === undefined || at > latest) ? at : latest), undefined);

  return (
    <section aria-label={viewModel.t('overview.dimensionOverview')}>
      <div className="mb-3 flex items-start justify-between gap-2">
        <div>
          <h2 className={sectionHeadingClassName}>
            {viewModel.t('overview.dimensionOverview')}
          </h2>
          <p className="mt-0.5 text-sm text-muted-foreground">
            {viewModel.t('overview.dimensionOverviewHint')}
          </p>
        </div>
        <div
          className="flex shrink-0 items-center gap-0.5 rounded-lg border border-border bg-card-background p-0.5"
          role="group"
          aria-label={viewModel.t('overview.trendWindowLabel')}
          data-testid="dimension-overview-trend-window"
        >
          {TREND_WINDOWS.map((window) => (
            <Button
              key={window}
              type="button"
              size="sm"
              variant={dimensionTrendWindow === window ? 'secondary' : 'ghost'}
              className="h-7 px-2.5"
              aria-pressed={dimensionTrendWindow === window}
              data-testid={`dimension-overview-trend-window-${window}`}
              onClick={() => setDimensionTrendWindow(window)}
            >
              {viewModel.t(`trend.${window}`)}
            </Button>
          ))}
        </div>
      </div>
      <div className={dimensionCardGridClassName}>
        {presentation.dimensions.map((dimension) => {
          const trendSeries = trendSeriesByDimension.get(dimension.dimensionId);
          return (
            <DimensionOverviewCard
              key={dimension.dimensionId}
              dimension={dimension}
              counts={counts.find((entry) => entry.dimensionId === dimension.dimensionId)}
              trendPoints={trendSeries?.points ?? []}
              trendLoading={trendSeries?.isLoading ?? false}
              measuredCaption={measuredCaptionFor(dimension.measuredAt, freshest, viewModel.t, language)}
              t={viewModel.t}
              onOpen={() => viewModel.navigateToDimension(dimension.dimensionId)}
            />
          );
        })}
      </div>
    </section>
  );
}

/** "Measured <date>" for a card whose run is older than the freshest card's, so that no card passes for fresher than it is. */
function measuredCaptionFor(
  measuredAt: string | undefined,
  freshest: string | undefined,
  t: (key: string) => string,
  language: string
): string | null {
  if (measuredAt === undefined || freshest === undefined || measuredAt >= freshest) {
    return null;
  }
  return interpolate(t('overview.measuredAt'), {
    date: formatLastSuccessfulRun(measuredAt, language === 'ja' ? 'ja-JP' : 'en-GB'),
  });
}
