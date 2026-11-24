import { Skeleton } from '@cognite/aura/components/skeleton';

import {
  sectionHeadingClassName,
  sectionHeadingDestructiveClassName,
  sectionHeadingSpacedClassName,
} from '../shell/sectionHeading';

import { ConfiguredViewMetricsPanel } from './ConfiguredViewMetricsPanel';
import { aggregateRowsForMetricCards } from './dimensionMetricPresentation';
import { DimensionSummary } from './DimensionSummary';
import { DrilldownPanel } from './DrilldownPanel';
import { findConfiguredGroup } from './findConfiguredGroup';
import { MetricCards } from './MetricCards';
import { TierQualityTable } from './TierQualityTable';
import { useDimensionViewModel } from './useDimensionViewModel';

export function DimensionPage() {
  const viewModel = useDimensionViewModel();

  if (viewModel.dimensionsScope === 'configured') {
    return (
      <ConfiguredViewMetricsPanel
        group={
          viewModel.configuredGroupId !== undefined
            ? findConfiguredGroup(viewModel.configuredModels, viewModel.configuredGroupId)
            : undefined
        }
        isLoading={viewModel.isConfiguredMetricsLoading}
        isError={viewModel.isConfiguredMetricsError}
        filters={viewModel.filters}
        trendWindow={viewModel.trendWindow}
        t={viewModel.t}
      />
    );
  }

  if (viewModel.isLoading) {
    return (
      <section aria-label={viewModel.t('dimension.loading')} className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-40 w-full" />
      </section>
    );
  }

  if (viewModel.isError || viewModel.dimension === undefined) {
    return (
      <section className="rounded-md border border-destructive-background bg-destructive-muted-background p-4">
        <h2 className={sectionHeadingDestructiveClassName}>{viewModel.t('dimension.error')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('dimension.errorHint')}</p>
      </section>
    );
  }

  if (viewModel.isDisabled) {
    return (
      <div className="space-y-4">
        <DimensionSummary viewModel={viewModel} />
      </div>
    );
  }

  if (viewModel.isEmpty) {
    return (
      <div className="space-y-4">
        <section className="rounded-md border border-border p-6 text-center">
          <h2 className={sectionHeadingClassName}>{viewModel.t('dimension.noData')}</h2>
          <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('dimension.noDataHint')}</p>
        </section>
      </div>
    );
  }

  const dimension = viewModel.dimension;

  return (
    <div className="space-y-6">
      <DimensionSummary viewModel={viewModel} />
      {dimension.tierScores.length > 0 ? (
        <TierQualityTable viewModel={viewModel} tierScores={dimension.tierScores} untiered={dimension.untiered} />
      ) : null}
      <MetricCards
        viewModel={viewModel}
        metrics={aggregateRowsForMetricCards(dimension.dimensionId, dimension.aggregateRows)}
      />
      <DrilldownPanel viewModel={viewModel} />

      {viewModel.lifecycleWarnings.length > 0 ? (
        <section aria-label={viewModel.t('dimension.lifecycleWarnings')}>
          <h2 className={sectionHeadingSpacedClassName}>{viewModel.t('dimension.lifecycleWarnings')}</h2>
          <ul className="space-y-2">
            {viewModel.lifecycleWarnings.map((warning) => (
              <li key={warning.metricId} className="rounded-md border border-warning-background bg-warning-muted-background p-3 text-sm">
                <p className="font-medium">{viewModel.t(warning.labelKey)}</p>
                <p className="text-xs text-muted-foreground">
                  {viewModel.t('dimension.affectedCount')}: {warning.count}{warning.atLeast === true ? '+' : ''} · {viewModel.t(warning.actionKey)}
                </p>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  );
}
