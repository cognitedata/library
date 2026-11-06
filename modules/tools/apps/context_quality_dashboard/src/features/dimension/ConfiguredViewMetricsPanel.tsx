import { Skeleton } from '@cognite/aura/components/skeleton';
import { useMemo, useState } from 'react';

import type {
  ConfiguredGroupDto,
  ConfiguredMetricKey,
  SliceFilters,
  TrendWindow,
} from '../../types';
import { ConfiguredDrilldownPanel } from '../overview/ConfiguredDrilldownPanel';
import { configuredMetricKeyId } from '../overview/configuredMetricKey';
import { ConfiguredMetricRow } from '../overview/ConfiguredMetricRow';
import { dimensionCardGridClassName } from '../overview/dimensionCardGrid';
import { useConfiguredMetricTrends } from '../overview/useConfiguredMetricTrends';
import { sectionHeadingSpacedClassName } from '../shell/sectionHeading';

type ConfiguredViewMetricsPanelProps = {
  group: ConfiguredGroupDto | undefined;
  isLoading: boolean;
  isError: boolean;
  filters: SliceFilters;
  trendWindow: TrendWindow;
  t: (key: string) => string;
};

export function ConfiguredViewMetricsPanel({
  group,
  isLoading,
  isError,
  filters,
  trendWindow,
  t,
}: ConfiguredViewMetricsPanelProps) {
  const [drilldown, setDrilldown] = useState<
    { key: ConfiguredMetricKey; viewLabel: string } | undefined
  >(undefined);

  const metricKeys = useMemo(
    () =>
      (group?.metrics ?? []).map((metric) => ({
        modelId: metric.modelId,
        view: metric.view,
        property: metric.label,
      })),
    [group?.metrics]
  );

  const trendSeries = useConfiguredMetricTrends(metricKeys, filters, trendWindow);

  if (isLoading) {
    return (
      <section aria-label={t('dimension.configuredView.loading')} className="space-y-4">
        <Skeleton className="h-8 w-56" />
        <Skeleton className="h-40 w-full" />
      </section>
    );
  }

  if (isError) {
    return (
      <section className="rounded-md border border-destructive-background bg-destructive-muted-background p-4">
        <h2 className="text-base font-semibold text-destructive-foreground">
          {t('dimension.configuredView.error')}
        </h2>
        <p className="mt-1 text-sm text-muted-foreground">{t('dimension.configuredView.errorHint')}</p>
      </section>
    );
  }

  if (group === undefined) {
    return (
      <section className="rounded-md border border-border p-6 text-center">
        <h2 className="text-base font-semibold">{t('dimension.configuredView.notFound')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{t('dimension.configuredView.notFoundHint')}</p>
      </section>
    );
  }

  if (group.metrics.length === 0) {
    return (
      <section className="rounded-md border border-border p-6 text-center">
        <h2 className="text-base font-semibold">{t('dimension.configuredView.noMetrics')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{t('overview.configured.empty')}</p>
      </section>
    );
  }

  return (
    <div className="space-y-6">
      <header className="space-y-1">
        <p className="text-sm text-muted-foreground">{group.modelLabel}</p>
        <h1 className="text-lg font-semibold">{group.viewLabel}</h1>
      </header>
      <section aria-label={t('dimension.metrics')}>
        <h2 className={sectionHeadingSpacedClassName}>{t('dimension.metrics')}</h2>
        <div className={dimensionCardGridClassName}>
          {group.metrics.map((metric) => {
            const key = configuredMetricKeyId({
              modelId: metric.modelId,
              view: metric.view,
              property: metric.label,
            });
            const series = trendSeries.get(key);
            return (
              <ConfiguredMetricRow
                key={metric.label}
                variant="dimension"
                groupId={group.id}
                viewLabel={group.viewLabel}
                metric={metric}
                trendPoints={series?.points ?? []}
                trendLoading={series?.isLoading ?? false}
                onOpenDrilldown={() => {
                  setDrilldown({
                    key: { modelId: metric.modelId, view: metric.view, property: metric.label },
                    viewLabel: group.viewLabel,
                  });
                }}
                t={t}
              />
            );
          })}
        </div>
      </section>
      {drilldown !== undefined ? (
        <ConfiguredDrilldownPanel
          metricKey={drilldown.key}
          viewLabel={drilldown.viewLabel}
          filters={filters}
          onClose={() => {
            setDrilldown(undefined);
          }}
          t={t}
        />
      ) : null}
    </div>
  );
}
