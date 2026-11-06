import { Button } from '@cognite/aura/components/button';
import { Skeleton } from '@cognite/aura/components/skeleton';
import { IconChevronRight } from '@cognite/aura/icons';
import { useMemo, useState, type ReactNode } from 'react';

import { interpolate } from '../../i18n/interpolate';
import type {
  ConfiguredDataModelDto,
  ConfiguredGroupDto,
  ConfiguredMetricKey,
  SliceFilters,
  TrendWindow,
} from '../../types';
import { FilterDropdownTile } from '../shell/FilterDropdownTile';
import { sectionHeadingClassName } from '../shell/sectionHeading';

import { ConfiguredDrilldownPanel } from './ConfiguredDrilldownPanel';
import { configuredMetricKeyId } from './configuredMetricKey';
import { ConfiguredMetricRow } from './ConfiguredMetricRow';
import { ConfiguredMetricsNotConfiguredEmpty } from './ConfiguredMetricsNotConfiguredEmpty';
import { useConfiguredMetricTrends } from './useConfiguredMetricTrends';

type ConfiguredMetricsSectionProps = {
  models: readonly ConfiguredDataModelDto[];
  filters: SliceFilters;
  isConfiguredLoaded: boolean;
  showNotConfiguredEmpty: boolean;
  canStartRun: boolean;
  onOpenConfiguration: () => void;
  t: (key: string) => string;
};

const TREND_WINDOWS: readonly TrendWindow[] = ['7d', '30d', '90d'];

const configuredMetricsShellClassName =
  'flex min-h-[32rem] flex-col overflow-x-hidden overflow-y-visible rounded-xl border border-border bg-card-background sm:flex-row';

/**
 * Config-file metrics in a master–detail layout: views in the sidebar, stacked metrics in the panel.
 */
export function ConfiguredMetricsSection({
  models,
  filters,
  isConfiguredLoaded,
  showNotConfiguredEmpty,
  canStartRun,
  onOpenConfiguration,
  t,
}: ConfiguredMetricsSectionProps) {
  const [configuredTrendWindow, setConfiguredTrendWindow] = useState<TrendWindow>('30d');
  const [selectedModelId, setSelectedModelId] = useState<string | undefined>(undefined);
  const [selectedGroupId, setSelectedGroupId] = useState<string | undefined>(undefined);
  const [drilldown, setDrilldown] = useState<{ key: ConfiguredMetricKey; viewLabel: string } | undefined>(undefined);

  const modelsWithGroups = useMemo(
    () => models.filter((model) => model.groups.length > 0),
    [models]
  );

  const resolvedModelId = useMemo(() => {
    if (modelsWithGroups.length === 0) {
      return undefined;
    }
    if (
      selectedModelId !== undefined &&
      modelsWithGroups.some((model) => model.id === selectedModelId)
    ) {
      return selectedModelId;
    }
    return modelsWithGroups[0]?.id;
  }, [modelsWithGroups, selectedModelId]);

  const selectedModel =
    modelsWithGroups.find((model) => model.id === resolvedModelId) ?? modelsWithGroups[0];
  const groupsForModel = useMemo(() => selectedModel?.groups ?? [], [selectedModel?.groups]);

  const resolvedGroupId = useMemo(() => {
    if (groupsForModel.length === 0) {
      return undefined;
    }
    if (
      selectedGroupId !== undefined &&
      groupsForModel.some((group) => group.id === selectedGroupId)
    ) {
      return selectedGroupId;
    }
    return groupsForModel[0]?.id;
  }, [groupsForModel, selectedGroupId]);

  const selectedGroup =
    groupsForModel.find((group) => group.id === resolvedGroupId) ?? groupsForModel[0];

  const metricKeys = useMemo(
    () =>
      (selectedGroup?.metrics ?? []).map((metric) => ({
        modelId: metric.modelId,
        view: metric.view,
        property: metric.label,
      })),
    [selectedGroup?.metrics]
  );
  const trendSeries = useConfiguredMetricTrends(metricKeys, filters, configuredTrendWindow);

  if (models.length === 0 && !showNotConfiguredEmpty && isConfiguredLoaded) {
    return null;
  }

  const hasAnyGroups = models.some((model) => model.groups.length > 0);

  const body = ((): ReactNode => {
    if (!isConfiguredLoaded) {
      return (
        <div className={configuredMetricsShellClassName} data-testid="configured-metrics-loading">
          <Skeleton className="min-h-[32rem] w-full rounded-xl" />
        </div>
      );
    }
    if (showNotConfiguredEmpty) {
      return (
        <div
          className={`${configuredMetricsShellClassName} items-center justify-center p-6`}
          data-testid="configured-metrics-not-configured-shell"
        >
          <ConfiguredMetricsNotConfiguredEmpty
            canStartRun={canStartRun}
            onOpenConfiguration={onOpenConfiguration}
            t={t}
          />
        </div>
      );
    }
    if (!hasAnyGroups) {
      return (
        <p className="text-sm text-muted-foreground" data-testid="configured-metrics-empty">
          {t('overview.configured.empty')}
        </p>
      );
    }
    return (
      <div className={configuredMetricsShellClassName} data-testid="configured-metrics-layout">
          <nav
            aria-label={t('overview.configured.viewsNav')}
            className="flex w-full shrink-0 flex-col gap-1.5 border-border bg-card-background p-3 sm:w-[32%] sm:max-w-xs sm:border-r"
            data-testid="configured-metrics-sidebar"
          >
            {selectedModel !== undefined ? (
              <div
                className="mb-2 border-b border-border pb-3"
                data-testid="configured-metrics-sidebar-model"
              >
                <FilterDropdownTile
                  label={t('overview.configured.dataModel')}
                  value={selectedModel.id}
                  displayValue={selectedModel.label}
                  triggerTestId="configured-metrics-model-select"
                  triggerWidthClass="w-full max-w-none"
                  truncateDisplayValue={false}
                  disabled={modelsWithGroups.length <= 1}
                  items={modelsWithGroups.map((model) => ({ value: model.id, label: model.label }))}
                  onValueChange={(value) => {
                    setSelectedModelId(value);
                  }}
                />
              </div>
            ) : null}
            {groupsForModel.map((group) => (
              <ViewNavButton
                key={group.id}
                group={group}
                isSelected={group.id === resolvedGroupId}
                onSelect={() => {
                  setSelectedGroupId(group.id);
                }}
                t={t}
              />
            ))}
          </nav>

          {selectedGroup !== undefined ? (
            <ConfiguredMetricsDetail
              group={selectedGroup}
              trendSeries={trendSeries}
              onOpenDrilldown={(key) => {
                setDrilldown({ key, viewLabel: selectedGroup.viewLabel });
              }}
              t={t}
            />
          ) : null}
        </div>
    );
  })();

  return (
    <section aria-label={t('overview.configured.title')} className="flex flex-col gap-3">
      <div className="flex items-start justify-between gap-2">
        <div>
          <h2 className={sectionHeadingClassName}>{t('overview.configured.title')}</h2>
          <p className="mt-0.5 text-sm text-muted-foreground">{t('overview.configured.hint')}</p>
        </div>
        {hasAnyGroups && isConfiguredLoaded && !showNotConfiguredEmpty ? (
          <div
            className="flex shrink-0 items-center gap-0.5 rounded-lg border border-border bg-card-background p-0.5"
            role="group"
            aria-label={t('overview.trendWindowLabel')}
            data-testid="configured-metrics-trend-window"
          >
            {TREND_WINDOWS.map((window) => (
              <Button
                key={window}
                type="button"
                size="sm"
                variant={configuredTrendWindow === window ? 'secondary' : 'ghost'}
                className="h-7 px-2.5"
                aria-pressed={configuredTrendWindow === window}
                data-testid={`configured-metrics-trend-window-${window}`}
                onClick={() => setConfiguredTrendWindow(window)}
              >
                {t(`trend.${window}`)}
              </Button>
            ))}
          </div>
        ) : null}
      </div>

      {body}

      {drilldown === undefined ? null : (
        <ConfiguredDrilldownPanel
          key={configuredMetricKeyId(drilldown.key)}
          metricKey={drilldown.key}
          viewLabel={drilldown.viewLabel}
          filters={filters}
          onClose={() => {
            setDrilldown(undefined);
          }}
          t={t}
        />
      )}
    </section>
  );
}

type ViewNavButtonProps = {
  group: ConfiguredGroupDto;
  isSelected: boolean;
  onSelect: () => void;
  t: (key: string) => string;
};

function ViewNavButton({ group, isSelected, onSelect, t }: ViewNavButtonProps) {
  const metricCount = group.metrics.length;
  return (
    <button
      type="button"
      data-testid={`configured-view-nav-${group.id}`}
      aria-current={isSelected ? 'page' : undefined}
      className={`flex w-full items-center justify-between gap-2 rounded-lg px-3 py-2.5 text-left transition-colors ${
        isSelected
          ? 'bg-muted-background text-foreground'
          : 'bg-transparent text-foreground hover:bg-muted-background/60'
      }`}
      onClick={onSelect}
    >
      <span className="min-w-0">
        <span className={`block truncate text-sm ${isSelected ? 'font-semibold' : 'font-medium'}`}>
          {group.viewLabel}
        </span>
        <span className="mt-0.5 block truncate text-xs text-muted-foreground">
          {interpolate(t('overview.configured.viewNavSubtitle'), {
            model: group.modelLabel,
            count: String(metricCount),
          })}
        </span>
      </span>
      {isSelected ? (
        <IconChevronRight className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
      ) : null}
    </button>
  );
}

type ConfiguredMetricsDetailProps = {
  group: ConfiguredGroupDto;
  trendSeries: ReturnType<typeof useConfiguredMetricTrends>;
  onOpenDrilldown: (key: ConfiguredMetricKey) => void;
  t: (key: string) => string;
};

function ConfiguredMetricsDetail({ group, trendSeries, onOpenDrilldown, t }: ConfiguredMetricsDetailProps) {
  return (
    <div
      className="min-w-0 flex-1 bg-card-background p-4 sm:p-5"
      data-testid={`configured-group-${group.id}`}
    >
      <div className="flex flex-col gap-4">
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
              groupId={group.id}
              viewLabel={group.viewLabel}
              metric={metric}
              trendPoints={series?.points ?? []}
              trendLoading={series?.isLoading ?? false}
              onOpenDrilldown={() => {
                onOpenDrilldown({ modelId: metric.modelId, view: metric.view, property: metric.label });
              }}
              t={t}
            />
          );
        })}
      </div>
    </div>
  );
}

