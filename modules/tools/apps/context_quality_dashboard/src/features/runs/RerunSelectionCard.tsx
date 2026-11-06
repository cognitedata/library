import { Alert, AlertDescription, AlertTitle } from '@cognite/aura/components/alert';
import { Badge } from '@cognite/aura/components/badge';
import { Button } from '@cognite/aura/components/button';
import { Card, CardContent } from '@cognite/aura/components/card';
import {
  CheckboxGroup,
  CheckboxItem,
  CheckboxItemControl,
  CheckboxItemLabel,
} from '@cognite/aura/components/checkbox';
import { Label } from '@cognite/aura/components/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@cognite/aura/components/select';
import { Tabs, TabsList, TabsPanel, TabsTrigger } from '@cognite/aura/components/tabs';
import { IconChevronDown, IconChevronRight } from '@cognite/aura/icons';
import { useId, useState } from 'react';

import { fill } from './fill';
import { METRICS_MODEL_FILTER_ALL } from './rerunSelectionContext';
import { RerunSelectionFooter } from './RerunSelectionFooter';
import type { CheckState, ModelNode } from './selectionTree';
import type { RerunViewModel } from './useRerunViewModel';

const checkboxItemClassName = 'max-w-none w-full min-h-0 items-center gap-2 py-0.5';
const toolbarCheckboxItemClassName = 'max-w-none w-full min-h-8 items-center py-0';

function ParentCheckbox({
  state,
  label,
  onToggle,
  testId,
  className,
}: {
  state: CheckState;
  label: string;
  onToggle: () => void;
  testId: string;
  className?: string;
}) {
  const labelId = useId();
  return (
    <CheckboxItem className={className ?? checkboxItemClassName}>
      <CheckboxItemControl
        checked={state === 'checked'}
        indeterminate={state === 'indeterminate'}
        onCheckedChange={onToggle}
        aria-labelledby={labelId}
        data-testid={testId}
      />
      <CheckboxItemLabel id={labelId}>{label}</CheckboxItemLabel>
    </CheckboxItem>
  );
}

const viewGroupCheckboxItemClassName = 'max-w-none w-full min-h-0 items-center gap-2 py-0';
const metricCheckboxItemClassName = 'max-w-none w-full min-h-0 items-center gap-2 py-0.5';
/** Align nested metric checkboxes with the view checkbox column (chevron + gap). */
const viewMetricsListClassName = 'space-y-0.5 border-t border-border py-2 pl-11 pr-2';

function ViewGroup({
  view,
  viewModel,
}: {
  view: ModelNode['views'][number];
  viewModel: RerunViewModel;
}) {
  const [open, setOpen] = useState(true);
  const { t } = viewModel;
  const ids = view.metrics.map((metric) => metric.id);
  const selected = ids.filter((id) => viewModel.isMetricSelected(id)).length;
  const panelId = useId();

  return (
    <li className="rounded-md border border-border" data-testid={`rerun-view-${view.id}`}>
      <div className="grid grid-cols-[1.75rem_minmax(0,1fr)_auto] items-center gap-x-2 px-2 py-1.5">
        <Button
          variant="ghost"
          size="sm"
          className="size-7 shrink-0 p-0"
          aria-expanded={open}
          aria-controls={panelId}
          aria-label={fill(t('rerun.toggleView'), { name: view.view })}
          onClick={() => setOpen((value) => !value)}
        >
          {open ? <IconChevronDown className="size-4" aria-hidden /> : <IconChevronRight className="size-4" aria-hidden />}
        </Button>
        <ParentCheckbox
          state={viewModel.groupState(ids)}
          label={view.view}
          onToggle={() => viewModel.toggleGroup(ids)}
          testId={`rerun-view-checkbox-${view.id}`}
          className={viewGroupCheckboxItemClassName}
        />
        <span
          className="shrink-0 tabular-nums text-xs text-muted-foreground"
          data-testid={`rerun-view-count-${view.id}`}
        >
          {selected} / {ids.length}
        </span>
      </div>
      {open ? (
        <ul id={panelId} className={viewMetricsListClassName}>
          {view.metrics.map((metric) => {
            const labelId = `${panelId}-${metric.id}`;
            return (
              <li key={metric.id}>
                <CheckboxItem className={metricCheckboxItemClassName}>
                  <CheckboxItemControl
                    checked={viewModel.isMetricSelected(metric.id)}
                    onCheckedChange={() => viewModel.toggleMetric(metric.id)}
                    aria-labelledby={labelId}
                    data-testid={`rerun-metric-${metric.id}`}
                  />
                  <CheckboxItemLabel id={labelId}>{metric.property}</CheckboxItemLabel>
                </CheckboxItem>
              </li>
            );
          })}
        </ul>
      ) : null}
    </li>
  );
}

function MetricsPanel({ viewModel }: { viewModel: RerunViewModel }) {
  const { t, tree, metricsTree, dataModelIds, metricsModelFilter, setMetricsModelFilter } = viewModel;
  const showModelHeaders =
    metricsModelFilter === METRICS_MODEL_FILTER_ALL && dataModelIds.length > 1;
  if (tree === null || tree.length === 0) {
    return (
      <div className="px-5">
        <Alert data-testid="rerun-metrics-empty">
          <AlertTitle>{t('rerun.metrics.empty.title')}</AlertTitle>
          <AlertDescription>{t('rerun.metrics.empty.body')}</AlertDescription>
        </Alert>
      </div>
    );
  }
  const listTree = metricsTree ?? [];
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="shrink-0 px-5 pb-3">
        <div className="flex items-center justify-between gap-4">
          <div className="min-w-0 flex-1">
            <ParentCheckbox
              state={viewModel.allMetricsState}
              label={t('rerun.selectAllMetrics')}
              onToggle={viewModel.toggleAllMetrics}
              testId="rerun-select-all-metrics"
              className={toolbarCheckboxItemClassName}
            />
          </div>
          <div className="w-full max-w-xs shrink-0 sm:w-72">
            <Label htmlFor="rerun-metrics-model-filter" className="sr-only">
              {t('rerun.metrics.dataModel')}
            </Label>
            <Select value={metricsModelFilter} onValueChange={setMetricsModelFilter}>
              <SelectTrigger
                id="rerun-metrics-model-filter"
                className="h-8 w-full"
                data-testid="rerun-metrics-model-filter"
              >
                <SelectValue placeholder={t('rerun.metrics.dataModel')} />
              </SelectTrigger>
              <SelectContent>
                {dataModelIds.length > 1 ? (
                  <SelectItem value={METRICS_MODEL_FILTER_ALL}>{t('rerun.metrics.allModels')}</SelectItem>
                ) : null}
                {dataModelIds.map((modelId) => (
                  <SelectItem key={modelId} value={modelId}>
                    {modelId}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>
      <div className="w-full shrink-0 border-b border-border" role="presentation" data-testid="rerun-metrics-toolbar-divider" />
      <div className="min-h-0 flex-1 space-y-4 overflow-y-auto px-5 pt-3 pl-6">
        {listTree.map((node) => (
          <section key={node.model} aria-label={node.model} className="space-y-2" data-testid={`rerun-model-${node.model}`}>
            {showModelHeaders ? (
              <h3 className="text-xs font-medium text-muted-foreground">{node.model}</h3>
            ) : null}
            <ul className="space-y-2">
              {node.views.map((view) => (
                <ViewGroup key={view.id} view={view} viewModel={viewModel} />
              ))}
            </ul>
          </section>
        ))}
      </div>
    </div>
  );
}

function DimensionsPanel({ viewModel }: { viewModel: RerunViewModel }) {
  const { t } = viewModel;
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="shrink-0 px-5 pb-3">
        <ParentCheckbox
          state={viewModel.allDimensionsState}
          label={t('rerun.selectAllDimensions')}
          onToggle={viewModel.toggleAllDimensions}
          testId="rerun-select-all-dimensions"
          className={toolbarCheckboxItemClassName}
        />
      </div>
      <div className="w-full shrink-0 border-b border-border" role="presentation" data-testid="rerun-dimensions-toolbar-divider" />
      <CheckboxGroup className="min-h-0 flex-1 overflow-y-auto px-5 pt-3">
        <ul className="w-full space-y-0.5">
          {viewModel.dimensionIds.map((id) => {
            const labelId = `rerun-dimension-label-${id}`;
            return (
              <li key={id}>
                <CheckboxItem className={checkboxItemClassName}>
                  <CheckboxItemControl
                    checked={viewModel.isDimensionSelected(id)}
                    onCheckedChange={() => viewModel.toggleDimension(id)}
                    aria-labelledby={labelId}
                    data-testid={`rerun-dimension-${id}`}
                  />
                  <CheckboxItemLabel id={labelId}>{t(`dimension.${id}`)}</CheckboxItemLabel>
                  {viewModel.isDimensionNotMeasured(id) ? (
                    <Badge variant="secondary" className="font-normal" data-testid={`rerun-dimension-not-measured-${id}`}>
                      {t('rerun.dimension.notMeasured')}
                    </Badge>
                  ) : null}
                </CheckboxItem>
              </li>
            );
          })}
        </ul>
      </CheckboxGroup>
    </div>
  );
}

const tabsListClassName =
  'inline-flex w-auto max-w-full gap-6 rounded-none bg-transparent [&_[data-slot=tabs-indicator]]:hidden';
const tabsTriggerClassName =
  '-mb-px h-auto shrink-0 justify-start rounded-none border-b-2 border-transparent bg-transparent px-1 pb-3 pt-0 shadow-none hover:bg-transparent hover:text-foreground data-active:border-foreground data-active:bg-transparent data-active:shadow-none';
const tabsPanelClassName =
  'flex min-h-0 w-full flex-1 flex-col pb-5 pt-4 data-[hidden]:hidden';

/** The picks: default dimensions and configured metrics, on two tabs that keep what was chosen. */
export function RerunSelectionCard({ viewModel }: { viewModel: RerunViewModel }) {
  const { t } = viewModel;
  const onTabChange = (value: string | number | null): void => {
    if (value === 'dimensions' || value === 'metrics') {
      viewModel.setTab(value);
    }
  };
  return (
    <Card
      className="gap-0 border border-border bg-card-background p-0 shadow-none"
      data-testid="rerun-selection-card"
    >
      <CardContent className="flex h-[680px] w-full min-w-0 flex-col overflow-hidden p-0">
        <Tabs
          value={viewModel.tab}
          onValueChange={onTabChange}
          className="flex h-full min-h-0 w-full flex-1 flex-col"
        >
          <div className="w-full shrink-0 pt-5">
            <div className="px-5">
              <TabsList size="default" className={tabsListClassName} aria-label={t('rerun.selection.label')}>
                <TabsTrigger value="dimensions" className={tabsTriggerClassName}>
                  {t('rerun.tab.dimensions')}
                </TabsTrigger>
                <TabsTrigger value="metrics" className={tabsTriggerClassName}>
                  {t('rerun.tab.metrics')}
                </TabsTrigger>
              </TabsList>
            </div>
            <div className="w-full border-b border-border" role="presentation" data-testid="rerun-tabs-divider" />
          </div>
          <TabsPanel value="dimensions" className={tabsPanelClassName}>
            <DimensionsPanel viewModel={viewModel} />
          </TabsPanel>
          <TabsPanel value="metrics" className={tabsPanelClassName}>
            <MetricsPanel viewModel={viewModel} />
          </TabsPanel>
        </Tabs>
        <RerunSelectionFooter viewModel={viewModel} />
      </CardContent>
    </Card>
  );
}
