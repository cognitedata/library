import { useCallback, useEffect, useMemo } from 'react';

import { ALL_DIMENSION_IDS } from '../../domain/dimensions';
import type { DimensionId } from '../../types';
import { useRunConfigFileViewModel } from '../configuration/useRunConfigFileViewModel';
import { useQualityOperations } from '../shell/useQualityOperations';
import { useRunBannerViewModel, type RunBannerViewModel } from '../shell/useRunBannerViewModel';

import { METRICS_MODEL_FILTER_ALL, useRerunSelection, type RerunTab } from './rerunSelectionContext';
import type { RunRequest, SelectedLink } from './runRequest';
import {
  allMetrics,
  buildSelectionTree,
  checkState,
  pruneSelection,
  toggleMany,
  type CheckState,
  type MetricNode,
  type ModelNode,
} from './selectionTree';
import { useRunLauncher } from './useRunLauncher';

export type RerunViewModel = {
  t: (key: string) => string;
  tab: RerunTab;
  setTab: (tab: RerunTab) => void;
  dimensionIds: readonly DimensionId[];
  selectedDimensions: readonly DimensionId[];
  isDimensionSelected: (id: DimensionId) => boolean;
  /** Whether no completed run has measured the dimension. False while it is not known what the runs measured. */
  isDimensionNotMeasured: (id: DimensionId) => boolean;
  toggleDimension: (id: DimensionId) => void;
  allDimensionsState: CheckState;
  toggleAllDimensions: () => void;
  /** Null while no valid configuration exists. */
  tree: readonly ModelNode[] | null;
  /** Metrics tree after the data-model filter is applied. */
  metricsTree: readonly ModelNode[] | null;
  dataModelIds: readonly string[];
  metricsModelFilter: string;
  setMetricsModelFilter: (model: string) => void;
  selectedMetrics: readonly MetricNode[];
  totalMetrics: number;
  isMetricSelected: (id: string) => boolean;
  toggleMetric: (id: string) => void;
  groupState: (ids: readonly string[]) => CheckState;
  toggleGroup: (ids: readonly string[]) => void;
  allMetricsState: CheckState;
  toggleAllMetrics: () => void;
  selectedTotal: number;
  request: RunRequest | null;
  canRun: boolean;
  runAvailable: boolean;
  busy: boolean;
  otherRunInProgress: boolean;
  start: () => void;
  phase: ReturnType<typeof useRunConfigFileViewModel>['phase'];
  startError: ReturnType<typeof useRunConfigFileViewModel>['startError'];
  lastRequested: Extract<RunRequest, { kind: 'selection' }> | null;
  /** The outcome of the latest run, the same message the page shows at the top. */
  runBanner: RunBannerViewModel;
};

export function useRerunViewModel(): RerunViewModel {
  const file = useRunConfigFileViewModel();
  const launcher = useRunLauncher();
  const selection = useRerunSelection();
  const runBanner = useRunBannerViewModel();
  const dimensionRuns = useQualityOperations().runStatus?.dimensionRuns;
  const { t, models } = file;

  const tree = useMemo(() => (models === null ? null : buildSelectionTree(models)), [models]);
  const dataModelIds = useMemo(() => (tree === null ? [] : tree.map((node) => node.model)), [tree]);
  const metricsModelFilter = selection.metricsModelFilter;
  const effectiveMetricsModelFilter = useMemo(() => {
    if (dataModelIds.length === 0) {
      return METRICS_MODEL_FILTER_ALL;
    }
    if (dataModelIds.length === 1) {
      return dataModelIds[0];
    }
    if (
      metricsModelFilter === METRICS_MODEL_FILTER_ALL ||
      dataModelIds.includes(metricsModelFilter)
    ) {
      return metricsModelFilter;
    }
    return METRICS_MODEL_FILTER_ALL;
  }, [dataModelIds, metricsModelFilter]);
  const { setMetricsModelFilter } = selection;
  useEffect(() => {
    if (effectiveMetricsModelFilter !== metricsModelFilter) {
      setMetricsModelFilter(effectiveMetricsModelFilter);
    }
  }, [effectiveMetricsModelFilter, metricsModelFilter, setMetricsModelFilter]);
  const metricsTree = useMemo((): readonly ModelNode[] | null => {
    if (tree === null) {
      return null;
    }
    if (effectiveMetricsModelFilter === METRICS_MODEL_FILTER_ALL) {
      return tree;
    }
    return tree.filter((node) => node.model === effectiveMetricsModelFilter);
  }, [effectiveMetricsModelFilter, tree]);
  const metrics = useMemo(() => (tree === null ? [] : allMetrics(tree)), [tree]);
  const visibleMetrics = useMemo(
    () => (metricsTree === null ? [] : allMetrics(metricsTree)),
    [metricsTree]
  );
  const metricIds = useMemo(() => metrics.map((metric) => metric.id), [metrics]);
  const visibleMetricIds = useMemo(() => visibleMetrics.map((metric) => metric.id), [visibleMetrics]);
  const liveMetrics = useMemo(() => pruneSelection(selection.metrics, metricIds), [selection.metrics, metricIds]);
  const selectedMetrics = useMemo(() => metrics.filter((metric) => liveMetrics.has(metric.id)), [metrics, liveMetrics]);
  const selectedDimensions = useMemo(
    () => ALL_DIMENSION_IDS.filter((id) => selection.dimensions.has(id)),
    [selection.dimensions]
  );
  const selectedTotal = selectedDimensions.length + selectedMetrics.length;

  const { setDimensions, setMetrics } = selection;
  const toggleDimension = useCallback(
    (id: DimensionId) => {
      const next = new Set(selection.dimensions);
      if (!next.delete(id)) {
        next.add(id);
      }
      setDimensions(next);
    },
    [selection.dimensions, setDimensions]
  );
  const toggleAllDimensions = useCallback(() => {
    const all = checkState(ALL_DIMENSION_IDS, selection.dimensions) === 'checked';
    setDimensions(all ? new Set() : new Set(ALL_DIMENSION_IDS));
  }, [selection.dimensions, setDimensions]);
  const toggleGroup = useCallback(
    (ids: readonly string[]) => setMetrics(toggleMany(ids, liveMetrics)),
    [liveMetrics, setMetrics]
  );

  const request = useMemo((): RunRequest | null => {
    if (selectedTotal === 0) {
      return null;
    }
    const links: SelectedLink[] = selectedMetrics.map((metric) => ({ model: metric.model, link: metric.link }));
    return { kind: 'selection', dimensionIds: selectedDimensions, links };
  }, [selectedDimensions, selectedMetrics, selectedTotal]);

  const busy = launcher?.busy === true;
  const runAvailable = launcher !== null;
  const start = useCallback(() => {
    if (launcher === null || launcher.busy || request === null) {
      return;
    }
    launcher.start(request, models);
  }, [launcher, models, request]);

  const lastRequested = launcher?.request?.kind === 'selection' ? launcher.request : null;

  return {
    t,
    tab: selection.tab,
    setTab: selection.setTab,
    dimensionIds: ALL_DIMENSION_IDS,
    selectedDimensions,
    isDimensionSelected: (id) => selection.dimensions.has(id),
    toggleDimension,
    allDimensionsState: checkState(ALL_DIMENSION_IDS, selection.dimensions),
    toggleAllDimensions,
    tree,
    selectedMetrics,
    totalMetrics: metrics.length,
    isMetricSelected: (id) => liveMetrics.has(id),
    toggleMetric: (id) => toggleGroup([id]),
    groupState: (ids) => checkState(ids, liveMetrics),
    toggleGroup,
    allMetricsState: checkState(visibleMetricIds, liveMetrics),
    toggleAllMetrics: () => toggleGroup(visibleMetricIds),
    metricsTree,
    dataModelIds,
    metricsModelFilter: effectiveMetricsModelFilter,
    setMetricsModelFilter,
    selectedTotal,
    request,
    isDimensionNotMeasured: (id: DimensionId) => dimensionRuns !== undefined && dimensionRuns[id] === undefined,
    canRun: runAvailable && !busy && request !== null,
    runBanner,
    runAvailable,
    busy,
    otherRunInProgress: launcher?.otherRunInProgress === true,
    start,
    phase: file.phase,
    startError: file.startError,
    lastRequested,
  };
}
