import type { DimensionId, TrendWindow } from '../../types';

import { DEFAULT_HOST_SYNCED_STATE } from './defaultState';
import type { HostSyncedState, PageId } from './types';

const PAGE_IDS: readonly PageId[] = [
  'overview',
  'dimensions',
  'issues',
  'alerts',
  'configuration',
];

const DIMENSION_IDS: readonly DimensionId[] = [
  'asset-hierarchy',
  'equipment-asset',
  'time-series',
  'maintenance',
  'file-annotation',
  'model-3d',
  'files',
];

const TREND_WINDOWS: readonly TrendWindow[] = ['7d', '30d', '90d'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isPageId(value: unknown): value is PageId {
  if (typeof value !== 'string') {
    return false;
  }
  return PAGE_IDS.some((pageId) => pageId === value);
}

function isDimensionId(value: unknown): value is DimensionId {
  if (typeof value !== 'string') {
    return false;
  }
  return DIMENSION_IDS.some((dimensionId) => dimensionId === value);
}

function isTrendWindow(value: unknown): value is TrendWindow {
  if (typeof value !== 'string') {
    return false;
  }
  return TREND_WINDOWS.some((window) => window === value);
}

function parseFilters(value: unknown): HostSyncedState['filters'] {
  if (!isRecord(value)) {
    return {};
  }

  const filters: HostSyncedState['filters'] = {};

  if (isNonEmptyString(value.facilityId)) {
    filters.facilityId = value.facilityId;
  }
  if (isNonEmptyString(value.locationId)) {
    filters.locationId = value.locationId;
  }

  return filters;
}

function mergeHostSyncedState(partial: Record<string, unknown>): HostSyncedState {
  const next: HostSyncedState = {
    page: DEFAULT_HOST_SYNCED_STATE.page,
    dimensionId: DEFAULT_HOST_SYNCED_STATE.dimensionId,
    metricId: DEFAULT_HOST_SYNCED_STATE.metricId,
    filters: { ...DEFAULT_HOST_SYNCED_STATE.filters },
    trendWindow: DEFAULT_HOST_SYNCED_STATE.trendWindow,
  };

  if (isPageId(partial.page)) {
    next.page = partial.page;
  }

  if (partial.dimensionId === undefined) {
    next.dimensionId = undefined;
  } else if (isDimensionId(partial.dimensionId)) {
    next.dimensionId = partial.dimensionId;
  }

  if (partial.metricId === undefined) {
    next.metricId = undefined;
  } else if (isNonEmptyString(partial.metricId)) {
    next.metricId = partial.metricId;
  }

  if (partial.runNotesOpen === true) {
    next.runNotesOpen = true;
  }

  if (partial.configurationOnboardingComplete === true) {
    next.configurationOnboardingComplete = true;
  }

  if (partial.builtInAutoRunAttempted === true) {
    next.builtInAutoRunAttempted = true;
  }

  if (partial.configurationOptionalSkipped === true) {
    next.configurationOptionalSkipped = true;
  }

  next.filters = parseFilters(partial.filters);

  if (isTrendWindow(partial.trendWindow)) {
    next.trendWindow = partial.trendWindow;
  }

  if (partial.dimensionsScope === 'builtin' || partial.dimensionsScope === 'configured') {
    next.dimensionsScope = partial.dimensionsScope;
  }
  if (partial.configuredModelId === undefined) {
    next.configuredModelId = undefined;
  } else if (isNonEmptyString(partial.configuredModelId)) {
    next.configuredModelId = partial.configuredModelId;
  }
  if (partial.configuredGroupId === undefined) {
    next.configuredGroupId = undefined;
  } else if (isNonEmptyString(partial.configuredGroupId)) {
    next.configuredGroupId = partial.configuredGroupId;
  }

  return next;
}

export function parseInitialState(initialState: string | undefined): HostSyncedState {
  if (initialState === undefined || initialState.trim() === '') {
    return { ...DEFAULT_HOST_SYNCED_STATE, filters: { ...DEFAULT_HOST_SYNCED_STATE.filters } };
  }

  try {
    const parsed: unknown = JSON.parse(initialState);
    if (!isRecord(parsed)) {
      return { ...DEFAULT_HOST_SYNCED_STATE, filters: { ...DEFAULT_HOST_SYNCED_STATE.filters } };
    }
    return mergeHostSyncedState(parsed);
  } catch {
    return { ...DEFAULT_HOST_SYNCED_STATE, filters: { ...DEFAULT_HOST_SYNCED_STATE.filters } };
  }
}
