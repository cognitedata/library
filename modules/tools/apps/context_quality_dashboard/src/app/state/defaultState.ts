import type { HostSyncedState } from './types';

export const DEFAULT_HOST_SYNCED_STATE: HostSyncedState = {
  page: 'overview',
  dimensionId: undefined,
  metricId: undefined,
  filters: {},
  trendWindow: '30d',
};
