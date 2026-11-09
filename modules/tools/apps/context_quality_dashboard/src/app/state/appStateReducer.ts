import type { AppStateAction, HostSyncedState } from './types';

export function appStateReducer(state: HostSyncedState, action: AppStateAction): HostSyncedState {
  switch (action.type) {
    case 'setPage':
      return { ...state, page: action.page };
    case 'setDimensionId':
      return {
        ...state,
        dimensionId: action.dimensionId,
        dimensionsScope: 'builtin',
        configuredModelId: undefined,
        configuredGroupId: undefined,
      };
    case 'setDimensionsScope':
      if (action.scope === 'builtin') {
        return {
          ...state,
          dimensionsScope: 'builtin',
          dimensionId: action.dimensionId,
          configuredModelId: undefined,
          configuredGroupId: undefined,
          metricId: undefined,
        };
      }
      return {
        ...state,
        dimensionsScope: 'configured',
        configuredModelId: action.configuredModelId,
        configuredGroupId: action.configuredGroupId,
        metricId: undefined,
      };
    case 'setMetricId':
      return { ...state, metricId: action.metricId };
    case 'setFilters':
      return { ...state, filters: { ...action.filters } };
    case 'setFilter': {
      const filters = { ...state.filters };
      if (action.value === undefined) {
        delete filters[action.key];
      } else {
        filters[action.key] = action.value;
      }
      if (action.key === 'facilityId') {
        delete filters.divisionId;
        delete filters.disciplineId;
        delete filters.ownerId;
      }
      return { ...state, filters };
    }
    case 'setTrendWindow':
      return { ...state, trendWindow: action.trendWindow };
    case 'setRunNotesOpen':
      return { ...state, runNotesOpen: action.open ? true : undefined };
    case 'setConfigurationOnboardingComplete':
      return {
        ...state,
        configurationOnboardingComplete: action.complete ? true : undefined,
        configurationOptionalSkipped: action.complete ? undefined : state.configurationOptionalSkipped,
      };
    case 'setBuiltInAutoRunAttempted':
      return {
        ...state,
        builtInAutoRunAttempted: action.attempted ? true : undefined,
      };
    case 'setConfigurationOptionalSkipped':
      return {
        ...state,
        configurationOptionalSkipped: action.skipped ? true : undefined,
      };
    case 'replace':
      return {
        ...action.state,
        filters: { ...action.state.filters },
      };
  }
}
