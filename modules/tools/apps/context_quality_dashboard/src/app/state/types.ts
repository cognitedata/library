import type { DimensionId, TrendWindow } from '../../types';

export type PageId = 'overview' | 'dimensions' | 'issues' | 'alerts' | 'configuration';

export type DimensionsScopeKind = 'builtin' | 'configured';

export type HostSyncedState = {
  page: PageId;
  dimensionId?: DimensionId;
  /** Built-in dimension vs configured data-model view on the Dimensions page. */
  dimensionsScope?: DimensionsScopeKind;
  configuredModelId?: string;
  configuredGroupId?: string;
  metricId?: string;
  /** Whether the run notes panel is open. Absent means closed. */
  runNotesOpen?: boolean;
  /** User finished or skipped the Configuration first-run onboarding. */
  configurationOnboardingComplete?: boolean;
  /** Built-in auto-start was requested once (avoids duplicate runs on reload). */
  builtInAutoRunAttempted?: boolean;
  /** User skipped optional additional checks during first-run onboarding. */
  configurationOptionalSkipped?: boolean;
  filters: {
    facilityId?: string;
    locationId?: string;
    divisionId?: string;
    disciplineId?: string;
    ownerId?: string;
  };
  trendWindow: TrendWindow;
};

export type FilterKey = keyof HostSyncedState['filters'];

export type AppStateAction =
  | { type: 'setPage'; page: PageId }
  | { type: 'setDimensionId'; dimensionId?: DimensionId }
  | {
      type: 'setDimensionsScope';
      scope: DimensionsScopeKind;
      dimensionId?: DimensionId;
      configuredModelId?: string;
      configuredGroupId?: string;
    }
  | { type: 'setMetricId'; metricId?: string }
  | { type: 'setFilters'; filters: HostSyncedState['filters'] }
  | { type: 'setFilter'; key: FilterKey; value?: string }
  | { type: 'setTrendWindow'; trendWindow: TrendWindow }
  | { type: 'setRunNotesOpen'; open: boolean }
  | { type: 'setConfigurationOnboardingComplete'; complete: boolean }
  | { type: 'setBuiltInAutoRunAttempted'; attempted: boolean }
  | { type: 'setConfigurationOptionalSkipped'; skipped: boolean }
  | { type: 'replace'; state: HostSyncedState };

export type AppStateContextValue = {
  state: HostSyncedState;
  dispatch: (action: AppStateAction) => void;
};
