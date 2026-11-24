import { describe, expect, it } from 'vitest';

import { appStateReducer } from './appStateReducer';
import { DEFAULT_HOST_SYNCED_STATE } from './defaultState';
import type { HostSyncedState } from './types';

describe(appStateReducer.name, () => {
  it('updates the space filter and clears legacy slice keys when space changes', () => {
    let state: HostSyncedState = {
      ...DEFAULT_HOST_SYNCED_STATE,
      filters: { divisionId: 'prod-a', disciplineId: 'mechanical' },
    };

    state = appStateReducer(state, {
      type: 'setFilter',
      key: 'facilityId',
      value: 'demo-north',
    });

    expect(state.filters).toEqual({
      facilityId: 'demo-north',
    });

    state = appStateReducer(state, {
      type: 'setFilter',
      key: 'facilityId',
      value: undefined,
    });

    expect(state.filters).toEqual({});
  });

  it('updates trend window state', () => {
    const next = appStateReducer(DEFAULT_HOST_SYNCED_STATE, {
      type: 'setTrendWindow',
      trendWindow: '7d',
    });

    expect(next.trendWindow).toBe('7d');
  });

});

describe('run notes panel state', () => {
  it('opens and closes the run notes panel, and closed is the absent state', () => {
    const opened = appStateReducer(DEFAULT_HOST_SYNCED_STATE, { type: 'setRunNotesOpen', open: true });
    const closed = appStateReducer(opened, { type: 'setRunNotesOpen', open: false });

    expect(opened.runNotesOpen).toBe(true);
    expect(closed.runNotesOpen).toBeUndefined();
  });
});

describe('configuration onboarding state', () => {
  it('persists completion and built-in auto-start flags', () => {
    const completed = appStateReducer(DEFAULT_HOST_SYNCED_STATE, {
      type: 'setConfigurationOnboardingComplete',
      complete: true,
    });
    const attempted = appStateReducer(completed, {
      type: 'setBuiltInAutoRunAttempted',
      attempted: true,
    });

    expect(completed.configurationOnboardingComplete).toBe(true);
    expect(attempted.builtInAutoRunAttempted).toBe(true);
  });

  it('persists optional checks skipped during onboarding', () => {
    const skipped = appStateReducer(DEFAULT_HOST_SYNCED_STATE, {
      type: 'setConfigurationOptionalSkipped',
      skipped: true,
    });
    expect(skipped.configurationOptionalSkipped).toBe(true);
  });

  it('clears optional skipped when onboarding is marked complete', () => {
    const skipped = appStateReducer(DEFAULT_HOST_SYNCED_STATE, {
      type: 'setConfigurationOptionalSkipped',
      skipped: true,
    });
    const completed = appStateReducer(skipped, {
      type: 'setConfigurationOnboardingComplete',
      complete: true,
    });
    expect(completed.configurationOnboardingComplete).toBe(true);
    expect(completed.configurationOptionalSkipped).toBeUndefined();
  });
});
