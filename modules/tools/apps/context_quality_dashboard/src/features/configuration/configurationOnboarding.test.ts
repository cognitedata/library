import { describe, expect, it } from 'vitest';

import { DEFAULT_HOST_SYNCED_STATE } from '../../app/state/defaultState';

import { shouldShowConfigurationOnboarding } from './configurationOnboarding';

describe(shouldShowConfigurationOnboarding.name, () => {
  it('should hide onboarding after the user completes or skips it', () => {
    const state = { ...DEFAULT_HOST_SYNCED_STATE, configurationOnboardingComplete: true };
    expect(shouldShowConfigurationOnboarding(state, true, { DEV: false })).toBe(false);
  });

  it('should show onboarding when there is no completed run', () => {
    expect(shouldShowConfigurationOnboarding(DEFAULT_HOST_SYNCED_STATE, true, { DEV: false })).toBe(true);
  });

  it('should hide onboarding after the first run completes even before the completion flag is set', () => {
    expect(shouldShowConfigurationOnboarding(DEFAULT_HOST_SYNCED_STATE, false, { DEV: false })).toBe(false);
  });

  it('should honour the dev preview flag without an empty stream', () => {
    expect(
      shouldShowConfigurationOnboarding(DEFAULT_HOST_SYNCED_STATE, false, {
        DEV: true,
        VITE_DEV_FORCE_FIRST_RUN_ONBOARDING: 'true',
      })
    ).toBe(true);
  });
});
