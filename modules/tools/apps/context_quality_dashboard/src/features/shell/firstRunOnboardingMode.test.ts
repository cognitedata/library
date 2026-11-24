import { describe, expect, it } from 'vitest';

import {
  isBuiltInAutoStartEnabled,
  isFirstRunOnboardingPreview,
  shouldShowFirstRunOnboarding,
} from './firstRunOnboardingMode';

describe(isFirstRunOnboardingPreview.name, () => {
  it('is true only in dev when the preview flag is set', () => {
    expect(isFirstRunOnboardingPreview({ DEV: true, VITE_DEV_FORCE_FIRST_RUN_ONBOARDING: 'true' })).toBe(true);
    expect(isFirstRunOnboardingPreview({ DEV: false, VITE_DEV_FORCE_FIRST_RUN_ONBOARDING: 'true' })).toBe(false);
    expect(isFirstRunOnboardingPreview({ DEV: true, VITE_DEV_FORCE_FIRST_RUN_ONBOARDING: 'false' })).toBe(false);
  });
});

describe(shouldShowFirstRunOnboarding.name, () => {
  it('is true when there is no completed run or preview is forced', () => {
    expect(shouldShowFirstRunOnboarding(true, { DEV: false })).toBe(true);
    expect(shouldShowFirstRunOnboarding(false, { DEV: true, VITE_DEV_FORCE_FIRST_RUN_ONBOARDING: 'true' })).toBe(
      true
    );
    expect(shouldShowFirstRunOnboarding(false, { DEV: true })).toBe(false);
  });
});

describe(isBuiltInAutoStartEnabled.name, () => {
  it('is off unless the build sets the flag to true', () => {
    expect(isBuiltInAutoStartEnabled({})).toBe(false);
    expect(isBuiltInAutoStartEnabled({ VITE_CQ_AUTO_START_FIRST_RUN: 'false' })).toBe(false);
    expect(isBuiltInAutoStartEnabled({ VITE_CQ_AUTO_START_FIRST_RUN: 'true' })).toBe(true);
  });
});
