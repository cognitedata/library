import { describe, expect, it } from 'vitest';

import { DEFAULT_HOST_SYNCED_STATE } from '../../app/state/defaultState';
import { parseInitialState } from '../../app/state/parseInitialState';

import {
  deriveBuiltInOnboardingStatus,
  shouldAutoStartBuiltInRun,
} from './builtInOnboardingStatus';

describe(deriveBuiltInOnboardingStatus.name, () => {
  it('should report unavailable when runs cannot be started', () => {
    expect(
      deriveBuiltInOnboardingStatus({
        canRun: false,
        hasNoRunYet: true,
        configPhase: 'idle',
        configStartError: null,
        backendRunInProgress: false,
        builtInAutoRunAttempted: false,
      })
    ).toBe('unavailable');
  });

  it('should report running when the config phase is active', () => {
    expect(
      deriveBuiltInOnboardingStatus({
        canRun: true,
        hasNoRunYet: true,
        configPhase: 'running',
        configStartError: null,
        backendRunInProgress: false,
        builtInAutoRunAttempted: true,
      })
    ).toBe('running');
  });

  it('should report completed when a run has finished in the backend', () => {
    expect(
      deriveBuiltInOnboardingStatus({
        canRun: true,
        hasNoRunYet: false,
        configPhase: 'idle',
        configStartError: null,
        backendRunInProgress: false,
        builtInAutoRunAttempted: true,
      })
    ).toBe('completed');
  });
});

describe(shouldAutoStartBuiltInRun.name, () => {
  it('should not auto-start when already attempted or backend is in progress', () => {
    expect(
      shouldAutoStartBuiltInRun({
        canRun: true,
        hasNoRunYet: true,
        configPhase: 'idle',
        configStartError: null,
        backendRunInProgress: true,
        builtInAutoRunAttempted: false,
      })
    ).toBe(false);

    expect(
      shouldAutoStartBuiltInRun({
        canRun: true,
        hasNoRunYet: true,
        configPhase: 'idle',
        configStartError: null,
        backendRunInProgress: false,
        builtInAutoRunAttempted: true,
      })
    ).toBe(false);
  });

  it('should auto-start once when idle with no completed run', () => {
    expect(
      shouldAutoStartBuiltInRun({
        canRun: true,
        hasNoRunYet: true,
        configPhase: 'idle',
        configStartError: null,
        backendRunInProgress: false,
        builtInAutoRunAttempted: false,
      })
    ).toBe(true);
  });
});

describe('configuration onboarding host flags', () => {
  it('should restore onboarding flags from host state', () => {
    const parsed = parseInitialState(
      JSON.stringify({
        configurationOnboardingComplete: true,
        builtInAutoRunAttempted: true,
      })
    );
    expect(parsed.configurationOnboardingComplete).toBe(true);
    expect(parsed.builtInAutoRunAttempted).toBe(true);
    expect(parseInitialState(undefined)).toEqual({
      ...DEFAULT_HOST_SYNCED_STATE,
      filters: { ...DEFAULT_HOST_SYNCED_STATE.filters },
    });
  });
});
