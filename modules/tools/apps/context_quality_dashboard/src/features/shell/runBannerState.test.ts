import { describe, expect, it } from 'vitest';

import { deriveRunBannerState, type DeriveRunBannerInput } from './runBannerState';

const IDLE_INPUT: DeriveRunBannerInput = {
  configPhase: 'idle',
  configStartError: null,
  configTerminalDismissed: false,
  activeRunRequest: null,
  operationsIsInProgress: false,
  operationsIsRefreshing: false,
  operationsIsRerunning: false,
  operationsRefreshLatchedSuccess: false,
  operationsRerunLatchedSuccess: false,
  operationsRefreshLatchedFailure: false,
  operationsRerunLatchedFailure: false,
  operationsTerminalDismissed: false,
  backendRunInProgress: false,
};

describe(deriveRunBannerState.name, () => {
  it('should hide when nothing is running or latched', () => {
    expect(deriveRunBannerState(IDLE_INPUT).visible).toBe(false);
  });

  it('should show config running while starting or polling', () => {
    expect(deriveRunBannerState({ ...IDLE_INPUT, configPhase: 'starting' }).copy.titleKey).toBe(
      'runBanner.config.running.title'
    );
    expect(deriveRunBannerState({ ...IDLE_INPUT, configPhase: 'running' }).variant).toBe('running');
  });

  it('should show configuration-file copy when running with an uploaded config', () => {
    expect(
      deriveRunBannerState({
        ...IDLE_INPUT,
        configPhase: 'running',
        activeRunRequest: { kind: 'everything' },
      }).copy.titleKey
    ).toBe('runBanner.configFile.running.title');
  });

  it('should prefer config running over latched refresh success', () => {
    const state = deriveRunBannerState({
      ...IDLE_INPUT,
      configPhase: 'running',
      operationsRefreshLatchedSuccess: true,
    });
    expect(state.source).toBe('config');
  });

  it('should show refresh running when refresh is pending', () => {
    expect(
      deriveRunBannerState({ ...IDLE_INPUT, operationsIsInProgress: true, operationsIsRefreshing: true }).source
    ).toBe('refresh');
  });

  it('should show config success until dismissed', () => {
    expect(deriveRunBannerState({ ...IDLE_INPUT, configPhase: 'completed' }).variant).toBe('success');
    expect(deriveRunBannerState({ ...IDLE_INPUT, configPhase: 'completed', configTerminalDismissed: true }).visible).toBe(
      false
    );
  });

  it('should show config failed until dismissed', () => {
    expect(deriveRunBannerState({ ...IDLE_INPUT, configPhase: 'failed' }).variant).toBe('failed');
  });

  it('should show start error as failed config banner', () => {
    const state = deriveRunBannerState({
      ...IDLE_INPUT,
      configPhase: 'not-started',
      configStartError: { key: 'configuration.run.error.other', message: 'boom' },
    });
    expect(state.variant).toBe('failed');
    expect(state.copy.bodyValues?.message).toBe('boom');
  });

  it('should show latched refresh success when idle', () => {
    expect(
      deriveRunBannerState({ ...IDLE_INPUT, operationsRefreshLatchedSuccess: true }).source
    ).toBe('refresh');
  });

  it('should hide latched terminal when operations banner dismissed', () => {
    expect(
      deriveRunBannerState({
        ...IDLE_INPUT,
        operationsRefreshLatchedSuccess: true,
        operationsTerminalDismissed: true,
      }).visible
    ).toBe(false);
  });

  it('should prefer refresh failure over refresh success when both latched', () => {
    expect(
      deriveRunBannerState({
        ...IDLE_INPUT,
        operationsRefreshLatchedSuccess: true,
        operationsRefreshLatchedFailure: true,
      }).variant
    ).toBe('failed');
  });
});
