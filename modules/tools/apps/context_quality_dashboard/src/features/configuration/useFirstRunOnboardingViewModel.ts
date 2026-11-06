import { useCallback, useEffect, useMemo, useRef } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { isBackendError } from '../../services/cdf/backendErrors';
import { isBuiltInAutoStartEnabled } from '../shell/firstRunOnboardingMode';
import { useQualityOperations } from '../shell/useQualityOperations';

import {
  deriveBuiltInOnboardingStatus,
  shouldAutoStartBuiltInRun,
  type BuiltInOnboardingStatus,
} from './builtInOnboardingStatus';
import { useRunConfigFileViewModel } from './useRunConfigFileViewModel';

export type FirstRunOnboardingStep = 'welcome';

export type FirstRunOnboardingViewModel = {
  step: FirstRunOnboardingStep;
  optionalChecksSkipped: boolean;
  builtInStatus: BuiltInOnboardingStatus;
  builtInStatusMessageKey: string;
  builtInStatusLabelKey: string;
  welcomeSubtitleKey: string;
  builtInFailureDetail: { key: string; message: string } | null;
  skip: () => void;
  goToOverview: () => void;
  autoStartEnabled: boolean;
  startBuiltIn: () => void;
  runAdditionalChecks: () => void;
  runAdditionalEnabled: boolean;
  runAdditionalInProgress: boolean;
  skipConfirmationBodyKey: string;
  fileViewModel: ReturnType<typeof useRunConfigFileViewModel>;
  openRunDetails: () => void;
  t: (key: string) => string;
  viewBuiltInResults: () => void;
};

function builtInStatusLabelKey(status: BuiltInOnboardingStatus, autoStartEnabled: boolean): string {
  switch (status) {
    case 'running':
      return 'configuration.firstRunOnboarding.builtIn.statusLabel.running';
    case 'completed':
      return 'configuration.firstRunOnboarding.builtIn.statusLabel.completed';
    case 'failed':
      return 'configuration.firstRunOnboarding.builtIn.statusLabel.failed';
    case 'unavailable':
      return 'configuration.firstRunOnboarding.builtIn.statusLabel.unavailable';
    case 'pending':
      return autoStartEnabled
        ? 'configuration.firstRunOnboarding.builtIn.statusLabel.pending'
        : 'configuration.firstRunOnboarding.builtIn.statusLabel.notStarted';
  }
}

function welcomeSubtitleKey(status: BuiltInOnboardingStatus, autoStartEnabled: boolean): string {
  switch (status) {
    case 'pending':
      return autoStartEnabled
        ? 'configuration.firstRunOnboarding.welcome.subtitle.running'
        : 'configuration.firstRunOnboarding.welcome.subtitle.notStarted';
    case 'running':
      return 'configuration.firstRunOnboarding.welcome.subtitle.running';
    case 'completed':
      return 'configuration.firstRunOnboarding.welcome.subtitle.completed';
    case 'failed':
      return 'configuration.firstRunOnboarding.welcome.subtitle.failed';
    case 'unavailable':
      return 'configuration.firstRunOnboarding.welcome.subtitle.unavailable';
  }
}

function builtInStatusMessageKey(status: BuiltInOnboardingStatus, autoStartEnabled: boolean): string {
  switch (status) {
    case 'running':
      return 'configuration.firstRunOnboarding.builtIn.status.running';
    case 'completed':
      return 'configuration.firstRunOnboarding.builtIn.status.completed';
    case 'failed':
      return 'configuration.firstRunOnboarding.builtIn.status.failed';
    case 'unavailable':
      return 'configuration.firstRun.noRun';
    case 'pending':
      return autoStartEnabled
        ? 'configuration.firstRunOnboarding.builtIn.status.pending'
        : 'configuration.firstRunOnboarding.builtIn.status.notStarted';
  }
}

function skipConfirmationBodyKey(status: BuiltInOnboardingStatus, autoStartEnabled: boolean): string {
  switch (status) {
    case 'pending':
      return autoStartEnabled
        ? 'configuration.firstRunOnboarding.skip.body.running'
        : 'configuration.firstRunOnboarding.skip.body.notStarted';
    case 'completed':
      return 'configuration.firstRunOnboarding.skip.body.completed';
    case 'failed':
      return 'configuration.firstRunOnboarding.skip.body.failed';
    case 'running':
      return 'configuration.firstRunOnboarding.skip.body.running';
    case 'unavailable':
      return 'configuration.firstRunOnboarding.skip.body.unavailable';
  }
}

export function useFirstRunOnboardingViewModel(): FirstRunOnboardingViewModel {
  const { state, dispatch } = useAppState();
  const fileViewModel = useRunConfigFileViewModel();
  const operations = useQualityOperations();
  const optionalChecksSkipped = state.configurationOptionalSkipped === true;

  const hasNoRunYet =
    isBackendError(operations.runStatusError) &&
    operations.runStatusError.kind === 'no-completed-run';

  const builtInAutoRunAttempted = state.builtInAutoRunAttempted === true;
  const backendRunInProgress = operations.runStatus?.inProgress === true;

  const statusInput = useMemo(
    () => ({
      canRun: fileViewModel.canRun,
      hasNoRunYet,
      configPhase: fileViewModel.phase,
      configStartError: fileViewModel.startError,
      backendRunInProgress,
      builtInAutoRunAttempted,
    }),
    [
      backendRunInProgress,
      builtInAutoRunAttempted,
      fileViewModel.canRun,
      fileViewModel.phase,
      fileViewModel.startError,
      hasNoRunYet,
    ]
  );

  const builtInStatus = useMemo(() => deriveBuiltInOnboardingStatus(statusInput), [statusInput]);

  const autoStartEnabled = isBuiltInAutoStartEnabled();
  const autoStartIssued = useRef(false);
  useEffect(() => {
    if (!autoStartEnabled || autoStartIssued.current || !shouldAutoStartBuiltInRun(statusInput)) {
      return;
    }
    autoStartIssued.current = true;
    dispatch({ type: 'setBuiltInAutoRunAttempted', attempted: true });
    fileViewModel.runBuiltIn();
  }, [autoStartEnabled, dispatch, fileViewModel, statusInput]);

  const startBuiltIn = useCallback(() => {
    dispatch({ type: 'setBuiltInAutoRunAttempted', attempted: true });
    fileViewModel.runBuiltIn();
  }, [dispatch, fileViewModel]);

  const completeOnboarding = useCallback(() => {
    dispatch({ type: 'setConfigurationOnboardingComplete', complete: true });
  }, [dispatch]);

  const skip = useCallback(() => {
    dispatch({ type: 'setConfigurationOptionalSkipped', skipped: true });
  }, [dispatch]);

  const goToOverview = useCallback(() => {
    completeOnboarding();
    dispatch({ type: 'setPage', page: 'overview' });
  }, [completeOnboarding, dispatch]);

  const runAdditionalInProgress =
    fileViewModel.phase === 'starting' || fileViewModel.phase === 'running';

  const runAdditionalEnabled =
    fileViewModel.result?.ok === true && fileViewModel.canRun && !runAdditionalInProgress;

  const runAdditionalChecks = useCallback(() => {
    if (!runAdditionalEnabled) {
      return;
    }
    fileViewModel.run();
    completeOnboarding();
  }, [completeOnboarding, fileViewModel, runAdditionalEnabled]);

  const builtInFailureDetail =
    builtInStatus === 'failed' && fileViewModel.startError !== null ? fileViewModel.startError : null;

  const openRunDetails = useCallback(() => {
    dispatch({ type: 'setRunNotesOpen', open: true });
  }, [dispatch]);

  const viewBuiltInResults = useCallback(() => {
    dispatch({ type: 'setPage', page: 'overview' });
  }, [dispatch]);

  return {
    step: 'welcome',
    optionalChecksSkipped,
    builtInStatus,
    builtInStatusMessageKey: builtInStatusMessageKey(builtInStatus, autoStartEnabled),
    builtInStatusLabelKey: builtInStatusLabelKey(builtInStatus, autoStartEnabled),
    welcomeSubtitleKey: welcomeSubtitleKey(builtInStatus, autoStartEnabled),
    builtInFailureDetail,
    skip,
    goToOverview,
    autoStartEnabled,
    startBuiltIn,
    runAdditionalChecks,
    runAdditionalEnabled,
    runAdditionalInProgress,
    skipConfirmationBodyKey: skipConfirmationBodyKey(builtInStatus, autoStartEnabled),
    fileViewModel,
    t: fileViewModel.t,
    openRunDetails,
    viewBuiltInResults,
  };
}
