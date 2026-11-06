import { useCallback, useMemo } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { interpolate } from '../../i18n/interpolate';
import { useLanguage } from '../../i18n/useLanguage';
import { useRunLauncher } from '../runs/useRunLauncher';

import { deriveRunBannerState, type RunBannerState } from './runBannerState';
import { useQualityOperations } from './useQualityOperations';
import { useRunExecution } from './useRunExecution';

export type RunBannerViewModel = {
  banner: RunBannerState;
  title: string;
  body: string;
  line: string;
  dismiss: (() => void) | null;
  primaryActionLabel: string;
  dismissLabel: string;
  onPrimaryAction: (() => void) | null;
};

export function useRunBannerViewModel(): RunBannerViewModel {
  const { t } = useLanguage();
  const execution = useRunExecution();
  const launcher = useRunLauncher();
  const operations = useQualityOperations();
  const { dispatch } = useAppState();

  const configPhase = launcher !== null ? launcher.phase : execution.phase;
  const configStartError = launcher?.startError ?? execution.startError;
  const configTerminalDismissed = execution.configTerminalDismissed;
  const activeRunRequest = launcher?.request ?? null;

  const banner = useMemo(
    () =>
      deriveRunBannerState({
        configPhase,
        configStartError,
        configTerminalDismissed,
        activeRunRequest,
        operationsIsInProgress: operations.isInProgress,
        operationsIsRefreshing: operations.isRefreshing,
        operationsIsRerunning: operations.isRerunning,
        operationsRefreshLatchedSuccess: operations.refreshLatchedSuccess,
        operationsRerunLatchedSuccess: operations.rerunLatchedSuccess,
        operationsRefreshLatchedFailure: operations.refreshLatchedFailure,
        operationsRerunLatchedFailure: operations.rerunLatchedFailure,
        operationsTerminalDismissed: operations.operationsTerminalDismissed,
        backendRunInProgress: operations.runStatus?.inProgress === true,
      }),
    [configPhase, configStartError, configTerminalDismissed, activeRunRequest, operations]
  );

  const title = useMemo(() => {
    if (!banner.visible || banner.copy.titleKey === '') {
      return '';
    }
    return t(banner.copy.titleKey);
  }, [banner, t]);

  const body = useMemo(() => {
    if (!banner.visible || banner.copy.bodyKey === '') {
      return '';
    }
    const bodyTemplate = t(banner.copy.bodyKey);
    return banner.copy.bodyValues === undefined
      ? bodyTemplate
      : interpolate(bodyTemplate, banner.copy.bodyValues);
  }, [banner, t]);

  const line = useMemo(() => {
    if (title === '') {
      return '';
    }
    return body === '' ? title : `${title} — ${body}`;
  }, [body, title]);

  const dismiss = useCallback(() => {
    if (!banner.dismissible) {
      return;
    }
    if (banner.source === 'config') {
      execution.dismissConfigTerminal();
      return;
    }
    operations.dismissOperationsTerminal();
  }, [banner.dismissible, banner.source, execution, operations]);

  const onPrimaryAction = useCallback(() => {
    if (banner.primaryAction === 'viewResults') {
      dispatch({ type: 'setPage', page: 'overview' });
      execution.dismissConfigTerminal();
      operations.dismissOperationsTerminal();
      return;
    }
    if (banner.primaryAction === 'viewRunDetails') {
      dispatch({ type: 'setRunNotesOpen', open: true });
    }
  }, [banner.primaryAction, dispatch, execution, operations]);

  const primaryActionLabel = useMemo(() => {
    if (banner.primaryAction === 'viewResults') {
      return t('runBanner.action.viewResults');
    }
    if (banner.primaryAction === 'viewRunDetails') {
      return t('runBanner.action.viewRunDetails');
    }
    return '';
  }, [banner.primaryAction, t]);

  return {
    banner,
    title,
    body,
    line,
    dismiss: banner.dismissible ? dismiss : null,
    primaryActionLabel,
    dismissLabel: t('common.dismiss'),
    onPrimaryAction: banner.primaryAction === undefined ? null : onPrimaryAction,
  };
}
