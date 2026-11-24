import type { RunPhase } from '../configuration/useRunConfigFileViewModel';
import type { RunRequest } from '../runs/runRequest';

export type RunBannerVariant = 'running' | 'success' | 'failed';

export type RunBannerSource = 'config' | 'refresh' | 'rerun' | 'backend';

export type RunBannerCopy = {
  titleKey: string;
  bodyKey: string;
  bodyValues?: Record<string, string>;
};

export type RunBannerPrimaryAction = 'viewResults' | 'viewRunDetails';

export type RunBannerState = {
  visible: boolean;
  variant: RunBannerVariant;
  source: RunBannerSource;
  dismissible: boolean;
  copy: RunBannerCopy;
  primaryAction?: RunBannerPrimaryAction;
};

export type DeriveRunBannerInput = {
  configPhase: RunPhase;
  configStartError: { key: string; message: string } | null;
  configTerminalDismissed: boolean;
  /** The run started from Configuration or the run launcher, when known. */
  activeRunRequest: RunRequest | null;
  operationsIsInProgress: boolean;
  operationsIsRefreshing: boolean;
  operationsIsRerunning: boolean;
  operationsRefreshLatchedSuccess: boolean;
  operationsRerunLatchedSuccess: boolean;
  operationsRefreshLatchedFailure: boolean;
  operationsRerunLatchedFailure: boolean;
  operationsTerminalDismissed: boolean;
  backendRunInProgress: boolean;
};

const HIDDEN: RunBannerState = {
  visible: false,
  variant: 'running',
  source: 'config',
  dismissible: false,
  copy: { titleKey: '', bodyKey: '' },
};

function configBannerCopy(request: RunRequest | null, variant: RunBannerVariant): RunBannerCopy {
  const kind = request?.kind;
  if (kind === 'everything') {
    if (variant === 'running') {
      return {
        titleKey: 'runBanner.configFile.running.title',
        bodyKey: 'runBanner.configFile.running.body',
      };
    }
    if (variant === 'success') {
      return {
        titleKey: 'runBanner.configFile.success.title',
        bodyKey: 'runBanner.configFile.success.body',
      };
    }
    return {
      titleKey: 'runBanner.config.failed.title',
      bodyKey: 'runBanner.configFile.failed.body',
    };
  }
  if (kind === 'selection' || kind === 'configuredView') {
    const prefix = 'runBanner.rerun';
    if (variant === 'running') {
      return {
        titleKey: `${prefix}.running.title`,
        bodyKey: `${prefix}.running.body`,
      };
    }
    if (variant === 'success') {
      return {
        titleKey: 'runBanner.ops.success.title',
        bodyKey: 'runBanner.ops.success.body',
      };
    }
    return {
      titleKey: 'runBanner.ops.failed.title',
      bodyKey: 'runBanner.ops.failed.body',
    };
  }
  if (variant === 'running') {
    return {
      titleKey: 'runBanner.config.running.title',
      bodyKey: 'runBanner.config.running.body',
    };
  }
  if (variant === 'success') {
    return {
      titleKey: 'runBanner.config.success.title',
      bodyKey: 'runBanner.config.success.body',
    };
  }
  return {
    titleKey: 'runBanner.config.failed.title',
    bodyKey: 'runBanner.config.failed.body',
  };
}

function configRunning(request: RunRequest | null): RunBannerState {
  return {
    visible: true,
    variant: 'running',
    source: 'config',
    dismissible: false,
    copy: configBannerCopy(request, 'running'),
  };
}

function configSuccess(request: RunRequest | null): RunBannerState {
  return {
    visible: true,
    variant: 'success',
    source: 'config',
    dismissible: true,
    primaryAction: 'viewResults',
    copy: configBannerCopy(request, 'success'),
  };
}

function configFailed(request: RunRequest | null, bodyKey?: string): RunBannerState {
  const copy =
    bodyKey === undefined
      ? configBannerCopy(request, 'failed')
      : {
          titleKey: 'runBanner.config.failed.title',
          bodyKey,
        };
  return {
    visible: true,
    variant: 'failed',
    source: 'config',
    dismissible: true,
    primaryAction: 'viewRunDetails',
    copy,
  };
}

function opsRunning(source: RunBannerSource): RunBannerState {
  const prefix = source === 'rerun' ? 'runBanner.rerun' : 'runBanner.refresh';
  return {
    visible: true,
    variant: 'running',
    source,
    dismissible: false,
    copy: {
      titleKey: `${prefix}.running.title`,
      bodyKey: `${prefix}.running.body`,
    },
  };
}

function backendRunning(): RunBannerState {
  return {
    visible: true,
    variant: 'running',
    source: 'backend',
    dismissible: false,
    copy: {
      titleKey: 'runBanner.config.running.title',
      bodyKey: 'runBanner.config.running.body',
    },
  };
}

function opsSuccess(source: 'refresh' | 'rerun'): RunBannerState {
  return {
    visible: true,
    variant: 'success',
    source,
    dismissible: true,
    primaryAction: 'viewResults',
    copy: {
      titleKey: 'runBanner.ops.success.title',
      bodyKey: 'runBanner.ops.success.body',
    },
  };
}

function opsFailed(source: 'refresh' | 'rerun'): RunBannerState {
  return {
    visible: true,
    variant: 'failed',
    source,
    dismissible: true,
    primaryAction: 'viewRunDetails',
    copy: {
      titleKey: 'runBanner.ops.failed.title',
      bodyKey: 'runBanner.ops.failed.body',
    },
  };
}

/** Derives the global run banner from config-file execution and dashboard refresh/rerun state. */
export function deriveRunBannerState(input: DeriveRunBannerInput): RunBannerState {
  const configActive =
    input.configPhase === 'starting' || input.configPhase === 'running';

  if (configActive) {
    return configRunning(input.activeRunRequest);
  }

  if (input.operationsIsRefreshing) {
    return opsRunning('refresh');
  }

  if (input.operationsIsRerunning) {
    return opsRunning('rerun');
  }

  if (input.configPhase === 'completed' && !input.configTerminalDismissed) {
    return configSuccess(input.activeRunRequest);
  }

  if (input.configPhase === 'failed' && !input.configTerminalDismissed) {
    return configFailed(input.activeRunRequest);
  }

  if (
    input.configPhase === 'not-started' &&
    input.configStartError !== null &&
    !input.configTerminalDismissed
  ) {
    return {
      ...configFailed(input.activeRunRequest, 'runBanner.config.failedStart.body'),
      copy: {
        titleKey: 'runBanner.config.failed.title',
        bodyKey: input.configStartError.key,
        bodyValues: { message: input.configStartError.message },
      },
    };
  }

  if (!input.operationsTerminalDismissed) {
    if (input.operationsRefreshLatchedFailure) {
      return opsFailed('refresh');
    }
    if (input.operationsRerunLatchedFailure) {
      return opsFailed('rerun');
    }
    if (input.operationsRefreshLatchedSuccess) {
      return opsSuccess('refresh');
    }
    if (input.operationsRerunLatchedSuccess) {
      return opsSuccess('rerun');
    }
  }

  if (input.backendRunInProgress && input.operationsIsInProgress) {
    return backendRunning();
  }

  return HIDDEN;
}
