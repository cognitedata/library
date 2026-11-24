import { useQuery } from '@tanstack/react-query';
import { useCallback, useContext, useMemo, useSyncExternalStore } from 'react';

import type { LanguageCode } from '../../i18n/messages';
import { useLanguage } from '../../i18n/useLanguage';
import { useQualityDashboardService, useRunService } from '../../services/useServices';
import type { RunConfigurationDto } from '../../types';
import type { RunPhase } from '../runs/runLauncherContext';
import { useRunLauncher } from '../runs/useRunLauncher';

import {
  getRunCustomConfigDisabledReasonKey,
  hasPendingRunConfig,
  parseRunConfigFile,
  type RunConfigModel,
  type RunConfigResult,
} from './runConfigFile';
import { RunConfigFileContext } from './runConfigFileContext';

export type { RunPhase } from '../runs/runLauncherContext';

export type RunConfigFileViewModel = {
  fileName: string | null;
  /** Draft text in the store; empty string when nothing is stored. */
  text: string;
  /** What the stored file reads as; null while there is no file. */
  result: RunConfigResult | null;
  upload: (file: File) => Promise<void>;
  /** Replaces the draft text (paste / edit). Uses the current file name, or config.yaml when none. */
  setText: (text: string) => void;
  remove: () => void;
  /** False when the deployment names no workflow: the page then only keeps the file and offers no Run buttons. */
  canRun: boolean;
  /** What the valid file asks for, to start a run with; null while there is no valid file. */
  models: readonly RunConfigModel[] | null;
  run: () => void;
  /** Starts a run with built-in checks only (all dimensions, no configured metrics). */
  runBuiltIn: () => void;
  runBuiltInEnabled: boolean;
  runEnabled: boolean;
  /** Tooltip when Run custom configuration file is disabled; null when enabled or no reason. */
  runDisabledTooltip: string | null;
  /** Open the configuration review for the current valid draft (after a successful run, including a prior one). */
  showViewConfiguration: boolean;
  phase: RunPhase;
  /** Why the run could not be started, as a translation key plus the message the API gave. */
  startError: { key: string; message: string } | null;
  /** What the latest run was started with; undefined while it is read or when it cannot be read, null before any run. */
  latestRunConfiguration: RunConfigurationDto | null | undefined;
  language: LanguageCode;
  t: (key: string) => string;
};

export function useRunConfigFileViewModel(): RunConfigFileViewModel {
  const { store } = useContext(RunConfigFileContext);
  const { t, language } = useLanguage();
  const runService = useRunService();
  const launcher = useRunLauncher();
  const qualityDashboardService = useQualityDashboardService();
  const file = useSyncExternalStore(store.subscribe, store.getSnapshot);

  const latest = useQuery({
    queryKey: ['latestRunConfiguration'],
    queryFn: () => qualityDashboardService.getLatestRunConfiguration(),
  });

  const result = useMemo(() => (file === null ? null : parseRunConfigFile(file.text)), [file]);

  const upload = useCallback(
    async (selected: File): Promise<void> => {
      const text = await selected.text().catch(() => '');
      store.save({ fileName: selected.name, text });
    },
    [store]
  );

  const setText = useCallback(
    (text: string): void => {
      store.save({ fileName: file?.fileName ?? 'config.yaml', text });
    },
    [file?.fileName, store]
  );

  const models = result?.ok === true ? result.models : null;
  const configChangedSinceLatestRun =
    models !== null && hasPendingRunConfig(models, latest.data);
  const phase = launcher?.phase ?? 'idle';
  const showViewConfiguration = result?.ok === true;

  const runInProgress = phase === 'starting' || phase === 'running';
  const busy = launcher?.busy === true;

  const run = useCallback(() => {
    if (launcher === null || models === null || !configChangedSinceLatestRun || busy || runInProgress) {
      return;
    }
    launcher.start({ kind: 'everything' }, models);
  }, [busy, configChangedSinceLatestRun, launcher, models, runInProgress]);

  const runBuiltIn = useCallback(() => {
    if (launcher === null || runService === null || busy || runInProgress) {
      return;
    }
    launcher.start({ kind: 'dimensions' }, null);
  }, [busy, launcher, runInProgress, runService]);

  const runDisabledReasonKey = useMemo(
    () =>
      getRunCustomConfigDisabledReasonKey({
        models,
        configChangedSinceLatestRun,
        busy,
        runInProgress,
        otherRunInProgress: launcher?.otherRunInProgress === true,
        latestRunConfiguration: latest.data,
      }),
    [
      busy,
      configChangedSinceLatestRun,
      latest.data,
      launcher?.otherRunInProgress,
      models,
      runInProgress,
    ]
  );

  const runDisabledTooltip =
    runDisabledReasonKey === null ? null : t(runDisabledReasonKey);

  return {
    fileName: file?.fileName ?? null,
    text: file?.text ?? '',
    result,
    upload,
    setText,
    remove: store.clear,
    canRun: runService !== null && launcher !== null,
    models,
    run,
    runBuiltIn,
    runBuiltInEnabled: runService !== null && launcher !== null && !busy && !runInProgress,
    runEnabled: configChangedSinceLatestRun && !busy && !runInProgress,
    runDisabledTooltip,
    showViewConfiguration,
    phase,
    startError: launcher?.startError ?? null,
    latestRunConfiguration: latest.data,
    language,
    t,
  };
}
