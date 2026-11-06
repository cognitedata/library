import { Button } from '@cognite/aura/components/button';

import { useAppState } from '../../app/state/useAppState';
import { useLanguage } from '../../i18n/useLanguage';
import { DEFAULT_DIMENSION_ID } from '../dimension/useDimensionViewModel';
import { configuredGroupRunRequest } from '../runs/configuredGroupRunRequest';
import { fill } from '../runs/fill';
import { RunRequestButton } from '../runs/RunRequestButton';
import { useRunLauncher } from '../runs/useRunLauncher';

import { formatLastSuccessfulRun } from './formatLastSuccessfulRun';
import { formatRunScopeKey } from './formatRunScope';
import { useConfiguredRunGroup } from './useConfiguredRunGroup';
import { useQualityOperations } from './useQualityOperations';

export function RunStatusIndicator() {
  const { state } = useAppState();
  const { t, language } = useLanguage();
  const operations = useQualityOperations();
  const onDimensionsPage = state.page === 'dimensions';
  const dimensionsScope = state.dimensionsScope ?? 'builtin';
  const dimensionId = state.dimensionId ?? DEFAULT_DIMENSION_ID;
  const showBuiltinDimensionRunButton = onDimensionsPage && dimensionsScope === 'builtin';
  const showConfiguredViewRunButton =
    onDimensionsPage && dimensionsScope === 'configured' && state.configuredGroupId !== undefined;

  const configuredGroup = useConfiguredRunGroup(showConfiguredViewRunButton, state.configuredGroupId);

  const configuredRun =
    configuredGroup !== undefined && configuredGroup.metrics.length > 0
      ? configuredGroupRunRequest(configuredGroup)
      : null;

  const launcher = useRunLauncher();

  // A dimension's page says when that dimension was last measured: a run can measure some dimensions only.
  const lastRunAt = onDimensionsPage
    ? (operations.runStatus?.dimensionRuns?.[dimensionId] ?? operations.runStatus?.lastSuccessAt)
    : operations.runStatus?.lastSuccessAt;
  const lastSuccessfulRunText =
    lastRunAt === undefined
      ? null
      : `${t('runStatus.lastSuccessfulRun')}: ${formatLastSuccessfulRun(
          lastRunAt,
          language === 'ja' ? 'ja-JP' : 'en-GB'
        )}`;

  const scopeLabel =
    operations.runStatus?.inProgress && operations.runStatus.scope.kind !== 'all'
      ? t(formatRunScopeKey(operations.runStatus.scope))
      : null;

  // Refresh only reads the latest runs again; starting a run is a separate, confirmed button
  const refreshLabel = operations.isRefreshing ? t('runStatus.refreshing') : t('runStatus.refresh');

  const handleRefresh = () => {
    operations.refresh();
  };

  return (
    <div
      aria-label={t('runStatus.label')}
      className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground"
      data-testid="run-status-indicator"
    >
      {lastSuccessfulRunText === null ? (
        <span data-testid="run-status-text">{t('runStatus.ready')}</span>
      ) : (
        <span data-testid="run-status-text">{lastSuccessfulRunText}</span>
      )}
      {scopeLabel ? (
        <span className="text-muted-foreground/80" data-testid="run-status-scope">
          ({scopeLabel})
        </span>
      ) : null}
      <Button
        variant="outline"
        size="sm"
        onClick={handleRefresh}
        disabled={operations.isInProgress}
        data-testid="run-status-refresh"
      >
        {refreshLabel}
      </Button>
      {showBuiltinDimensionRunButton ? (
        <RunRequestButton
          request={{ kind: 'dimension', dimensionId }}
          label={fill(t('run.button.dimension'), { name: t(`dimension.${dimensionId}`) })}
          models={null}
        />
      ) : null}
      {showConfiguredViewRunButton && configuredRun !== null && configuredGroup !== undefined ? (
        <RunRequestButton
          request={configuredRun.request}
          models={configuredRun.models}
          label={fill(t('run.button.configuredView'), { name: configuredGroup.viewLabel })}
        />
      ) : null}
      {launcher?.otherRunInProgress === true ? (
        <span className="text-muted-foreground/80" data-testid="run-status-busy">
          {t('run.busy.hint')}
        </span>
      ) : null}
    </div>
  );
}
