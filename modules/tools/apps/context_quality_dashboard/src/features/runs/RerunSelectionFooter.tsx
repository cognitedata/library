import { Alert, AlertActions, AlertDescription } from '@cognite/aura/components/alert';
import { Button } from '@cognite/aura/components/button';
import { IconCircleCheckFilled, IconX } from '@cognite/aura/icons';
import { useState } from 'react';

import { interpolate } from '../../i18n/interpolate';

import { ConfirmRunDialog } from './ConfirmRunDialog';
import type { RerunViewModel } from './useRerunViewModel';

/** Compact selection counts and run action at the bottom of the selection card. */
export function RerunSelectionFooter({ viewModel }: { viewModel: RerunViewModel }) {
  const { t, request } = viewModel;
  const [open, setOpen] = useState(false);
  const inProgress = viewModel.phase === 'starting' || viewModel.phase === 'running';
  const runBusy = viewModel.busy;
  const failed = viewModel.phase === 'failed' && viewModel.lastRequested !== null;
  const blocked = viewModel.busy ? (viewModel.otherRunInProgress ? 'other' : 'own') : null;

  return (
    <>
      <div
        className="w-full min-w-0 shrink-0 overflow-hidden border-t border-border"
        data-testid="rerun-selection-footer"
      >
        {failed && viewModel.lastRequested !== null ? (
          <div className="box-border w-full px-5 pt-3">
            <Alert variant="error" className="w-full max-w-full" data-testid="rerun-failed-items">
              <AlertDescription>
                <p>{t('rerun.summary.failed')}</p>
                <ul className="mt-1 list-disc pl-4">
                  {viewModel.lastRequested.dimensionIds.map((id) => (
                    <li key={id}>{t(`dimension.${id}`)}</li>
                  ))}
                  {viewModel.lastRequested.links.map((entry) => (
                    <li key={`${entry.model}|${entry.link}`}>{entry.link.replace('.', ' → ')}</li>
                  ))}
                </ul>
              </AlertDescription>
            </Alert>
          </div>
        ) : null}
        {viewModel.startError !== null && viewModel.phase === 'not-started' ? (
          <div className="box-border w-full px-5 pt-3">
            <Alert variant="error" role="alert" className="w-full max-w-full">
              <AlertDescription>
                {interpolate(t(viewModel.startError.key), { message: viewModel.startError.message })}
              </AlertDescription>
            </Alert>
          </div>
        ) : null}
        {inProgress || runBusy ? (
          <div className="box-border w-full px-5 pt-3">
            <Alert
              variant="info"
              alertStyle="card"
              className="w-full max-w-full"
              role="status"
              data-testid="rerun-summary-busy"
            >
              <AlertDescription>{t('rerun.summary.busy')}</AlertDescription>
            </Alert>
          </div>
        ) : null}
        <div className="flex w-full min-w-0 items-center justify-between gap-4 px-5 py-4">
          <div className="min-w-0 flex-1 space-y-0.5" aria-live="polite">
            <p className="text-sm font-medium text-foreground" data-testid="rerun-summary-total">
              {viewModel.selectedTotal === 0
                ? t('rerun.summary.none')
                : interpolate(t('rerun.summary.total'), { count: String(viewModel.selectedTotal) })}
            </p>
            <p className="text-xs text-muted-foreground">
              <span data-testid="rerun-summary-dimensions">
                {t('rerun.summary.dimensions')}:{' '}
                {interpolate(t('rerun.summary.counts'), {
                  selected: String(viewModel.selectedDimensions.length),
                  total: String(viewModel.dimensionIds.length),
                })}
              </span>
              <span aria-hidden>{' · '}</span>
              <span data-testid="rerun-summary-metrics">
                {t('rerun.summary.metrics')}:{' '}
                {interpolate(t('rerun.summary.counts'), {
                  selected: String(viewModel.selectedMetrics.length),
                  total: String(viewModel.totalMetrics),
                })}
              </span>
            </p>
            {!viewModel.runAvailable ? (
              <p className="text-xs text-muted-foreground">{t('rerun.summary.noWorkflow')}</p>
            ) : null}
          </div>

          <Button
            className="ml-auto shrink-0"
            disabled={!viewModel.canRun || runBusy}
            onClick={() => setOpen(true)}
            data-testid="rerun-run-selected"
          >
            {runBusy ? (
              <span className="inline-flex items-center gap-2">
                <span
                  className="size-3.5 shrink-0 animate-spin rounded-full border-2 border-current border-t-transparent"
                  aria-hidden
                />
                {t('configuration.run.runningButton')}
              </span>
            ) : (
              viewModel.selectedTotal === 0
                ? t('rerun.summary.runNone')
                : interpolate(t('rerun.summary.run'), { count: String(viewModel.selectedTotal) })
            )}
          </Button>
        </div>
        <RunStatusMessage runBanner={viewModel.runBanner} />
      </div>

      {request !== null ? (
        <ConfirmRunDialog
          open={open}
          onOpenChange={setOpen}
          request={request}
          blocked={blocked}
          onConfirm={viewModel.start}
        />
      ) : null}
    </>
  );
}

/** The outcome of the latest run, below the run button, so it is seen where the run was started. */
function RunStatusMessage({ runBanner }: { runBanner: RerunViewModel['runBanner'] }) {
  const { banner, title, body, dismiss, dismissLabel, primaryActionLabel, onPrimaryAction } = runBanner;
  if (!banner.visible) {
    return null;
  }
  const success = banner.variant === 'success';
  return (
    <div className="box-border w-full px-5 pb-4">
      <Alert
        variant={banner.variant === 'running' ? 'info' : banner.variant === 'failed' ? 'error' : 'default'}
        alertStyle="card"
        role="status"
        icon={success ? <IconCircleCheckFilled className="size-4 text-success-foreground" aria-hidden /> : undefined}
        className={`w-full max-w-full ${success ? 'border-l-4 border-l-success-foreground' : ''}`}
        data-testid="rerun-run-status"
      >
        <AlertDescription>
          <span className="font-semibold text-foreground">{title}</span>
          {body === '' ? null : <span className="text-secondary-foreground">{` — ${body}`}</span>}
        </AlertDescription>
        {dismiss === null && onPrimaryAction === null ? null : (
          <AlertActions>
            {onPrimaryAction === null ? null : (
              <Button type="button" variant="ghost" size="sm" className="shrink-0" onClick={onPrimaryAction}>
                {primaryActionLabel}
              </Button>
            )}
            {dismiss === null ? null : (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="size-7 shrink-0 p-0"
                aria-label={dismissLabel}
                onClick={dismiss}
              >
                <IconX className="size-4" aria-hidden />
              </Button>
            )}
          </AlertActions>
        )}
      </Alert>
    </div>
  );
}
