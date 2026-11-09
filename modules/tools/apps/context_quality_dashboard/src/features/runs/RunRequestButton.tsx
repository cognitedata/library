import { Button } from '@cognite/aura/components/button';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@cognite/aura/components/tooltip';
import { useId, useMemo, useState } from 'react';

import { useLanguage } from '../../i18n/useLanguage';
import type { RunConfigModel } from '../configuration/runConfigFile';

import { ConfirmRunDialog } from './ConfirmRunDialog';
import { requestKey, requestNeedsConfigFile, type RunRequest } from './runRequest';
import { useRunLauncher } from './useRunLauncher';

type RunRequestButtonProps = {
  request: RunRequest;
  label: string;
  /** Shown on the button while this request is starting or running. */
  runningLabel?: string;
  /** What the configuration file asks for; null when there is no valid file. */
  models: readonly RunConfigModel[] | null;
  variant?: 'default' | 'outline';
  size?: 'default' | 'sm' | 'lg' | 'icon';
  /** When false, the button stays visible but cannot start a run (e.g. draft matches the latest run). */
  runEnabled?: boolean;
  /** Shown on hover when the button is disabled. */
  disabledTooltip?: string | null;
};

/**
 * A button that starts a run, but only after the person has read what it does and confirmed. While a run is going on it
 * does not start another: it says why.
 */
export function RunRequestButton({
  request,
  label,
  runningLabel = label,
  models,
  variant = 'outline',
  size = 'sm',
  runEnabled = true,
  disabledTooltip = null,
}: RunRequestButtonProps) {
  const { t } = useLanguage();
  const launcher = useRunLauncher();
  const [open, setOpen] = useState(false);
  const disabledReasonId = useId();

  const missingFile = launcher !== null && requestNeedsConfigFile(request) && models === null;
  const key = requestKey(request);
  const requestMatches =
    launcher !== null &&
    launcher.request !== null &&
    requestKey(launcher.request) === key;
  const inProgress =
    launcher !== null &&
    (launcher.phase === 'starting' || launcher.phase === 'running') &&
    requestMatches;
  const blocked =
    launcher !== null && launcher.busy ? (launcher.otherRunInProgress ? 'other' : 'own') : null;
  const isDisabled =
    launcher !== null && (missingFile || launcher.busy || !runEnabled);

  const disabledReason = useMemo((): string | null => {
    if (!isDisabled) {
      return null;
    }
    if (disabledTooltip !== null && disabledTooltip !== '') {
      return disabledTooltip;
    }
    if (missingFile) {
      return t('configuration.run.needsFile');
    }
    if (launcher !== null && launcher.busy) {
      return t('configuration.run.disabled.busy');
    }
    if (!runEnabled) {
      return t('configuration.run.disabled.generic');
    }
    return t('configuration.run.disabled.generic');
  }, [disabledTooltip, isDisabled, launcher, missingFile, runEnabled, t]);

  if (launcher === null) {
    return null;
  }

  if (inProgress) {
    return (
      <Button variant={variant} size={size} disabled data-testid={`run-button-${key}`}>
        <span className="inline-flex items-center gap-2">
          <span
            className="size-3.5 shrink-0 animate-spin rounded-full border-2 border-current border-t-transparent"
            aria-hidden
          />
          {runningLabel}
        </span>
      </Button>
    );
  }

  const button = (
    <Button
      variant={variant}
      size={size}
      disabled={isDisabled}
      onClick={() => setOpen(true)}
      data-testid={`run-button-${key}`}
      aria-describedby={disabledReason === null ? undefined : disabledReasonId}
    >
      {label}
    </Button>
  );

  return (
    <>
      {disabledReason === null ? (
        button
      ) : (
        <>
          <span id={disabledReasonId} className="sr-only">
            {disabledReason}
          </span>
          <TooltipProvider delayDuration={200}>
            <Tooltip>
              <TooltipTrigger
                render={
                  <span
                    className="inline-flex w-fit cursor-not-allowed"
                    data-testid={`run-button-${key}-tooltip-trigger`}
                  />
                }
              >
                {button}
              </TooltipTrigger>
              <TooltipContent
                className="max-w-sm text-pretty"
                data-testid={`run-button-${key}-disabled-tooltip`}
              >
                {disabledReason}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </>
      )}
      <ConfirmRunDialog
        open={open}
        onOpenChange={setOpen}
        request={request}
        blocked={blocked}
        onConfirm={() => launcher.start(request, models)}
      />
    </>
  );
}
