import { Alert, AlertActions, AlertDescription, AlertTitle } from '@cognite/aura/components/alert';
import { Button } from '@cognite/aura/components/button';

import { useLanguage } from '../../i18n/useLanguage';

import { presentBackendError } from './backendErrorPresentation';

type BackendErrorPanelProps = {
  error: unknown;
  onRetry?: () => void;
  /** Opens the Configuration page. Only offered for a failure that Configuration can help with (no run yet). */
  onGoToConfiguration?: () => void;
  /** Whether this build can start a run. The "no run yet" steps say who has to start one when it cannot. */
  canStartRun?: boolean;
};

/**
 * Explains why the backend could not be read, in order: what is wrong, why, and the steps to take. It never offers to
 * create anything. "No run yet" is informational, since that is where a new user starts; the rest are failures.
 */
export function BackendErrorPanel({ error, onRetry, onGoToConfiguration, canStartRun = true }: BackendErrorPanelProps) {
  const { t } = useLanguage();
  const presentation = presentBackendError(error, { canStartRun });
  const showConfiguration = presentation.primaryAction === 'configuration' && onGoToConfiguration !== undefined;
  const showRetry = presentation.canRetry && onRetry !== undefined;

  return (
    <Alert
      role="alert"
      variant={presentation.tone}
      alertStyle="card"
      className="space-y-3"
      data-testid="backend-error"
      data-tone={presentation.tone}
    >
      <AlertTitle>
        <h2 className="text-base font-semibold">{t(presentation.titleKey)}</h2>
      </AlertTitle>
      <AlertDescription className="space-y-3">
        <p>{t(presentation.hintKey)}</p>
        {presentation.stepKeys.length > 0 ? (
          <div data-testid="backend-error-steps">
            <p className="text-sm font-medium">{t('backend.error.nextSteps')}</p>
            <ol className="mt-1 list-decimal space-y-1 pl-5">
              {presentation.stepKeys.map((key) => (
                <li key={key}>{t(key)}</li>
              ))}
            </ol>
          </div>
        ) : null}
        <details className="text-xs text-muted-foreground">
          <summary>{t('backend.error.details')}</summary>
          <p className="mt-1 break-words font-mono" data-testid="backend-error-details">
            {presentation.details}
          </p>
        </details>
      </AlertDescription>
      {showConfiguration || showRetry ? (
        <AlertActions>
          {showConfiguration ? (
            <Button size="sm" onClick={onGoToConfiguration} data-testid="backend-error-configuration">
              {t('backend.error.goToConfiguration')}
            </Button>
          ) : null}
          {showRetry ? (
            <Button variant="outline" size="sm" onClick={onRetry} data-testid="backend-error-retry">
              {t('backend.error.retry')}
            </Button>
          ) : null}
        </AlertActions>
      ) : null}
    </Alert>
  );
}
