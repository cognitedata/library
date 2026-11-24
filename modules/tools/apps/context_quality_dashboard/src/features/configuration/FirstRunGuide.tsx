import { Alert, AlertDescription, AlertTitle } from '@cognite/aura/components/alert';

type FirstRunGuideProps = {
  /** No quality run has finished yet, so the Overview has nothing to show. */
  noRunYet: boolean;
  /** Whether this build can start a run. Without it, uploading a file starts nothing and the steps cannot be done here. */
  canRun: boolean;
  t: (key: string) => string;
};

/**
 * Explains what the configuration file is for while nothing has used it yet: the built-in checks need no file, the file
 * is how the project's own data-model links are added, and what to do in what order. It never claims the dashboard
 * deploys anything.
 */
export function FirstRunGuide({ noRunYet, canRun, t }: FirstRunGuideProps) {
  return (
    <Alert variant="info" alertStyle="card" data-testid="first-run-guide">
      <AlertTitle>
        <h3 className="text-base font-semibold">{t('configuration.firstRun.title')}</h3>
      </AlertTitle>
      <AlertDescription className="space-y-2">
        <p>{t('configuration.firstRun.body')}</p>
        {noRunYet ? (
          <p data-testid="first-run-guide-no-run">{t('configuration.firstRun.noResultsYet')}</p>
        ) : (
          <p data-testid="first-run-guide-built-in-only">{t('configuration.firstRun.builtInOnlyHint')}</p>
        )}
        {canRun ? (
          <div>
            <p className="text-sm font-medium">{t('configuration.firstRun.steps.title')}</p>
            <ol className="mt-1 list-decimal space-y-1 pl-5" data-testid="first-run-guide-steps">
              <li>{t('configuration.firstRun.steps.1')}</li>
              <li>{t('configuration.firstRun.steps.2')}</li>
              <li>{t('configuration.firstRun.steps.3')}</li>
            </ol>
          </div>
        ) : (
          <p data-testid="first-run-guide-cannot-run">{t('configuration.firstRun.noRun')}</p>
        )}
      </AlertDescription>
    </Alert>
  );
}
