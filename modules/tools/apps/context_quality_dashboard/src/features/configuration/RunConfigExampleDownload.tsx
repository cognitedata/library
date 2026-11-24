import { Alert, AlertDescription, AlertTitle } from '@cognite/aura/components/alert';
import { Button } from '@cognite/aura/components/button';
import { IconFileText } from '@cognite/aura/icons';

import { downloadRunConfigExampleFile } from './runConfigFileExample';

type RunConfigExampleDownloadProps = {
  t: (key: string) => string;
};

/** Sample YAML help card; relies on Aura Alert’s built-in info icon (no second icon in the title). */
export function RunConfigExampleDownload({ t }: RunConfigExampleDownloadProps) {
  return (
    <Alert
      variant="info"
      alertStyle="card"
      className="p-4 sm:p-5"
      data-testid="run-config-example-card"
    >
      <div className="col-start-2 flex min-w-0 flex-col gap-3 sm:flex-row sm:items-center sm:justify-between sm:gap-6">
        <div className="min-w-0 space-y-1.5">
          <AlertTitle>
            <h3 className="text-base font-semibold leading-snug">{t('configuration.file.exampleCard.title')}</h3>
          </AlertTitle>
          <AlertDescription className="!mt-0">
            <p className="text-sm leading-relaxed text-muted-foreground">
              {t('configuration.file.exampleCard.body')}
            </p>
          </AlertDescription>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="inline-flex shrink-0 items-center gap-2 self-end sm:self-center"
          data-testid="run-config-example-download"
          onClick={downloadRunConfigExampleFile}
        >
          <IconFileText className="size-4 shrink-0" aria-hidden="true" />
          <span>{t('configuration.file.exampleDownload')}</span>
        </Button>
      </div>
    </Alert>
  );
}
