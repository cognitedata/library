import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@cognite/aura/components/dialog';

import { interpolate } from '../../i18n/interpolate';

import type { RunConfigError, RunConfigModel, RunConfigResult } from './runConfigFile';

type RunConfigFileReviewDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  result: RunConfigResult | null;
  t: (key: string) => string;
};

function errorText(error: RunConfigError, t: (key: string) => string): string {
  return interpolate(t(`configuration.file.error.${error.code}`), { subject: error.subject ?? '' }).trim();
}

/** View -> properties, in the order the file lists them. */
function groupByView(links: readonly string[]): [string, string[]][] {
  const views = new Map<string, string[]>();
  for (const link of links) {
    const [view = '', property = ''] = link.split('.');
    views.set(view, [...(views.get(view) ?? []), property]);
  }
  return [...views.entries()];
}

export function RunConfigFileReviewDialog({
  open,
  onOpenChange,
  result,
  t,
}: RunConfigFileReviewDialogProps) {
  if (result === null) {
    return null;
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="max-h-[min(32rem,90vh)] max-w-2xl overflow-y-auto"
        showCloseButton={false}
        data-testid="run-config-review-dialog"
      >
        <DialogHeader>
          <DialogTitle>{t('configuration.file.reviewTitle')}</DialogTitle>
        </DialogHeader>

        {result.ok ? (
          <div className="space-y-4">
            <p className="text-sm text-success-foreground">
              {interpolate(t('configuration.file.valid'), {
                models: String(result.models.length),
                links: String(result.models.reduce((total, entry) => total + entry.links.length, 0)),
              })}
            </p>
            <Preview models={result.models} t={t} />
          </div>
        ) : (
          <div className="space-y-1" role="alert">
            <p className="text-sm text-destructive-foreground">{t('configuration.file.invalid')}</p>
            <ul className="list-disc pl-5 text-sm text-destructive-foreground">
              {result.errors.map((error, index) => (
                <li key={`${error.code}-${error.subject ?? ''}-${String(index)}`}>{errorText(error, t)}</li>
              ))}
            </ul>
          </div>
        )}

      </DialogContent>
    </Dialog>
  );
}

function Preview({ models, t }: { models: readonly RunConfigModel[]; t: (key: string) => string }) {
  return (
    <div className="space-y-3" data-testid="run-config-preview">
      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{t('configuration.file.preview')}</p>
      <div className="flex flex-col gap-4">
        {models.map((entry) => (
          <div
            key={entry.model}
            data-testid={`run-config-model-${entry.model}`}
            className="rounded-md border border-border bg-muted/30 p-4"
          >
            <div className="border-b border-border pb-3">
              <p className="text-xs font-medium text-muted-foreground">{t('configuration.latest.dataModel')}</p>
              <p className="mt-1 font-mono text-sm font-medium text-foreground">{entry.model}</p>
            </div>
            <ul className="mt-3 flex flex-col gap-2.5" aria-label={entry.model}>
              {groupByView(entry.links).map(([view, properties]) => (
                <li key={view} className="flex flex-wrap items-center gap-x-2 gap-y-1.5 text-sm">
                  <span className="font-medium text-foreground">{view}</span>
                  <span aria-hidden="true" className="text-muted-foreground">|</span>
                  <ul className="flex flex-wrap gap-1.5">
                    {properties.map((property) => (
                      <li
                        key={property}
                        className="rounded-md border border-border bg-background px-2 py-0.5 text-xs text-muted-foreground"
                      >
                        {property}
                      </li>
                    ))}
                  </ul>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}
