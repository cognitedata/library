import { Skeleton } from '@cognite/aura/components/skeleton';

import { useBackend } from '../../services/useServices';
import { UnavailablePanel } from '../backend/UnavailablePanel';
import { sectionHeadingClassName, sectionHeadingDestructiveClassName } from '../shell/sectionHeading';

import { AlertSummary } from './AlertSummary';
import { AlertTable } from './AlertTable';
import { useAlertsViewModel } from './useAlertsViewModel';

export function AlertsPage() {
  const backend = useBackend();
  if (!backend.capabilities.alerts) {
    return <UnavailablePanel messageKey="backend.unavailable.alerts" />;
  }
  return <AlertsContent />;
}

function AlertsContent() {
  const viewModel = useAlertsViewModel();

  if (viewModel.isLoading) {
    return (
      <section aria-label={viewModel.t('alerts.loading')} className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-40 w-full" />
      </section>
    );
  }

  if (viewModel.isError) {
    return (
      <section className="rounded-md border border-destructive-background bg-destructive-muted-background p-4">
        <h2 className={sectionHeadingDestructiveClassName}>{viewModel.t('alerts.error')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('alerts.errorHint')}</p>
      </section>
    );
  }

  if (viewModel.isEmpty) {
    return (
      <section className="rounded-md border border-border p-6 text-center" data-testid="alerts-empty">
        <h2 className={sectionHeadingClassName}>{viewModel.t('alerts.noAlerts')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('alerts.noAlertsForScope')}</p>
      </section>
    );
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-lg font-semibold">{viewModel.t('alerts.title')}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('alerts.description')}</p>
      </header>
      <AlertSummary viewModel={viewModel} />
      <AlertTable viewModel={viewModel} />
    </div>
  );
}
