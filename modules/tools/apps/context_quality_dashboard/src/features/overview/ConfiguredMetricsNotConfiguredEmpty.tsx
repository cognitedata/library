import { Button } from '@cognite/aura/components/button';
import {
  EmptyState,
  EmptyStateDescription,
  EmptyStateTitle,
} from '@cognite/aura/components/empty-state';

type ConfiguredMetricsNotConfiguredEmptyProps = {
  canStartRun: boolean;
  onOpenConfiguration: () => void;
  t: (key: string) => string;
};

export function ConfiguredMetricsNotConfiguredEmpty({
  canStartRun,
  onOpenConfiguration,
  t,
}: ConfiguredMetricsNotConfiguredEmptyProps) {
  return (
    <EmptyState className="max-w-lg" data-testid="configured-metrics-not-configured">
      <EmptyStateTitle>{t('overview.configured.notConfigured.title')}</EmptyStateTitle>
      <EmptyStateDescription className="space-y-2">
        <p>{t('overview.configured.notConfigured.body')}</p>
        {canStartRun ? null : (
          <p className="text-sm">{t('overview.configured.notConfigured.hintNoRun')}</p>
        )}
      </EmptyStateDescription>
      <Button size="sm" onClick={onOpenConfiguration} data-testid="configured-metrics-open-configuration">
        {t('overview.configured.notConfigured.button')}
      </Button>
    </EmptyState>
  );
}
