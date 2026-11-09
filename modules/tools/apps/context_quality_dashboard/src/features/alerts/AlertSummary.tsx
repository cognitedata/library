import { Badge } from '@cognite/aura/components/badge';

import type { AlertsViewModel } from './useAlertsViewModel';

type AlertSummaryProps = {
  viewModel: AlertsViewModel;
};

export function AlertSummary({ viewModel }: AlertSummaryProps) {
  const summary = viewModel.summary;
  if (summary === undefined) {
    return null;
  }

  return (
    <section
      aria-label={viewModel.t('alerts.summary')}
      className="grid gap-3 rounded-md border border-border bg-muted-background p-4 sm:grid-cols-4"
      data-testid="alert-summary"
    >
      <SummaryItem label={viewModel.t('alerts.total')} value={summary.total} />
      <SummaryItem
        label={viewModel.t('alerts.severity.critical')}
        value={summary.critical}
        variant="error"
      />
      <SummaryItem
        label={viewModel.t('alerts.severity.warning')}
        value={summary.warning}
        variant="warning"
      />
      <SummaryItem
        label={viewModel.t('alerts.severity.informational')}
        value={summary.informational}
        variant="secondary"
      />
    </section>
  );
}

type SummaryItemProps = {
  label: string;
  value: number;
  variant?: 'error' | 'warning' | 'secondary';
};

function SummaryItem({ label, value, variant }: SummaryItemProps) {
  return (
    <div className="space-y-1">
      <p className="text-xs text-muted-foreground">{label}</p>
      {variant ? (
        <Badge variant={variant} data-testid={`alert-summary-${variant}`}>
          <span className="tabular-nums">{value}</span>
        </Badge>
      ) : (
        <p className="text-2xl font-semibold tabular-nums" data-testid="alert-summary-total">
          {value}
        </p>
      )}
    </div>
  );
}
