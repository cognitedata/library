import { Badge } from '@cognite/aura/components/badge';
import { Button } from '@cognite/aura/components/button';

import type { AlertDto } from '../../types';

import {
  alertStatusLabelKey,
  alertTypeLabelKey,
  severityBadgeVariant,
  severityLabelKey,
} from './alertLabels';
import type { AlertsViewModel } from './useAlertsViewModel';

type AlertTableProps = {
  viewModel: AlertsViewModel;
};

export function AlertTable({ viewModel }: AlertTableProps) {
  return (
    <section aria-label={viewModel.t('alerts.table')} className="overflow-x-auto rounded-md border border-border">
      <table className="w-full text-sm" data-testid="alert-table">
        <thead className="bg-muted-background text-left text-xs text-muted-foreground">
          <tr>
            <th className="px-3 py-2">{viewModel.t('alerts.severity')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.alert')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.dimension')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.metric')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.affected')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.owner')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.scope')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.status')}</th>
            <th className="px-3 py-2">{viewModel.t('alerts.action')}</th>
          </tr>
        </thead>
        <tbody>
          {viewModel.alerts.map((alert) => (
            <AlertRow key={alert.id} alert={alert} viewModel={viewModel} />
          ))}
        </tbody>
      </table>
    </section>
  );
}

type AlertRowProps = {
  alert: AlertDto;
  viewModel: AlertsViewModel;
};

function AlertRow({ alert, viewModel }: AlertRowProps) {
  const canNavigate = alert.dimensionId !== undefined;
  const facilityLabel =
    alert.facilityId === null
      ? viewModel.t('filter.all')
      : viewModel.facilityLabels[alert.facilityId] ?? alert.facilityId;
  const ownerLabel =
    alert.ownerId === null
      ? '—'
      : viewModel.ownerLabels[alert.ownerId] ?? alert.ownerId;

  return (
    <tr className="border-t border-border align-top" data-testid={`alert-row-${alert.id}`}>
      <td className="px-3 py-2">
        <Badge variant={severityBadgeVariant(alert.severity)}>
          {viewModel.t(severityLabelKey(alert.severity))}
        </Badge>
      </td>
      <td className="px-3 py-2">
        <p className="font-medium">{viewModel.t(alert.titleKey)}</p>
        <p className="mt-1 text-xs text-muted-foreground">{viewModel.t(alert.bodyKey)}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          {viewModel.t('alerts.type')}: {viewModel.t(alertTypeLabelKey(alert.type))}
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          {viewModel.t('alerts.created')}: {new Date(alert.createdAt).toLocaleString()}
        </p>
      </td>
      <td className="px-3 py-2">{viewModel.t(`dimension.${alert.dimensionId}`)}</td>
      <td className="px-3 py-2">
        {alert.metricId ? viewModel.t(`metric.${alert.metricId}`) : '—'}
      </td>
      <td className="px-3 py-2 tabular-nums">
        {alert.affectedCount === null ? '—' : alert.affectedCount}
      </td>
      <td className="px-3 py-2">{ownerLabel}</td>
      <td className="px-3 py-2">{facilityLabel}</td>
      <td className="px-3 py-2">{viewModel.t(alertStatusLabelKey(alert.status))}</td>
      <td className="px-3 py-2">
        <p className="mb-2 text-xs text-muted-foreground">{viewModel.t(alert.actionKey)}</p>
        {canNavigate ? (
          <Button
            variant="outline"
            size="sm"
            data-testid={`alert-investigate-${alert.id}`}
            onClick={() => viewModel.navigateToDimension(alert.dimensionId, alert.metricId ?? undefined)}
          >
            {alert.metricId ? viewModel.t('alerts.investigate') : viewModel.t('alerts.openDimension')}
          </Button>
        ) : null}
      </td>
    </tr>
  );
}
