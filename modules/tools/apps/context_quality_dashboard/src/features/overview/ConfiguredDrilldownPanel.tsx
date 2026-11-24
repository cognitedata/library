import { Skeleton } from '@cognite/aura/components/skeleton';

import { interpolate } from '../../i18n/interpolate';
import type { ConfiguredDrilldownDto, ConfiguredMetricKey, SliceFilters } from '../../types';
import { DrilldownSheetFrame } from '../dimension/DrilldownSheetFrame';
import { DownloadUnlinkedButton } from '../shell/DownloadUnlinkedButton';

import { useConfiguredDrilldownViewModel } from './useConfiguredDrilldownViewModel';

type ConfiguredDrilldownPanelProps = {
  metricKey: ConfiguredMetricKey;
  viewLabel: string;
  filters: SliceFilters;
  onClose: () => void;
  t: (key: string) => string;
};

/** The instances behind a configured metric's "not linked" count, in the same side sheet the other drill-downs use. */
export function ConfiguredDrilldownPanel({ metricKey, viewLabel, filters, onClose, t }: ConfiguredDrilldownPanelProps) {
  const query = useConfiguredDrilldownViewModel(metricKey, filters);
  const title = interpolate(t('overview.configured.metricTitle'), { view: viewLabel, property: metricKey.property });
  const drilldown = query.data;

  return (
    <DrilldownSheetFrame
      t={t}
      title={title}
      subtitle={drilldown === undefined ? undefined : t('dimension.drilldown')}
      onClose={onClose}
    >
      {query.isLoading ? (
        <Skeleton className="mt-4 h-32 w-full flex-1" />
      ) : drilldown === undefined ? (
        <p className="mt-4 text-sm text-destructive-foreground">{t('dimension.error')}</p>
      ) : (
        <DrilldownBody drilldown={drilldown} viewLabel={viewLabel} t={t} />
      )}
    </DrilldownSheetFrame>
  );
}

type DrilldownBodyProps = {
  drilldown: ConfiguredDrilldownDto;
  viewLabel: string;
  t: (key: string) => string;
};

function DrilldownBody({ drilldown, viewLabel, t }: DrilldownBodyProps) {
  return (
    <div className="mt-4 flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto" data-testid="configured-drilldown">
      <div className="flex flex-wrap gap-2">
        <DownloadUnlinkedButton />
      </div>

      <p className="text-sm text-muted-foreground">
        {interpolate(t('overview.configured.drilldown.hint'), { view: viewLabel, property: drilldown.key.property })}
      </p>

      {drilldown.cutAt === null ? null : (
        <p className="text-xs text-muted-foreground" data-testid="configured-drilldown-cut">
          {interpolate(t('overview.configured.drilldown.cut'), {
            limit: String(drilldown.cutAt),
            dropped: String(drilldown.dropped),
          })}
        </p>
      )}

      {drilldown.rows.length === 0 ? (
        <p className="text-sm text-muted-foreground">{t('dimension.noDrilldownRows')}</p>
      ) : (
        <div className="overflow-x-auto rounded-md border border-border">
          <table className="w-full text-sm" data-testid="configured-drilldown-table">
            <thead className="bg-muted-background text-left text-xs text-muted-foreground">
              <tr>
                <th className="px-3 py-2">{t('dimension.column.entity')}</th>
                <th className="px-3 py-2">{t('overview.configured.drilldown.space')}</th>
                <th className="px-3 py-2">{t('dimension.column.detail')}</th>
              </tr>
            </thead>
            <tbody>
              {drilldown.rows.map((row) => (
                <tr key={row.id} className="border-t border-border" data-testid={`configured-drilldown-row-${row.id}`}>
                  <td className="px-3 py-2">
                    {row.entity}
                    {row.entity === row.entityId ? null : (
                      <span className="block text-xs text-muted-foreground">{row.entityId}</span>
                    )}
                  </td>
                  <td className="px-3 py-2">{row.space}</td>
                  <td className="px-3 py-2">{row.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
