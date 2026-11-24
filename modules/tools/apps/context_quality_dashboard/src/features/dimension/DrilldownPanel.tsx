import { Button } from '@cognite/aura/components/button';
import { Skeleton } from '@cognite/aura/components/skeleton';
import { useState } from 'react';

import { interpolate } from '../../i18n/interpolate';
import { ClassificationFilter } from '../shell/ClassificationFilter';
import { matchesClassification, type ClassificationFilterState } from '../shell/classificationFilterModel';
import { DownloadUnlinkedButton } from '../shell/DownloadUnlinkedButton';

import type { DrilldownPanelViewModel } from './drilldownPanelViewModel';
import { DrilldownSheetFrame } from './DrilldownSheetFrame';

type DrilldownPanelProps = {
  viewModel: DrilldownPanelViewModel;
};

export function DrilldownPanel({ viewModel }: DrilldownPanelProps) {
  if (viewModel.selectedMetricId === undefined) {
    return null;
  }

  return <DrilldownPanelSheet key={viewModel.selectedMetricId} viewModel={viewModel} />;
}

function DrilldownPanelSheet({ viewModel }: DrilldownPanelProps) {
  const t = viewModel.t;
  const drilldown = viewModel.isDrilldownLoading || viewModel.isDrilldownError ? undefined : viewModel.drilldown;

  return (
    <DrilldownSheetFrame
      t={t}
      title={drilldown === undefined ? t('dimension.drilldown') : t(drilldown.labelKey)}
      subtitle={drilldown === undefined ? undefined : t('dimension.drilldown')}
      onClose={viewModel.clearMetric}
    >
      <DrilldownSidesheetBody viewModel={viewModel} />
    </DrilldownSheetFrame>
  );
}

function DrilldownSidesheetBody({ viewModel }: DrilldownPanelProps) {
  const [selectedRowIds, setSelectedRowIds] = useState<string[]>([]);
  const [classificationFilter, setClassificationFilter] = useState<ClassificationFilterState>({});

  if (viewModel.isDrilldownLoading) {
    return <Skeleton className="mt-4 h-32 w-full flex-1" />;
  }

  if (viewModel.isDrilldownError || viewModel.drilldown === undefined) {
    return <p className="mt-4 text-sm text-destructive-foreground">{viewModel.t('dimension.error')}</p>;
  }

  const drilldown = viewModel.drilldown;

  const toggleRow = (rowId: string) => {
    setSelectedRowIds((current) =>
      current.includes(rowId) ? current.filter((id) => id !== rowId) : [...current, rowId]
    );
  };

  const ownerId = drilldown.ownerId ?? drilldown.rows[0]?.ownerId ?? null;
  const visibleRows = drilldown.rows.filter((row) => matchesClassification(row, classificationFilter));
  const visibleRowIds = visibleRows.map((row) => row.id);
  const allVisibleSelected =
    visibleRowIds.length > 0 && visibleRowIds.every((id) => selectedRowIds.includes(id));
  const someVisibleSelected =
    visibleRowIds.some((id) => selectedRowIds.includes(id)) && !allVisibleSelected;

  const toggleAllVisible = () => {
    setSelectedRowIds((current) => {
      if (allVisibleSelected) {
        const visible = new Set(visibleRowIds);
        return current.filter((id) => !visible.has(id));
      }
      return [...new Set([...current, ...visibleRowIds])];
    });
  };

  return (
    <div className="mt-4 flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto">
      <div className="flex flex-wrap gap-2">
        <DownloadUnlinkedButton />
        {viewModel.canRerun ? (
          <Button
            variant="outline"
            size="sm"
            data-testid="rerun-metric"
            disabled={viewModel.isRerunningMetric}
            onClick={viewModel.rerunMetric}
          >
            {viewModel.isRerunningMetric
              ? viewModel.t('runStatus.rerunning')
              : viewModel.t('runStatus.rerun')}
          </Button>
        ) : null}
      </div>

      {drilldown.partial === null ? null : (
        <p className="text-xs text-muted-foreground" data-testid="drilldown-partial">
          <span className="font-medium text-foreground">{viewModel.t('issues.partial.title')}</span>{' '}
          {interpolate(viewModel.t('dimension.partial.body'), { limit: String(drilldown.partial.limit) })}
        </p>
      )}

      <p className="text-sm">
        <span className="text-muted-foreground">{viewModel.t('dimension.suggestedAction')}: </span>
        {viewModel.t(drilldown.actionKey)}
      </p>

      <ClassificationFilter
        items={drilldown.rows}
        filter={classificationFilter}
        onChange={setClassificationFilter}
        t={viewModel.t}
      />

      {visibleRows.length === 0 ? (
        <p className="text-sm text-muted-foreground">{viewModel.t('dimension.noDrilldownRows')}</p>
      ) : (
        <div className="overflow-x-auto rounded-md border border-border">
          <table className="w-full text-sm" data-testid="drilldown-table">
            <thead className="bg-muted-background text-left text-xs text-muted-foreground">
              <tr>
                <th className="px-3 py-2">
                  <input
                    type="checkbox"
                    data-testid="drilldown-select-all"
                    checked={allVisibleSelected}
                    ref={(element) => {
                      if (element !== null) {
                        element.indeterminate = someVisibleSelected;
                      }
                    }}
                    onChange={toggleAllVisible}
                    aria-label={viewModel.t('dimension.selectAllVisible')}
                  />
                </th>
                {drilldown.columnKeys.map((columnKey) => (
                  <th key={columnKey} className="px-3 py-2 capitalize">
                    {viewModel.t(`dimension.column.${columnKey}`)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((row) => (
                <tr key={row.id} className="border-t border-border" data-testid={`drilldown-row-${row.id}`}>
                  <td className="px-3 py-2">
                    <input
                      type="checkbox"
                      checked={selectedRowIds.includes(row.id)}
                      onChange={() => toggleRow(row.id)}
                      aria-label={`${viewModel.t('dimension.select')} ${row.cells.entity ?? row.id}`}
                    />
                  </td>
                  {drilldown.columnKeys.map((columnKey) => (
                    <td key={columnKey} className="px-3 py-2">
                      {columnKey === 'tier' && row.tier === null
                        ? viewModel.t('tier.unassigned')
                        : (row.cells[columnKey] ?? '—')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {viewModel.canNotifyOwner ? (
        <div className="flex flex-wrap items-center gap-2 pb-2">
          <Button
            size="sm"
            disabled={selectedRowIds.length === 0 || ownerId === null || viewModel.isNotifying}
            onClick={() => {
              if (ownerId !== null) {
                viewModel.notifyOwner(ownerId, selectedRowIds);
              }
            }}
          >
            {viewModel.isNotifying ? viewModel.t('dimension.notifying') : viewModel.t('dimension.notifyOwner')}
          </Button>
          {viewModel.notificationSent ? (
            <span className="text-sm text-success-foreground">{viewModel.t('dimension.notificationSent')}</span>
          ) : null}
          {viewModel.notifyError ? (
            <span className="text-sm text-destructive-foreground">{viewModel.t('dimension.notifyError')}</span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
