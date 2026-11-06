import { Alert, AlertActions, AlertDescription, AlertTitle } from '@cognite/aura/components/alert';
import { Skeleton } from '@cognite/aura/components/skeleton';
import { useMemo, useState } from 'react';

import { interpolate } from '../../i18n/interpolate';
import { DrilldownPanel } from '../dimension/DrilldownPanel';
import { DownloadUnlinkedButton } from '../shell/DownloadUnlinkedButton';
import { sectionHeadingClassName, sectionHeadingDestructiveClassName } from '../shell/sectionHeading';

import { IssuePatternFilters, type IssuePatternFiltersState } from './IssuePatternFilters';
import {
  buildIssuePatternRows,
  buildIssueTypeFilterOptions,
  mergeIssuePatternFilters,
} from './issuePatternPresentation';
import { IssuePatternTable } from './IssuePatternTable';
import { useIssueDrilldownViewModel } from './useIssueDrilldownViewModel';
import { useIssuesViewModel } from './useIssuesViewModel';

const EMPTY_FILTERS: IssuePatternFiltersState = { search: '' };

export function IssuesPage() {
  const viewModel = useIssuesViewModel();
  const drilldownViewModel = useIssueDrilldownViewModel();
  const [filters, setFilters] = useState<IssuePatternFiltersState>(EMPTY_FILTERS);

  const allRows = useMemo(() => buildIssuePatternRows(viewModel.issues), [viewModel.issues]);

  const dimensionOptions = useMemo(
    () =>
      uniqueBy(allRows, (row) => row.dimensionId).map((row) => ({
        id: row.dimensionId,
        label: viewModel.t(`dimension.${row.dimensionId}`),
      })),
    [allRows, viewModel]
  );

  const issueTypeOptions = useMemo(
    () => buildIssueTypeFilterOptions(allRows, filters.dimensionId, viewModel.t),
    [allRows, filters.dimensionId, viewModel]
  );

  const ownerOptions = useMemo(
    () =>
      Object.entries(viewModel.ownerLabels).map(([id, label]) => ({ id, label })),
    [viewModel.ownerLabels]
  );

  const filteredRows = useMemo(() => {
    const search = filters.search.trim().toLowerCase();
    return allRows.filter((row) => {
      if (filters.dimensionId !== undefined && row.dimensionId !== filters.dimensionId) {
        return false;
      }
      if (filters.kind !== undefined && row.kind !== filters.kind) {
        return false;
      }
      if (filters.ownerId !== undefined && row.ownerId !== filters.ownerId) {
        return false;
      }
      if (search.length > 0) {
        const name = viewModel.t(row.nameKey).toLowerCase();
        if (!name.includes(search) && !viewModel.t(row.descriptionKey).toLowerCase().includes(search)) {
          return false;
        }
      }
      return true;
    });
  }, [allRows, filters, viewModel]);

  if (viewModel.isLoading) {
    return (
      <section aria-label={viewModel.t('issues.loading')} className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-40 w-full" />
      </section>
    );
  }

  if (viewModel.isError) {
    return (
      <section className="rounded-md border border-destructive-background bg-destructive-muted-background p-4">
        <h2 className={sectionHeadingDestructiveClassName}>{viewModel.t('issues.error')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('issues.errorHint')}</p>
      </section>
    );
  }

  if (viewModel.isEmpty) {
    return (
      <section className="rounded-md border border-border p-6 text-center" data-testid="issues-empty">
        <h2 className={sectionHeadingClassName}>{viewModel.t('issues.noIssues')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('issues.noIssuesForScope')}</p>
      </section>
    );
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-lg font-semibold">{viewModel.t('issues.title')}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{viewModel.t('issues.description')}</p>
      </header>
      {viewModel.partial === null || viewModel.partial === undefined ? null : (
        <Alert variant="info" alertStyle="card" role="status" className="w-full" data-testid="issues-partial">
          <AlertTitle>
            <span className="text-base font-semibold">{viewModel.t('issues.partial.title')}</span>
          </AlertTitle>
          <AlertDescription>
            <p>
              {interpolate(viewModel.t('issues.partial.body'), {
                limit: String(viewModel.partial.limitPerMetric),
                count: String(viewModel.partial.metricCount),
              })}
            </p>
          </AlertDescription>
          <AlertActions>
            <DownloadUnlinkedButton />
          </AlertActions>
        </Alert>
      )}
      <IssuePatternFilters
        filters={filters}
        dimensionOptions={dimensionOptions}
        issueTypeOptions={issueTypeOptions}
        ownerOptions={ownerOptions}
        onChange={(next) => setFilters((previous) => mergeIssuePatternFilters(previous, next, allRows))}
        t={viewModel.t}
      />
      <IssuePatternTable
        rows={filteredRows}
        cutMetricIds={viewModel.partial?.cutMetricIds}
        ownerLabels={viewModel.ownerLabels}
        onSelect={viewModel.openIssueDrilldown}
        t={viewModel.t}
      />
      <DrilldownPanel viewModel={drilldownViewModel} />
      {viewModel.isLoadingMore ? (
        <p role="status" className="text-xs text-muted-foreground" data-testid="issues-loading-more">
          {viewModel.t('issues.loadingMore')}
        </p>
      ) : null}
      {viewModel.failedCount > 0 ? (
        <p role="status" className="text-xs text-destructive-foreground" data-testid="issues-some-failed">
          {interpolate(viewModel.t('issues.someFailed'), { count: String(viewModel.failedCount) })}
        </p>
      ) : null}
    </div>
  );
}

function uniqueBy<T, K>(items: readonly T[], key: (item: T) => K): T[] {
  const seen = new Set<K>();
  const result: T[] = [];
  for (const item of items) {
    const itemKey = key(item);
    if (!seen.has(itemKey)) {
      seen.add(itemKey);
      result.push(item);
    }
  }
  return result;
}
