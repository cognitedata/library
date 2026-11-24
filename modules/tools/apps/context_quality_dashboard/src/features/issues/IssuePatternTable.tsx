import type { DimensionId, MetricId } from '../../types';

import type { IssuePatternRow } from './issuePatternPresentation';

type IssuePatternTableProps = {
  rows: IssuePatternRow[];
  /** Metrics whose list was cut at the limit: their count is at least what is shown. */
  cutMetricIds?: readonly MetricId[];
  ownerLabels: Record<string, string>;
  onSelect: (dimensionId: DimensionId, metricId: MetricId) => void;
  t: (key: string) => string;
};

export function IssuePatternTable({ rows, cutMetricIds = [], ownerLabels, onSelect, t }: IssuePatternTableProps) {
  return (
    <section
      aria-label={t('issues.table')}
      className="overflow-x-auto rounded-md border border-border"
    >
      <table className="w-full text-sm" data-testid="issue-pattern-table">
        <thead className="bg-muted-background text-left text-xs text-muted-foreground">
          <tr>
            <th className="px-3 py-2">{t('issues.table.pattern')}</th>
            <th className="px-3 py-2">{t('issues.table.affectedItems')}</th>
            <th className="px-3 py-2">{t('issues.dimension')}</th>
            <th className="px-3 py-2">{t('issues.owner')}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={row.kind}
              className="cursor-pointer border-t border-border align-top hover:bg-muted-background"
              data-testid={`issue-pattern-row-${row.kind}`}
              onClick={() => onSelect(row.dimensionId, row.metricId)}
            >
              <td className="px-3 py-2">
                <p className="font-medium">{t(row.nameKey)}</p>
                <p className="mt-1 text-xs text-muted-foreground">{t(row.descriptionKey)}</p>
              </td>
              <td className="px-3 py-2 tabular-nums">
                {cutMetricIds.includes(row.metricId) ? (
                  <span title={t('issues.table.atLeast')} data-testid={`issue-pattern-at-least-${row.kind}`}>
                    {row.affectedCount}+
                  </span>
                ) : (
                  row.affectedCount
                )}
              </td>
              <td className="px-3 py-2">{t(`dimension.${row.dimensionId}`)}</td>
              <td className="px-3 py-2">{ownerLabels[row.ownerId] ?? row.ownerId}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
