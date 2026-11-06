import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';

import type { IssuePatternRow } from './issuePatternPresentation';
import { IssuePatternTable } from './IssuePatternTable';

const row = (kind: IssuePatternRow['kind'], metricId: IssuePatternRow['metricId'], affectedCount: number): IssuePatternRow => ({
  kind,
  dimensionId: 'time-series',
  metricId,
  nameKey: `issue.kind.${kind}`,
  descriptionKey: `issue.kind.${kind}.description`,
  affectedCount,
  ownerId: '',
  issueIds: [],
});

describe(IssuePatternTable.name, () => {
  it('shows a lower bound with a plus for a metric whose list was cut, and the plain count for the others', () => {
    render(
      <IssuePatternTable
        rows={[row('stale-timeseries', 'staleTimeSeries', 200), row('orphan-timeseries', 'orphansByPrefix', 7)]}
        cutMetricIds={['staleTimeSeries']}
        ownerLabels={{}}
        onSelect={() => undefined}
        t={(key) => translate(key, 'en')}
      />
    );

    expect(screen.getByTestId('issue-pattern-at-least-stale-timeseries')).toHaveTextContent('200+');
    expect(screen.queryByTestId('issue-pattern-at-least-orphan-timeseries')).not.toBeInTheDocument();
    expect(screen.getByTestId('issue-pattern-row-orphan-timeseries')).toHaveTextContent('7');
  });
});
