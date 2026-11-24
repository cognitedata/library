import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { translate } from '../../i18n/messages';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';
import type { IssuesDto } from '../../types';


import { buildIssuePatternRows, buildIssueTypeFilterOptions } from './issuePatternPresentation';
import { IssuesPage } from './IssuesPage';

function renderIssues(
  hostState: Record<string, unknown> = { page: 'issues' },
  services = createMockServices({ latency: 0 })
) {
  return renderOverview(<ProductShell hostInitialState={JSON.stringify(hostState)} />, { services });
}

describe(IssuesPage.name, () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('renders the issues page', async () => {
    renderIssues();

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Issues' })).toBeInTheDocument();
    });
  });

  it('does not render the removed criticality summary', async () => {
    renderIssues();

    await waitFor(() => {
      expect(screen.getByTestId('issue-pattern-table')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('issue-summary')).not.toBeInTheDocument();
    expect(screen.queryByText('Total issues')).not.toBeInTheDocument();
  });

  it('uses current filters in the issues query', async () => {
    const services = createMockServices({ latency: 0 });
    const getIssuesOfMetric = vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric');

    renderIssues({ page: 'issues', filters: { facilityId: 'demo-north' } }, services);

    await waitFor(() => {
      expect(getIssuesOfMetric).toHaveBeenCalledWith(expect.any(String), { facilityId: 'demo-north' });
    });
  });

  it('groups issues into pattern rows sorted by affected items, highest first', async () => {
    const services = createMockServices({ latency: 0 });
    const result = (await services.qualityDashboardService.getIssues({})) as IssuesDto;

    renderIssues({ page: 'issues' }, services);

    await waitFor(() => {
      expect(screen.getByTestId('issue-pattern-table')).toBeInTheDocument();
    });

    const table = screen.getByTestId('issue-pattern-table');
    const patternKeys = new Set(
      result.issues.map((issue) => `${issue.dimensionId}:${issue.kind}`)
    );
    const renderedRows = within(table).getAllByRole('row').slice(1);
    expect(renderedRows.length).toBe(patternKeys.size);

    const counts = renderedRows.map((row) => {
      const cells = within(row).getAllByRole('cell');
      return Number(cells[1].textContent);
    });
    const sortedDescending = [...counts].sort((a, b) => b - a);
    expect(counts).toEqual(sortedDescending);
  });

  it('does not render a per-row investigate button', async () => {
    renderIssues();

    await waitFor(() => {
      expect(screen.getByTestId('issue-pattern-table')).toBeInTheDocument();
    });

    expect(screen.queryByText('Investigate')).not.toBeInTheDocument();
  });

  it('opens the drilldown side panel on Issues when a pattern row is clicked', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const result = (await services.qualityDashboardService.getIssues({})) as IssuesDto;
    const firstIssue = result.issues[0];

    renderIssues({ page: 'issues' }, services);

    await waitFor(() => {
      expect(firstIssue).toBeDefined();
    });
    if (firstIssue === undefined) {
      return;
    }

    await waitFor(() => {
      expect(screen.getByTestId(`issue-pattern-row-${firstIssue.kind}`)).toBeInTheDocument();
    });

    expect(screen.queryByTestId('drilldown-sidesheet')).not.toBeInTheDocument();

    await user.click(screen.getByTestId(`issue-pattern-row-${firstIssue.kind}`));

    await waitFor(() => {
      expect(screen.getByRole('tab', { name: 'Issues', selected: true })).toBeInTheDocument();
      expect(screen.getByTestId('drilldown-sidesheet')).toBeInTheDocument();
    });
  });

  it('scopes issue type options to the selected dimension', async () => {
    const services = createMockServices({ latency: 0 });
    const result = (await services.qualityDashboardService.getIssues({})) as IssuesDto;
    const rows = buildIssuePatternRows(result.issues);
    const t = (key: string) => translate(key, 'en');

    const allKinds = buildIssueTypeFilterOptions(rows, undefined, t).map((option) => option.id);
    const timeSeriesKinds = buildIssueTypeFilterOptions(rows, 'time-series', t).map((option) => option.id);

    expect(timeSeriesKinds.length).toBeGreaterThan(0);
    expect(timeSeriesKinds.length).toBeLessThan(allKinds.length);
    expect(
      timeSeriesKinds.every((kind) =>
        rows.some((row) => row.dimensionId === 'time-series' && row.kind === kind)
      )
    ).toBe(true);
  });

  it('filters pattern rows by search text', async () => {
    const user = userEvent.setup();
    renderIssues();

    await waitFor(() => {
      expect(screen.getByTestId('issue-pattern-table')).toBeInTheDocument();
    });

    const rowCountBefore = within(screen.getByTestId('issue-pattern-table')).getAllByRole('row').length;

    await user.type(screen.getByPlaceholderText('Search issue patterns...'), 'zzzzz-no-match');

    await waitFor(() => {
      const rowCountAfter = within(screen.getByTestId('issue-pattern-table')).getAllByRole('row').length;
      expect(rowCountAfter).toBeLessThan(rowCountBefore);
    });
  });

  it('renders loading state', () => {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric').mockImplementation(
      () => new Promise(() => undefined)
    );

    renderOverview(<IssuesPage />, { services, hostInitialState: JSON.stringify({ page: 'issues' }) });

    expect(screen.getByLabelText('Loading issues')).toBeInTheDocument();
  });

  it('renders error state', async () => {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric').mockRejectedValue(new Error('issues failed'));

    renderOverview(<IssuesPage />, { services, hostInitialState: JSON.stringify({ page: 'issues' }) });

    await waitFor(() => {
      expect(screen.getByText('Failed to load issues')).toBeInTheDocument();
    });
  });

  it('says the list is not complete, with the workbook download, only when it is not', async () => {
    const services = createMockServices({ latency: 0 });
    const real = services.qualityDashboardService.getIssuesOfMetric.bind(services.qualityDashboardService);
    const spy = vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric');

    spy.mockImplementation(async (metricId, filters) => ({
      ...(await real(metricId, filters)),
      cut: metricId === 'orphansByTier',
      limit: 200,
    }));
    const view = renderIssues({ page: 'issues' }, services);
    const notice = await screen.findByTestId('issues-partial');
    expect(notice).toHaveTextContent('This is not the full list.');
    expect(notice).toHaveTextContent(/up to 200 items for each of \d+ metrics/);
    expect(within(notice).getByTestId('download-unlinked')).toBeInTheDocument();
    view.unmount();

    spy.mockImplementation(real);
    renderIssues({ page: 'issues' }, services);
    await screen.findByTestId('issue-pattern-table');
    expect(screen.queryByTestId('issues-partial')).not.toBeInTheDocument();
  });

  it('shows the issues that have arrived while the rest load, and what could not be loaded', async () => {
    const services = createMockServices({ latency: 0 });
    const real = services.qualityDashboardService.getIssuesOfMetric.bind(services.qualityDashboardService);
    vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric').mockImplementation((metricId, filters) => {
      if (metricId === 'orphansByTier') {
        return real(metricId, filters);
      }
      return metricId === 'staleTimeSeries' ? Promise.reject(new Error('x')) : new Promise(() => undefined);
    });

    renderIssues({ page: 'issues' }, services);

    expect(await screen.findByTestId('issue-pattern-table')).toBeInTheDocument();
    expect(screen.getByTestId('issues-loading-more')).toHaveTextContent('Loading more issues');
    expect(await screen.findByTestId('issues-some-failed')).toHaveTextContent('1 issue list(s) could not be loaded');
  });

  it('renders empty state without fake issues', async () => {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric').mockResolvedValue({
      issues: [],
      cut: false,
      limit: 200,
    });

    renderIssues({ page: 'issues', filters: { facilityId: 'missing-facility' } }, services);

    await waitFor(() => {
      expect(screen.getByTestId('issues-empty')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('issue-pattern-table')).not.toBeInTheDocument();
  });

  it('renders Japanese issue strings', async () => {
    localStorage.setItem('context-quality-dashboard.language', 'ja');

    renderIssues();

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: '問題' })).toBeInTheDocument();
    });

    expect(screen.getByPlaceholderText('問題パターンを検索...')).toBeInTheDocument();
  });
});
