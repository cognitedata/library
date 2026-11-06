import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { createMockServices } from '../../services/mock/createMockServices';
import { MockRunService } from '../../services/mock/MockRunService';
import { renderOverview } from '../../test/renderOverview';
import { createTestQueryClient } from '../../test/renderWithQueryClient';

import { invalidateDashboardQueries } from './invalidateDashboardQueries';

describe('RunStatusIndicator', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('renders run status', async () => {
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />);

    await waitFor(() => {
      expect(screen.getByTestId('run-status-indicator')).toBeInTheDocument();
    });
  });

  it('renders last successful run from the DTO', async () => {
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />);

    await waitFor(() => {
      expect(screen.getByTestId('run-status-text')).toHaveTextContent('Last successful run:');
    });
  });

  it('keeps the last successful run visible while refresh is running', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 50 });
    let resolveRefresh: (() => void) | undefined;
    vi.spyOn(services.qualityDashboardService, 'refresh').mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveRefresh = () =>
            resolve({
              lastSuccessAt: '2026-09-01T12:00:00.000Z',
              inProgress: false,
              scope: { kind: 'all' },
            });
        })
    );

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />, {
      services,
    });

    await waitFor(() => {
      expect(screen.getByTestId('run-status-refresh')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('run-status-refresh'));

    await waitFor(() => {
      expect(screen.getByTestId('run-status-text')).toHaveTextContent('Last successful run:');
      expect(screen.getByTestId('run-status-refresh')).toHaveTextContent('Refreshing');
    });

    resolveRefresh?.();
  });

  it('renders English run-status strings', async () => {
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Refresh' })).toBeInTheDocument();
    });
  });

  it('renders Japanese run-status strings', async () => {
    localStorage.setItem('context-quality-dashboard.language', 'ja');

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: '更新' })).toBeInTheDocument();
    });
  });
});

describe('refresh behavior', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('only reads the latest runs again when Refresh is pressed on a dimension page', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const refresh = vi.spyOn(services.qualityDashboardService, 'refresh');
    const rerun = vi.spyOn(services.qualityDashboardService, 'rerun');
    const start = vi.spyOn(services.runService ?? new MockRunService(), 'start');

    renderOverview(
      <ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'time-series' })} />,
      { services }
    );
    await waitFor(() => {
      expect(screen.getByTestId('run-status-refresh')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('run-status-refresh'));

    await waitFor(() => {
      expect(refresh).toHaveBeenCalled();
    });
    expect(rerun).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });

  it('offers a confirmed rerun of the dimension on its page, and nowhere else', async () => {
    const user = userEvent.setup();
    const runService = new MockRunService();
    const start = vi.spyOn(runService, 'start');
    const services = { ...createMockServices({ latency: 0 }), runService };

    const view = renderOverview(
      <ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'asset-hierarchy' })} />,
      { services }
    );

    const button = await screen.findByRole('button', { name: 'Rerun Asset Hierarchy' });
    await user.click(button);
    expect(start).not.toHaveBeenCalled(); // a click by mistake starts nothing
    const dialog = await screen.findByTestId('confirm-run-dialog');
    expect(dialog).toHaveTextContent('Rerun Asset Hierarchy?');
    expect(dialog).toHaveTextContent(
      'This will run the Asset Hierarchy quality check again using the latest data'
    );
    expect(dialog).toHaveTextContent('All other results and previous run history will be preserved');

    await user.click(screen.getByTestId('confirm-run-start-dimension-asset-hierarchy'));

    await waitFor(() => {
      expect(start).toHaveBeenCalledWith({
        extraLinks: [],
        scope: { dimensions: ['asset-hierarchy'], configured: false },
      });
    });
    view.unmount();
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />, { services });
    await screen.findByTestId('run-status-refresh');
    expect(screen.queryByRole('button', { name: /^Rerun/ })).not.toBeInTheDocument();
  });

  it('does not show a stale builtin dimension rerun on configured metric views', async () => {
    const configured = {
      models: [
        {
          id: 'a:Model:v1',
          label: 'Model',
          groups: [
            {
              id: 'a:Model:v1/Widget',
              modelId: 'a:Model:v1',
              modelLabel: 'Model',
              viewLabel: 'Widget',
              metrics: [
                {
                  label: 'owner',
                  modelId: 'a:Model:v1',
                  view: 'Widget',
                  totalCount: 10,
                  linkedCount: 9,
                  coveragePercent: 90,
                },
              ],
            },
          ],
        },
      ],
    };
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getConfiguredMetrics').mockResolvedValue(configured as never);

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'files',
          dimensionsScope: 'configured',
          configuredGroupId: 'a:Model:v1/Widget',
          configuredModelId: 'a:Model:v1',
        })}
      />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Widget' })).toBeInTheDocument();
    });
    expect(screen.queryByRole('button', { name: 'Rerun Files' })).not.toBeInTheDocument();
    expect(screen.queryByTestId('run-button-dimension-files')).not.toBeInTheDocument();
    expect(await screen.findByRole('button', { name: 'Rerun Widget' })).toBeInTheDocument();
  });

  it('says a run is already in progress instead of starting a second one', async () => {
    const user = userEvent.setup();
    const runService = new MockRunService();
    await runService.start({ extraLinks: [], scope: { dimensions: 'all', configured: false } }); // started elsewhere
    const start = vi.spyOn(runService, 'start');
    const services = { ...createMockServices({ latency: 0 }), runService };

    renderOverview(
      <ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'files' })} />,
      { services }
    );

    await waitFor(() => expect(screen.getByTestId('run-status-busy')).toBeInTheDocument());

    const rerun = await screen.findByRole('button', { name: 'Rerun Files' });
    expect(rerun).toBeDisabled();
    await user.click(rerun);

    expect(screen.queryByTestId('confirm-run-start-dimension-files')).not.toBeInTheDocument();
    expect(start).not.toHaveBeenCalled();
  });

  it('says when the dimension on the page was last measured, which can be earlier than the latest run', async () => {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getRunStatus').mockResolvedValue({
      lastSuccessAt: '2026-11-02T06:00:00.000Z',
      inProgress: false,
      scope: { kind: 'all' },
      dimensionRuns: { 'asset-hierarchy': '2026-10-30T06:00:00.000Z', files: '2026-11-02T06:00:00.000Z' },
    });

    renderOverview(
      <ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'asset-hierarchy' })} />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByTestId('run-status-text')).toHaveTextContent('30/10/2026');
    });
  });

  it('invokes service.refresh from the overview page', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const refresh = vi.spyOn(services.qualityDashboardService, 'refresh');

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />, {
      services,
    });

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Refresh' })).toBeInTheDocument();
    });

    await user.click(screen.getByRole('button', { name: 'Refresh' }));

    await waitFor(() => {
      expect(refresh).toHaveBeenCalled();
    });
  });

  it('keeps the last successful run visible after refresh completes', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />, {
      services,
    });

    await waitFor(() => {
      expect(screen.getByTestId('run-status-refresh')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('run-status-refresh'));

    await waitFor(() => {
      expect(screen.getByTestId('run-status-text')).toHaveTextContent('Last successful run:');
      expect(screen.getByTestId('run-status-refresh')).toHaveTextContent('Refresh');
    });
  });

  it('keeps overview scores unchanged after refresh', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const getOverview = vi.spyOn(services.qualityDashboardService, 'getOverview');

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />, {
      services,
    });

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });

    const beforeResult = getOverview.mock.results[0];
    const before = beforeResult ? await beforeResult.value : undefined;

    await user.click(screen.getByTestId('run-status-refresh'));

    await waitFor(() => {
      expect(getOverview.mock.calls.length).toBeGreaterThan(1);
    });

    const afterResult = getOverview.mock.results[getOverview.mock.results.length - 1];
    const after = afterResult ? await afterResult.value : undefined;
    expect(after?.overallAggregatePercent).toBe(before?.overallAggregatePercent);
  });
});

describe('invalidateDashboardQueries', () => {
  it('invalidates overview, dimension, drilldown, alerts, snapshots, and runStatus', async () => {
    const queryClient = createTestQueryClient();
    const invalidateQueries = vi.spyOn(queryClient, 'invalidateQueries');

    await invalidateDashboardQueries(queryClient);

    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['overview'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['dimension'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['drilldown'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['alerts'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['snapshots'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['runStatus'] });
  });
});

describe('rerun behavior', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('invokes rerun with dimensionId from the dimension page', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const rerun = vi.spyOn(services.qualityDashboardService, 'rerun');

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
        })}
      />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByTestId('rerun-dimension')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('rerun-dimension'));

    await waitFor(() => {
      expect(rerun).toHaveBeenCalledWith({ dimensionId: 'asset-hierarchy' });
    });
  });

  it('invokes rerun with metricId from drilldown context', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const rerun = vi.spyOn(services.qualityDashboardService, 'rerun');

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
          metricId: 'orphanRate',
        })}
      />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByTestId('rerun-metric')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('rerun-metric'));

    await waitFor(() => {
      expect(rerun).toHaveBeenCalledWith({ metricId: 'orphanRate' });
    });
  });

  it('shows rerun in-progress state', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 50 });
    let resolveRerun: (() => void) | undefined;
    vi.spyOn(services.qualityDashboardService, 'rerun').mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveRerun = () =>
            resolve({
              lastSuccessAt: '2026-09-01T12:00:00.000Z',
              inProgress: false,
              scope: { kind: 'dimension', dimensionId: 'asset-hierarchy' },
            });
        })
    );

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
        })}
      />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByTestId('rerun-dimension')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('rerun-dimension'));

    await waitFor(() => {
      expect(screen.getByTestId('rerun-dimension')).toHaveTextContent('Rerunning');
    });

    resolveRerun?.();
  });

  it('returns the rerun button to idle after completion', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
        })}
      />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByTestId('rerun-dimension')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('rerun-dimension'));

    await waitFor(() => {
      expect(screen.getByTestId('run-status-text')).toHaveTextContent('Last successful run:');
      expect(screen.getByTestId('rerun-dimension')).toHaveTextContent('Rerun');
    });
  });

  it('keeps dimension scores deterministic after rerun', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const getDimension = vi.spyOn(services.qualityDashboardService, 'getDimension');

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
        })}
      />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Asset Hierarchy' })).toBeInTheDocument();
    });

    const beforeResult = getDimension.mock.results[0];
    const before = beforeResult ? await beforeResult.value : undefined;

    await user.click(screen.getByTestId('run-status-refresh'));

    await waitFor(() => {
      expect(getDimension.mock.calls.length).toBeGreaterThan(1);
    });

    const afterResult = getDimension.mock.results[getDimension.mock.results.length - 1];
    const after = afterResult ? await afterResult.value : undefined;
    expect(after?.overallPercent).toBe(before?.overallPercent);
  });
});
