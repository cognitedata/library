import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { CDF_CAPABILITIES } from '../../services/backendCapabilities';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';

import { OverviewPage } from './OverviewPage';

describe('OverviewPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('renders loading state', async () => {
    const services = createMockServices({ latency: 0 });
    const pendingOverview = services.qualityDashboardService.getOverview({});
    vi.spyOn(services.qualityDashboardService, 'getOverview').mockImplementation(
      () =>
        new Promise((resolve) => {
          void pendingOverview.then(resolve);
        })
    );

    renderOverview(<OverviewPage />, {
      services: {
        ...services,
        qualityDashboardService: {
          ...services.qualityDashboardService,
          getOverview: () => new Promise(() => undefined),
        },
      },
    });

    expect(screen.getByLabelText('Loading overview')).toBeInTheDocument();
  });

  it('says when a card was measured only when its run is older than the freshest card', async () => {
    const services = createMockServices({ latency: 0 });
    const base = await services.qualityDashboardService.getOverview({});
    vi.spyOn(services.qualityDashboardService, 'getOverview').mockResolvedValue({
      ...base,
      dimensionCards: base.dimensionCards.map((card) => ({
        ...card,
        measuredAt: card.dimensionId === 'asset-hierarchy' ? '2026-10-30T06:00:00.000Z' : '2026-11-02T06:00:00.000Z',
      })),
    });

    renderOverview(<OverviewPage />, { services });

    const older = await screen.findByTestId('dimension-card-asset-hierarchy');
    expect(within(older).getByTestId('dimension-card-measured')).toHaveTextContent('Measured 30/10/2026');
    const fresh = screen.getByTestId('dimension-card-time-series');
    expect(within(fresh).queryByTestId('dimension-card-measured')).not.toBeInTheDocument();
  });

  it('renders service data including contextualization health', async () => {
    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });

    expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    expect(screen.queryByText(/Needs attention/)).not.toBeInTheDocument();
  });

  it('renders dimension rows returned by the service', async () => {
    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-time-series')).toBeInTheDocument();
    });

    expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    expect(
      screen.getAllByTestId(
        /dimension-card-(asset-hierarchy|equipment-asset|time-series|maintenance|file-annotation|model-3d|files)$/
      )
    ).toHaveLength(7);
    expect(screen.getByRole('heading', { name: 'Dimension overview' })).toBeInTheDocument();
    expect(
      screen.getByText(
        'See how many items are contextualised within each dimension and how it has changed since the last run.'
      )
    ).toBeInTheDocument();
    expect(screen.getByTestId('dimension-overview-trend-window-30d')).toBeInTheDocument();
    expect(screen.queryAllByTestId(/dimension-trend-window-/)).toHaveLength(0);
    expect(within(screen.getByTestId('dimension-card-time-series')).queryByTestId('target-marker')).toBeNull();
  });

  it('navigates from a dimension overview open dimension action', async () => {
    const user = userEvent.setup();

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />);

    await waitFor(() => {
      expect(screen.getByLabelText('Dimension overview')).toBeInTheDocument();
    });

    await user.click(
      within(screen.getByLabelText('Dimension overview')).getAllByRole('button', { name: 'Open dimension' })[0]
    );

    await waitFor(() => {
      expect(screen.getByLabelText('Dimension')).toBeInTheDocument();
    });
  });

  it('handles optional disabled dimensions gracefully', async () => {
    const services = createMockServices({ latency: 0 });
    await services.configurationService.updateConfig({ model3dAvailable: false });

    renderOverview(<OverviewPage />, { services });

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-model-3d')).toBeInTheDocument();
    });

    expect(screen.getByTestId('dimension-card-model-3d')).toHaveTextContent('Disabled');
    expect(screen.getByTestId('dimension-card-model-3d')).not.toHaveTextContent('0%');

    await services.configurationService.updateConfig({ model3dAvailable: true });
  });

  it('does not render needs attention, classification, or progress-since-last-run sections', async () => {
    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });

    expect(screen.queryByLabelText('Needs attention')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Contextualisation by classification')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Progress since last run')).not.toBeInTheDocument();
  });

  it('does not render a standalone Priority Actions list', async () => {
    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });

    expect(screen.queryByLabelText('Priority Actions')).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Priority Actions' })).not.toBeInTheDocument();
  });

  it('does not render compact Alerts on Overview', async () => {
    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });

    expect(screen.queryByRole('heading', { name: 'Alerts' })).not.toBeInTheDocument();
    expect(screen.queryByText('View alerts')).not.toBeInTheDocument();
    expect(screen.queryByText('Tier 1 time series rate below target')).not.toBeInTheDocument();
  });

  it('does not render exclusions or custom quality rules on overview', async () => {
    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });

    expect(screen.queryByText('Exclusions')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Custom Quality Rules')).not.toBeInTheDocument();
  });

  it('responds to trend window changes', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const getSnapshots = vi.spyOn(services.qualityDashboardService, 'getSnapshots');

    renderOverview(
      <ProductShell hostInitialState={JSON.stringify({ page: 'overview', trendWindow: '7d' })} />,
      { services }
    );

    await waitFor(() => {
      expect(screen.getByText('Contextualization Quality')).toBeInTheDocument();
    });

    await user.click(screen.getByRole('combobox', { name: /Trend window/i }));
    await user.click(screen.getByRole('option', { name: '90d' }));

    await waitFor(() => {
      expect(getSnapshots).toHaveBeenCalledWith(
        expect.any(String),
        '90d',
        expect.any(Object)
      );
    });
  });

  it('passes active filters to the overview query', async () => {
    const services = createMockServices({ latency: 0 });
    const getOverview = vi.spyOn(services.qualityDashboardService, 'getOverview');

    renderOverview(
      <OverviewPage />,
      {
        services,
        hostInitialState: JSON.stringify({
          page: 'overview',
          filters: { facilityId: 'demo-north' },
        }),
      }
    );

    await waitFor(() => {
      expect(getOverview).toHaveBeenCalledWith({ facilityId: 'demo-north' });
    });
  });

  it('updates overview query when filters change', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const getOverview = vi.spyOn(services.qualityDashboardService, 'getOverview');

    renderOverview(<ProductShell />, { services });

    await waitFor(() => {
      expect(screen.getByText('Contextualization Quality')).toBeInTheDocument();
    });

    const filtersSection = screen.getByLabelText('Global filters');
    const facilityTrigger = within(filtersSection).getAllByRole('combobox')[0];
    await user.click(facilityTrigger);
    await user.click(screen.getByRole('option', { name: 'Demo North' }));

    await waitFor(() => {
      expect(getOverview).toHaveBeenCalledWith({ facilityId: 'demo-north' });
    });
  });

  it('updates the Asset hierarchy card when the facility filter changes', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });

    renderOverview(<ProductShell />, { services });

    const hero = await screen.findByTestId('dimension-card-asset-hierarchy');
    const before = hero.textContent;

    const filtersSection = screen.getByLabelText('Global filters');
    const facilityTrigger = within(filtersSection).getAllByRole('combobox')[0];
    await user.click(facilityTrigger);
    await user.click(screen.getByRole('option', { name: 'Demo North' }));

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy').textContent).not.toBe(before);
    });
  });

  it('renders error state without crashing', async () => {
    const services = createMockServices({ latency: 0 });

    renderOverview(<OverviewPage />, {
      services: {
        ...services,
        qualityDashboardService: {
          ...services.qualityDashboardService,
          getOverview: () => Promise.reject(new Error('overview failed')),
        },
      },
    });

    await waitFor(() => {
      expect(screen.getByText('Unable to load overview')).toBeInTheDocument();
    });
  });

  it('renders empty state without fake zero scores', async () => {
    const services = createMockServices({ latency: 0 });

    renderOverview(<OverviewPage />, {
      services,
      hostInitialState: JSON.stringify({
        page: 'overview',
        filters: { facilityId: 'missing-facility' },
      }),
    });

    await waitFor(() => {
      expect(screen.getByText('No data for this scope')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('dimension-card-asset-hierarchy')).not.toBeInTheDocument();
    expect(screen.queryByText('0%')).not.toBeInTheDocument();
  });

  it('renders English translations', async () => {
    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });
  });

  it('renders Japanese translations', async () => {
    localStorage.setItem('context-quality-dashboard.language', 'ja');

    renderOverview(<OverviewPage />);

    await waitFor(() => {
      expect(screen.getByRole('heading', { level: 2, name: 'ディメンション概要' })).toBeInTheDocument();
    });

    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

});


describe('OverviewPage configured metrics not configured', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  const LINKED = {
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
              { label: 'owner', modelId: 'a:Model:v1', view: 'Widget', totalCount: 10, linkedCount: 9, coveragePercent: 90 },
            ],
          },
        ],
      },
    ],
  };

  function cdfServicesWith(configured: unknown) {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getConfiguredMetrics').mockResolvedValue(configured as never);
    return { ...services, backend: { kind: 'cdf' as const, capabilities: CDF_CAPABILITIES } };
  }

  it('invites the user to Configuration while the latest run measured only the built-in checks', async () => {
    renderOverview(<OverviewPage />, { services: cdfServicesWith({ models: [] }) });

    const empty = await screen.findByTestId('configured-metrics-not-configured');
    expect(empty).toHaveTextContent('Measure your own data models');
    expect(empty).toHaveTextContent('Upload a configuration file on Configuration');
    expect(screen.getByRole('heading', { level: 2, name: 'Custom configured metrics' })).toBeInTheDocument();
  });

  it('says who has to start the run when this build cannot', async () => {
    const services = { ...cdfServicesWith({ models: [] }), runService: null };

    renderOverview(<OverviewPage />, { services });

    expect(await screen.findByTestId('configured-metrics-not-configured')).toHaveTextContent(
      'This dashboard cannot start a run.'
    );
  });

  it('is not shown on the demo data', async () => {
    renderOverview(<OverviewPage />, { services: createMockServices({ latency: 0 }) });

    await screen.findByTestId('dimension-card-asset-hierarchy');
    expect(screen.queryByTestId('configured-metrics-not-configured')).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { level: 2, name: 'Custom configured metrics' })).not.toBeInTheDocument();
  });

  it('is not shown once the latest run has configured metrics', async () => {
    renderOverview(<OverviewPage />, { services: cdfServicesWith(LINKED) });

    await screen.findByTestId('dimension-card-asset-hierarchy');
    expect(screen.queryByTestId('configured-metrics-not-configured')).not.toBeInTheDocument();
    expect(screen.getByTestId('configured-metrics-layout')).toBeInTheDocument();
  });

  it('shows a loading shell before configured metrics have been read', async () => {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getConfiguredMetrics').mockImplementation(() => new Promise(() => undefined));

    renderOverview(<OverviewPage />, { services: { ...services, backend: { kind: 'cdf', capabilities: CDF_CAPABILITIES } } });

    await screen.findByTestId('dimension-card-asset-hierarchy');
    expect(screen.getByTestId('configured-metrics-loading')).toBeInTheDocument();
    expect(screen.queryByTestId('configured-metrics-not-configured')).not.toBeInTheDocument();
  });

  it('opens the Configuration page from its button', async () => {
    const user = userEvent.setup();
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />, {
      services: cdfServicesWith({ models: [] }),
    });

    await user.click(await screen.findByTestId('configured-metrics-open-configuration'));

    expect(await screen.findByText('Upload custom configuration file')).toBeInTheDocument();
  });
});
