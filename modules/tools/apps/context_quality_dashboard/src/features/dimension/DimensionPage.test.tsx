import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { CDF_CAPABILITIES } from '../../services/backendCapabilities';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';

import { DimensionPage } from './DimensionPage';

function renderDimensions(
  hostInitialState?: string,
  services = createMockServices({ latency: 0 })
) {
  return renderOverview(
    <ProductShell
      hostInitialState={JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
        ...(hostInitialState ? JSON.parse(hostInitialState) : {}),
      })}
    />,
    { services }
  );
}

describe('DimensionPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('renders the selected dimension', async () => {
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByText('Asset Hierarchy')).toBeInTheDocument();
    });
  });

  it('does not show run notes in the filter bar', async () => {
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByLabelText('Dimension')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('run-notes-button')).not.toBeInTheDocument();
  });

  it('allows selecting all seven dimensions', async () => {
    const user = userEvent.setup();
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByLabelText('Dimension')).toBeInTheDocument();
    });

    const switcher = screen.getByLabelText('Dimension');
    await user.click(switcher);
    expect(screen.getByRole('option', { name: 'Files' })).toBeInTheDocument();
    expect(screen.getAllByRole('option')).toHaveLength(7);
  });

  it('stores selected dimension in app state', async () => {
    const user = userEvent.setup();
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByLabelText('Dimension')).toBeInTheDocument();
    });

    await user.click(screen.getByLabelText('Dimension'));
    await user.click(screen.getByRole('option', { name: 'Time Series' }));

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Time Series' })).toBeInTheDocument();
    });
  });

  it('passes active filters to getDimension', async () => {
    const services = createMockServices({ latency: 0 });
    const getDimension = vi.spyOn(services.qualityDashboardService, 'getDimension');

    renderOverview(<DimensionPage />, {
      services,
      hostInitialState: JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
        filters: { facilityId: 'demo-north' },
      }),
    });

    await waitFor(() => {
      expect(getDimension).toHaveBeenCalledWith('asset-hierarchy', { facilityId: 'demo-north' });
    });
  });

  it('updates dimension query when filters change on another page', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const getDimension = vi.spyOn(services.qualityDashboardService, 'getDimension');

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({ page: 'overview', dimensionId: 'asset-hierarchy' })}
      />,
      { services }
    );

    const filtersSection = screen.getByLabelText('Global filters');
    await user.click(within(filtersSection).getAllByRole('combobox')[0]);
    await user.click(screen.getByRole('option', { name: 'Demo North' }));

    await user.click(screen.getByRole('tab', { name: 'Dimensions' }));

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Asset Hierarchy' })).toBeInTheDocument();
    });

    await waitFor(() => {
      expect(getDimension).toHaveBeenCalledWith('asset-hierarchy', { facilityId: 'demo-north' });
    });
  });

  it('renders headline quality from the DTO', async () => {
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Asset Hierarchy' })).toBeInTheDocument();
    });

    expect(screen.getByRole('heading', { name: 'Asset Hierarchy' })).toBeInTheDocument();
    const summaryCard = screen.getByTestId('dimension-summary-card');
    expect(within(summaryCard).getByTestId('dimension-readiness')).toBeInTheDocument();
    expect(within(summaryCard).getByTestId('dimension-coverage-percent')).toBeInTheDocument();
  });

  it('renders tier scores from the DTO', async () => {
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByText('Quality by tier')).toBeInTheDocument();
    });

    expect(screen.getByText('Tier 1')).toBeInTheDocument();
  });

  it('renders metric cards from the DTO', async () => {
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByTestId('metric-card-orphanRate')).toBeInTheDocument();
    });
    expect(screen.queryByTestId('metric-card-hierarchyCompletionRate')).not.toBeInTheDocument();
  });

  it('uses locked metric IDs', async () => {
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByTestId('metric-card-orphansByTier')).toBeInTheDocument();
    });

    expect(screen.getByTestId('metric-card-orphanRate')).toBeInTheDocument();
  });

  it('loads drilldown data for the selected metric', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const getDrilldown = vi.spyOn(services.qualityDashboardService, 'getDrilldown');

    renderOverview(<DimensionPage />, {
      services,
      hostInitialState: JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
      }),
    });

    await waitFor(() => {
      expect(screen.getByTestId('metric-card-orphansByTier')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('metric-card-open-orphansByTier'));

    await waitFor(() => {
      expect(getDrilldown).toHaveBeenCalledWith('orphansByTier', {});
    });
  });

  it('renders drilldown entity rows', async () => {
    const user = userEvent.setup();
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByTestId('metric-card-orphansByTier')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('metric-card-open-orphansByTier'));

    await waitFor(() => {
      expect(screen.getByTestId('drilldown-table')).toBeInTheDocument();
    });

    expect(screen.getAllByTestId(/drilldown-row-/).length).toBeGreaterThan(0);
  });

  it('renders suggested action for drilldown', async () => {
    const user = userEvent.setup();
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByTestId('metric-card-orphansByTier')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('metric-card-open-orphansByTier'));

    await waitFor(() => {
      expect(screen.getByText(/Suggested action:/)).toBeInTheDocument();
    });

    expect(screen.getByText(/Download the full list/)).toBeInTheDocument();
  });

  it('handles disabled optional dimensions gracefully', async () => {
    const services = createMockServices({ latency: 0 });
    await services.configurationService.updateConfig({ model3dAvailable: false });

    renderOverview(<DimensionPage />, {
      services,
      hostInitialState: JSON.stringify({ page: 'dimensions', dimensionId: 'model-3d' }),
    });

    await waitFor(() => {
      expect(screen.getByText('This optional dimension is disabled in configuration.')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('metric-card-threeDToAssetRate')).not.toBeInTheDocument();
    await services.configurationService.updateConfig({ model3dAvailable: true });
  });

  it('renders empty state without fake zero quality', async () => {
    renderOverview(<DimensionPage />, {
      hostInitialState: JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
        filters: { facilityId: 'missing-facility' },
      }),
    });

    await waitFor(() => {
      expect(screen.getByText('No data for this scope')).toBeInTheDocument();
    });

    expect(screen.queryByText('Quality by tier')).not.toBeInTheDocument();
  });

  it('offers the workbook of all unlinked items in the drill-down instead of a capped CSV', async () => {
    renderOverview(<DimensionPage />, {
      hostInitialState: JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
        metricId: 'orphansByTier',
      }),
    });

    expect(await screen.findByRole('button', { name: 'Download all unlinked (Excel)' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Export CSV' })).not.toBeInTheDocument();
  });

  it('invokes notifyOwner for selected entities', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const notifyOwner = vi.spyOn(services.qualityDashboardService, 'notifyOwner');

    renderOverview(<DimensionPage />, {
      services,
      hostInitialState: JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
        metricId: 'orphansByTier',
      }),
    });

    await waitFor(() => {
      expect(screen.getByTestId('drilldown-table')).toBeInTheDocument();
    });

    const firstCheckbox = screen.getAllByRole('checkbox')[0];
    await user.click(firstCheckbox);
    await user.click(screen.getByRole('button', { name: 'Notify owner' }));

    await waitFor(() => {
      expect(notifyOwner).toHaveBeenCalled();
    });
  });

  it('renders notification success state', async () => {
    const user = userEvent.setup();

    renderOverview(<DimensionPage />, {
      hostInitialState: JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
        metricId: 'orphansByTier',
      }),
    });

    await waitFor(() => {
      expect(screen.getByTestId('drilldown-table')).toBeInTheDocument();
    });

    await user.click(screen.getAllByRole('checkbox')[0]);
    await user.click(screen.getByRole('button', { name: 'Notify owner' }));

    await waitFor(() => {
      expect(screen.getByText('Notification sent')).toBeInTheDocument();
    });
  });

  it('uses the trend window carried in host state, with no filter bar control on this page', async () => {
    const services = createMockServices({ latency: 0 });
    const getSnapshots = vi.spyOn(services.qualityDashboardService, 'getSnapshots');

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
          trendWindow: '90d',
        })}
      />,
      { services }
    );

    await waitFor(() => {
      expect(getSnapshots).toHaveBeenCalledWith(expect.any(String), '90d', expect.any(Object));
    });

    expect(screen.queryByRole('combobox', { name: /Trend window/i })).not.toBeInTheDocument();
  });

  it('renders lifecycle warnings when provided', async () => {
    const services = createMockServices({ latency: 0 });
    await services.configurationService.updateConfig({ lifecycleMetadataAvailable: true });

    renderOverview(<DimensionPage />, {
      services,
      hostInitialState: JSON.stringify({
        page: 'dimensions',
        dimensionId: 'asset-hierarchy',
      }),
    });

    await waitFor(() => {
      expect(screen.getByText('Lifecycle and cross-view warnings')).toBeInTheDocument();
    });
  });

  it('renders English translations', async () => {
    renderDimensions();

    await waitFor(() => {
      expect(screen.getByText('Metrics')).toBeInTheDocument();
    });
  });

  it('renders Japanese translations', async () => {
    localStorage.setItem('context-quality-dashboard.language', 'ja');

    renderDimensions();

    await waitFor(() => {
      expect(screen.getByText('メトリクス')).toBeInTheDocument();
    });
  });

  it('renders configured view metrics when scope is configured', async () => {
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
          dimensionsScope: 'configured',
          configuredGroupId: 'a:Model:v1/Widget',
          configuredModelId: 'a:Model:v1',
        })}
      />,
      { services: { ...services, backend: { kind: 'cdf', capabilities: CDF_CAPABILITIES } } }
    );

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Widget' })).toBeInTheDocument();
    });

    expect(screen.queryByTestId('dimension-summary-card')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Rerun Files' })).not.toBeInTheDocument();
    expect(await screen.findByRole('button', { name: 'Rerun Widget' })).toBeInTheDocument();
  });
});

describe('Overview to dimension navigation', () => {
  it('opens the selected dimension from overview', async () => {
    const user = userEvent.setup();

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'overview' })} />);

    await waitFor(() => {
      expect(screen.getByTestId('dimension-card-asset-hierarchy')).toBeInTheDocument();
    });

    const card = screen.getByTestId('dimension-card-asset-hierarchy');
    await user.click(within(card).getByRole('button', { name: 'Open dimension' }));

    await waitFor(() => {
      expect(screen.getByLabelText('Dimension')).toBeInTheDocument();
    });

    expect(screen.getByRole('heading', { name: 'Asset Hierarchy' })).toBeInTheDocument();
  });
});
