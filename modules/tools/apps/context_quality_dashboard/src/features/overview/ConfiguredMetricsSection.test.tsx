import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';
import type { ConfiguredDataModelDto } from '../../types';

import { ConfiguredMetricsSection } from './ConfiguredMetricsSection';

// Mocked: the trend history comes from a live CDF read that this test does not exercise, and the hook has no injection seam.
vi.mock('./useConfiguredMetricTrends', () => ({
  useConfiguredMetricTrends: () => new Map(),
  hasConfiguredMetricTrendHistory: () => false,
}));

const MODELS: ConfiguredDataModelDto[] = [
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
            totalCount: 1326,
            linkedCount: 1246,
            coveragePercent: 94,
          },
          {
            label: 'colour',
            modelId: 'a:Model:v1',
            view: 'Widget',
            totalCount: 0,
            linkedCount: 0,
            coveragePercent: null,
          },
        ],
      },
    ],
  },
  {
    id: 'b:Other:v2',
    label: 'Other',
    groups: [
      {
        id: 'b:Other:v2/Gadget',
        modelId: 'b:Other:v2',
        modelLabel: 'Other',
        viewLabel: 'Gadget',
        metrics: [
          {
            label: 'maker',
            modelId: 'b:Other:v2',
            view: 'Gadget',
            totalCount: 10,
            linkedCount: 5,
            coveragePercent: 50,
          },
        ],
      },
    ],
  },
];

describe(ConfiguredMetricsSection.name, () => {
  it('shows trend window controls matching dimension overview', () => {
    renderSection([MODELS[0]]);

    expect(screen.getByTestId('configured-metrics-trend-window')).toBeInTheDocument();
    expect(screen.getByTestId('configured-metrics-trend-window-7d')).toBeInTheDocument();
    expect(screen.getByTestId('configured-metrics-trend-window-30d')).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByTestId('configured-metrics-trend-window-90d')).toBeInTheDocument();
  });

  it('shows the selected view and stacked metrics with coverage', () => {
    renderSection([MODELS[0]]);

    expect(screen.getByText('Custom configured metrics')).toBeInTheDocument();
    expect(screen.getByText('Explore contextualisation coverage by view and metric.')).toBeInTheDocument();
    expect(screen.getByTestId('configured-metrics-layout')).toBeInTheDocument();
    expect(screen.getByTestId('configured-group-a:Model:v1/Widget')).toBeInTheDocument();
    const ownerRow = screen.getByTestId('configured-metric-a:Model:v1/Widget.owner');
    expect(within(ownerRow).getByRole('heading', { name: 'Widget → owner' })).toBeInTheDocument();
    expect(within(ownerRow).getByText('94% coverage')).toBeInTheDocument();
    expect(within(ownerRow).getByText('1,246 / 1,326')).toBeInTheDocument();
    expect(within(ownerRow).getByTestId('dimension-population-legend-contextualised')).toBeInTheDocument();
    expect(within(ownerRow).getByTestId('dimension-population-legend-remaining')).toBeInTheDocument();
  });

  it('shows a dash, not 0%, for a metric with nothing to count', () => {
    renderSection([MODELS[0]]);

    const row = screen.getByTestId('configured-metric-a:Model:v1/Widget.colour');
    expect(within(row).getByText('No instances to measure for this metric.')).toBeInTheDocument();
    expect(within(row).queryByText('0% coverage')).not.toBeInTheDocument();
  });

  it('shows nothing when the run was not given a config file and demo-style empty', () => {
    const { container } = renderSection([]);

    expect(container).toBeEmptyDOMElement();
  });

  it('shows the not-configured empty state when requested', () => {
    renderSection([], {
      showNotConfiguredEmpty: true,
      onOpenConfiguration: vi.fn(),
    });

    expect(screen.getByTestId('configured-metrics-not-configured-shell')).toBeInTheDocument();
    expect(screen.getByTestId('configured-metrics-not-configured')).toHaveTextContent(
      'Measure your own data models'
    );
  });

  it('shows a loading skeleton while configured metrics are loading', () => {
    renderSection([], { isConfiguredLoaded: false, showNotConfiguredEmpty: true });

    expect(screen.getByTestId('configured-metrics-loading')).toBeInTheDocument();
    expect(screen.queryByTestId('configured-metrics-not-configured')).not.toBeInTheDocument();
  });

  it('shows a data model dropdown above the view list', () => {
    renderSection([MODELS[0]]);

    const trigger = screen.getByTestId('configured-metrics-model-select');
    expect(within(screen.getByTestId('configured-metrics-sidebar-model')).getByText('Data model')).toBeInTheDocument();
    expect(trigger).toHaveTextContent('Model');
    expect(screen.getByTestId('configured-view-nav-a:Model:v1/Widget')).toHaveTextContent('Model | 2 metrics');
  });

  it('switches detail panel when another view is selected in the sidebar', async () => {
    const user = userEvent.setup();
    const twoViews: ConfiguredDataModelDto[] = [
      {
        id: 'a:Model:v1',
        label: 'Model',
        groups: [
          MODELS[0].groups[0],
          {
            id: 'a:Model:v1/OtherView',
            modelId: 'a:Model:v1',
            modelLabel: 'Model',
            viewLabel: 'OtherView',
            metrics: [
              {
                label: 'size',
                modelId: 'a:Model:v1',
                view: 'OtherView',
                totalCount: 5,
                linkedCount: 5,
                coveragePercent: 100,
              },
            ],
          },
        ],
      },
    ];
    renderSection(twoViews);

    expect(screen.getByTestId('configured-metric-a:Model:v1/Widget.owner')).toBeInTheDocument();
    await user.click(screen.getByTestId('configured-view-nav-a:Model:v1/OtherView'));
    expect(screen.queryByTestId('configured-metric-a:Model:v1/Widget.owner')).not.toBeInTheDocument();
    expect(screen.getByTestId('configured-metric-a:Model:v1/OtherView.size')).toBeInTheDocument();
  });

  it('switches views when another data model is chosen in the dropdown', async () => {
    const user = userEvent.setup();
    renderSection(MODELS);

    expect(screen.getByTestId('configured-metrics-model-select')).toBeInTheDocument();
    expect(screen.getByTestId('configured-group-a:Model:v1/Widget')).toBeInTheDocument();

    await user.click(screen.getByTestId('configured-metrics-model-select'));
    await user.click(screen.getByRole('option', { name: 'Other' }));

    expect(screen.queryByTestId('configured-group-a:Model:v1/Widget')).not.toBeInTheDocument();
    expect(screen.getByTestId('configured-group-b:Other:v2/Gadget')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Gadget → maker' })).toBeInTheDocument();
  });

  it('shows an empty state when no views are configured', () => {
    renderSection([
      {
        id: 'empty:Model:v1',
        label: 'Empty',
        groups: [],
      },
    ]);

    expect(screen.getByTestId('configured-metrics-empty')).toHaveTextContent(
      'No configured metrics are available for the current filters.'
    );
  });
});

function renderSection(
  models: ConfiguredDataModelDto[],
  overrides?: Partial<{
    isConfiguredLoaded: boolean;
    showNotConfiguredEmpty: boolean;
    canStartRun: boolean;
    onOpenConfiguration: () => void;
  }>
) {
  return render(
    <ConfiguredMetricsSection
      models={models}
      filters={{}}
      isConfiguredLoaded={true}
      showNotConfiguredEmpty={false}
      canStartRun={true}
      onOpenConfiguration={() => {}}
      t={(key) => translate(key, 'en')}
      {...overrides}
    />
  );
}
