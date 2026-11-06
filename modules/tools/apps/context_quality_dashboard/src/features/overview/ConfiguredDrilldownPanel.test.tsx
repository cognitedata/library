import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';
import type { ConfiguredDataModelDto, ConfiguredDrilldownDto } from '../../types';

import { ConfiguredMetricsSection } from './ConfiguredMetricsSection';

// Mocked: the trend history comes from a live CDF read that this test does not exercise, and the hook has no injection seam.
vi.mock('./useConfiguredMetricTrends', () => ({
  useConfiguredMetricTrends: () => new Map(),
  hasConfiguredMetricTrendHistory: () => false,
}));

const KEY = { modelId: 'a:Model:v1', view: 'Widget', property: 'owner' };

function models(linked: number): ConfiguredDataModelDto[] {
  return [
    {
      id: KEY.modelId,
      label: 'Model',
      groups: [
        {
          id: `${KEY.modelId}/Widget`,
          modelId: KEY.modelId,
          modelLabel: 'Model',
          viewLabel: 'Widget',
          metrics: [
            { label: 'owner', modelId: KEY.modelId, view: 'Widget', totalCount: 10, linkedCount: linked, coveragePercent: linked * 10 },
          ],
        },
      ],
    },
  ];
}

function drilldown(partial: Partial<ConfiguredDrilldownDto> = {}): ConfiguredDrilldownDto {
  return {
    key: KEY,
    rows: [
      { id: 'north:w1', entity: 'Widget One', entityId: 'w1', space: 'north', facilityId: 'north', detail: 'link not set' },
      { id: 'north:w2', entity: 'w2', entityId: 'w2', space: 'north', facilityId: 'north', detail: 'link not set' },
    ],
    cutAt: null,
    dropped: 0,
    ...partial,
  };
}

function renderSection(linked: number, result: ConfiguredDrilldownDto) {
  const services = createMockServices({ latency: 0 });
  vi.spyOn(services.qualityDashboardService, 'getConfiguredDrilldown').mockResolvedValue(result);
  renderOverview(
    <ConfiguredMetricsSection
      models={models(linked)}
      filters={{}}
      isConfiguredLoaded={true}
      showNotConfiguredEmpty={false}
      canStartRun={true}
      onOpenConfiguration={() => {}}
      t={(key) => key}
    />,
    { services }
  );
}

describe('the drill-down of a configured metric', () => {
  it('opens the instances without the link in the side sheet', async () => {
    const user = userEvent.setup();
    renderSection(6, drilldown());

    await user.click(screen.getByTestId('configured-metric-open-drilldown-a:Model:v1/Widget.owner'));

    const sheet = await screen.findByTestId('drilldown-sidesheet');
    const table = await within(sheet).findByTestId('configured-drilldown-table');
    expect(within(table).getByText('Widget One')).toBeInTheDocument();
    expect(within(table).getByText('w1')).toBeInTheDocument();
    expect(within(table).getAllByText('link not set')).toHaveLength(2);
  });

  it('says when the run cut the list', async () => {
    const user = userEvent.setup();
    renderSection(6, drilldown({ cutAt: 500, dropped: 40 }));

    await user.click(screen.getByTestId('configured-metric-open-drilldown-a:Model:v1/Widget.owner'));

    expect(await screen.findByTestId('configured-drilldown-cut')).toBeInTheDocument();
  });

  it('closes with the close button', async () => {
    const user = userEvent.setup();
    renderSection(6, drilldown());

    await user.click(screen.getByTestId('configured-metric-open-drilldown-a:Model:v1/Widget.owner'));
    await user.click(await screen.findByRole('button', { name: 'dimension.closeDrilldown' }));

    await waitFor(() => {
      expect(screen.queryByTestId('drilldown-sidesheet')).not.toBeInTheDocument();
    });
  });

  it('offers no drill-down when every instance has the link', () => {
    renderSection(10, drilldown({ rows: [] }));

    expect(screen.getByTestId('configured-metric-open-drilldown-a:Model:v1/Widget.owner')).toBeDisabled();
  });
});
