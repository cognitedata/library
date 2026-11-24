import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';
import type { ConfiguredGroupDto } from '../../types';

import { ConfiguredViewMetricsPanel } from './ConfiguredViewMetricsPanel';

// Mocked: the trend history comes from a live CDF read that this test does not exercise, and the hook has no injection seam.
vi.mock('../overview/useConfiguredMetricTrends', () => ({
  useConfiguredMetricTrends: () => new Map(),
  hasConfiguredMetricTrendHistory: () => false,
}));

const GROUP: ConfiguredGroupDto = {
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
};

describe(ConfiguredViewMetricsPanel.name, () => {
  it('renders loading state', () => {
    render(
      <ConfiguredViewMetricsPanel
        group={undefined}
        isLoading
        isError={false}
        filters={{}}
        trendWindow="30d"
        t={(key) => translate(key, 'en')}
      />
    );

    expect(screen.getByLabelText('Loading configured view metrics')).toBeInTheDocument();
  });

  it('renders metric cards for a configured group', () => {
    render(
      <ConfiguredViewMetricsPanel
        group={GROUP}
        isLoading={false}
        isError={false}
        filters={{}}
        trendWindow="30d"
        t={(key) => translate(key, 'en')}
      />
    );

    expect(screen.getByRole('heading', { name: 'Widget' })).toBeInTheDocument();
    expect(screen.getByText('Model')).toBeInTheDocument();
    expect(screen.getByText('Metrics')).toBeInTheDocument();
  });

  it('renders not found when group is missing after load', () => {
    render(
      <ConfiguredViewMetricsPanel
        group={undefined}
        isLoading={false}
        isError={false}
        filters={{}}
        trendWindow="30d"
        t={(key) => translate(key, 'en')}
      />
    );

    expect(screen.getByText('View not found')).toBeInTheDocument();
  });
});
