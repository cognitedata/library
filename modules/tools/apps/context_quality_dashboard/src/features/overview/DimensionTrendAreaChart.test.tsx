import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';
import type { SnapshotPointDto } from '../../types';

import { DimensionTrendAreaChart } from './DimensionTrendAreaChart';

describe(DimensionTrendAreaChart.name, () => {
  it('renders area path and line for snapshot points', () => {
    const { container } = renderChart(points([60, 65, 70]));

    expect(screen.getByTestId('dimension-trend-chart-time-series')).toBeInTheDocument();
    expect(container.querySelector('path[d*="Z"]')).not.toBeNull();
    expect(container.querySelector('polyline')).not.toBeNull();
  });

  it('shows empty copy centered in the chart-sized frame when there are no points', () => {
    renderChart([]);

    const frame = screen.getByTestId('dimension-trend-chart-time-series');
    expect(frame).toHaveClass('h-24');
    expect(screen.getByText('Not enough history yet')).toBeInTheDocument();
  });

  it('shows tooltip with contextualised counts and change on hover', async () => {
    const user = userEvent.setup();
    renderChart(points([60, 84, 70]));

    await user.hover(screen.getByTestId('dimension-trend-plot-time-series'));

    const tooltip = screen.getByTestId('dimension-trend-tooltip-time-series');
    expect(tooltip).toHaveTextContent('58 / 97 (60%)');
    expect(tooltip).toHaveTextContent('No change vs previous point');
    expect(tooltip.textContent).toMatch(/2026/);
  });
});

function renderChart(points: SnapshotPointDto[]) {
  return render(
    <DimensionTrendAreaChart
      chartId="time-series"
      points={points}
      totalIncludingExcluded={97}
      t={(key: string) => translate(key, 'en')}
    />
  );
}

function points(values: readonly number[]): SnapshotPointDto[] {
  return values.map((valuePercent, index) => ({
    metricId: 'tsToAssetRate',
    capturedAt: new Date(Date.UTC(2026, 2, 1 + index * 7)).toISOString(),
    valuePercent,
    scope: {},
  }));
}
