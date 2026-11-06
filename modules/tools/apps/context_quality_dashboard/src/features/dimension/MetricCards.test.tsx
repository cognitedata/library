import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';

describe('MetricCards', () => {
  it('shows a trend chart beside the orphan rate metric card', async () => {
    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'asset-hierarchy' })}
      />,
      { services: createMockServices({ latency: 0 }) }
    );

    await waitFor(() => {
      expect(screen.getByTestId('metric-card-orphanRate')).toBeInTheDocument();
    });

    const card = await screen.findByTestId('metric-card-orphanRate');
    const openButton = within(card).getByTestId('metric-card-open-orphanRate');
    expect(openButton).toBeEnabled();
    expect(openButton.className).toContain('text-foreground');
    expect(within(card).getByTestId('dimension-trend-footer')).toBeInTheDocument();
    expect(within(card).getByTestId('metric-description-info-orphanRate')).toBeInTheDocument();
  });

  it('shows a disabled Open drilldown control with a reason when the metric has no entity list', async () => {
    const user = userEvent.setup();

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'equipment-asset' })}
      />,
      { services: createMockServices({ latency: 0 }) }
    );

    const openButton = await screen.findByTestId('metric-card-open-serialMfrCompleteness');
    expect(openButton).toBeDisabled();
    expect(screen.queryByTestId('metric-card-drilldown-disabled-info-serialMfrCompleteness')).not.toBeInTheDocument();

    expect(screen.getByTestId('metric-card-open-drilldown-trigger-serialMfrCompleteness')).toBeInTheDocument();
    expect(screen.getByTestId('metric-description-info-serialMfrCompleteness')).toBeInTheDocument();

    await user.click(openButton);
    expect(screen.queryByTestId('drilldown-sidesheet')).not.toBeInTheDocument();
  });
});
