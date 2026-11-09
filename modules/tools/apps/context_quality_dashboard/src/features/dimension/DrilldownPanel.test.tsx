import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';

describe('DrilldownPanel', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('opens drilldown in a side sheet when a metric with drilldown is selected', async () => {
    const user = userEvent.setup();

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
        })}
      />,
      { services: createMockServices({ latency: 0 }) }
    );

    await waitFor(() => {
      expect(screen.getByTestId('metric-card-orphansByTier')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('drilldown-sidesheet')).not.toBeInTheDocument();

    await user.click(screen.getByTestId('metric-card-open-orphansByTier'));

    await waitFor(() => {
      expect(screen.getByTestId('drilldown-sidesheet')).toBeInTheDocument();
    });
    expect(screen.getByRole('dialog')).toHaveAttribute('data-testid', 'drilldown-sidesheet');
    expect(screen.getByTestId('drilldown-table')).toBeInTheDocument();
    expect(screen.getByTestId('drilldown-sidesheet-resize')).toBeInTheDocument();
  });

  it('closes the side sheet when the close control is activated', async () => {
    const user = userEvent.setup();

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
          metricId: 'orphansByTier',
        })}
      />,
      { services: createMockServices({ latency: 0 }) }
    );

    await waitFor(() => {
      expect(screen.getByTestId('drilldown-sidesheet')).toBeInTheDocument();
    });

    await user.click(screen.getByRole('button', { name: 'Close drilldown' }));

    await waitFor(() => {
      expect(screen.queryByTestId('drilldown-sidesheet')).not.toBeInTheDocument();
    });
  });

  it('selects and deselects all visible drilldown rows from the header checkbox', async () => {
    const user = userEvent.setup();

    renderOverview(
      <ProductShell
        hostInitialState={JSON.stringify({
          page: 'dimensions',
          dimensionId: 'asset-hierarchy',
          metricId: 'orphansByTier',
        })}
      />,
      { services: createMockServices({ latency: 0 }) }
    );

    await waitFor(() => {
      expect(screen.getByTestId('drilldown-table')).toBeInTheDocument();
    });

    const selectAll = screen.getByTestId('drilldown-select-all');
    const rowCheckboxes = (): HTMLInputElement[] =>
      screen.getAllByRole('checkbox').filter((input): input is HTMLInputElement => input !== selectAll);

    expect(rowCheckboxes().every((input) => !input.checked)).toBe(true);

    await user.click(selectAll);

    await waitFor(() => {
      expect(rowCheckboxes().every((input) => input.checked)).toBe(true);
    });

    await user.click(selectAll);

    await waitFor(() => {
      expect(rowCheckboxes().every((input) => !input.checked)).toBe(true);
    });
  });
});
