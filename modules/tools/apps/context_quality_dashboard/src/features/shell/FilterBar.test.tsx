import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';

import { FilterBar } from './FilterBar';

const spaces = [{ id: 'demo-central', label: 'Demo Central' }];

describe(FilterBar.name, () => {
  it('renders the space and trend window filter tiles', () => {
    renderFilterBar();

    expect(screen.getByRole('combobox', { name: /Space/i })).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: /Trend window/i })).toBeInTheDocument();
    expect(screen.queryByRole('combobox', { name: /Division/i })).not.toBeInTheDocument();
  });

  it('shows All instead of the internal sentinel when no filter is selected', () => {
    renderFilterBar();

    expect(screen.getByRole('combobox', { name: /Space/i })).toHaveTextContent('All');
    expect(screen.queryByText('__all__')).not.toBeInTheDocument();
  });

  it('shows the selected space label in the space tile', () => {
    renderFilterBar({ filters: { facilityId: 'demo-central' } });

    expect(screen.getByRole('combobox', { name: /Space/i })).toHaveTextContent('Demo Central');
  });

  it('offers a location tile only when there are locations, and reports the choice', async () => {
    const user = userEvent.setup();
    const onFilterChange = vi.fn();
    renderFilterBar();
    expect(screen.queryByRole('combobox', { name: /Location/i })).not.toBeInTheDocument();

    renderFilterBar({
      locations: [
        { id: 'region', label: 'Region' },
        { id: 'site', label: '\u00A0\u00A0↳ Site' },
      ],
      filters: { locationId: 'region' },
      onFilterChange,
    });

    const tile = screen.getByRole('combobox', { name: /Location/i });
    expect(tile).toHaveTextContent('Region');
    await user.click(tile);
    await user.click(screen.getByRole('option', { name: /Site/ }));
    expect(onFilterChange).toHaveBeenCalledWith('locationId', 'site');
  });

  it('reports trend window changes', async () => {
    const user = userEvent.setup();
    const onTrendWindowChange = vi.fn();
    renderFilterBar({ onTrendWindowChange });

    await user.click(screen.getByRole('combobox', { name: /Trend window/i }));
    await user.click(screen.getByRole('option', { name: '90d' }));

    expect(onTrendWindowChange).toHaveBeenCalledWith('90d');
  });

  it('renders trailing run-status content', () => {
    renderFilterBar({ trailing: <div data-testid="run-status-slot">Run status</div> });

    expect(screen.getByTestId('run-status-slot')).toBeInTheDocument();
  });
});

type RenderOverrides = Partial<Parameters<typeof FilterBar>[0]>;

function renderFilterBar(overrides: RenderOverrides = {}) {
  return render(
    <FilterBar
      filters={{}}
      spaces={spaces}
      trendWindow="30d"
      isLoading={false}
      onFilterChange={vi.fn()}
      onTrendWindowChange={vi.fn()}
      t={(key: string) => translate(key, 'en')}
      {...overrides}
    />
  );
}
