import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';

import { DimensionPopulationLegend } from './DimensionPopulationLegend';

describe(DimensionPopulationLegend.name, () => {
  it('lists in-scope segments with count and percent of in-scope total', () => {
    render(
      <DimensionPopulationLegend
        contextualised={81}
        remaining={8}
        total={89}
        t={(key: string) => translate(key, 'en')}
      />
    );

    expect(screen.getByTestId('dimension-population-legend-contextualised')).toHaveTextContent('81');
    expect(screen.getByTestId('dimension-population-legend-contextualised')).toHaveTextContent('91%');
    expect(screen.queryByTestId('dimension-population-legend-excluded')).not.toBeInTheDocument();
  });

  it('omits zero-value segments', () => {
    render(
      <DimensionPopulationLegend
        contextualised={100}
        remaining={0}
        total={100}
        t={(key: string) => translate(key, 'en')}
      />
    );

    expect(screen.getByTestId('dimension-population-legend-contextualised')).toBeInTheDocument();
    expect(screen.queryByTestId('dimension-population-legend-remaining')).not.toBeInTheDocument();
  });

  it('can hide percent labels for a minimal layout', () => {
    render(
      <DimensionPopulationLegend
        contextualised={81}
        remaining={8}
        total={89}
        showPercent={false}
        density="minimal"
        t={(key: string) => translate(key, 'en')}
      />
    );

    const row = screen.getByTestId('dimension-population-legend-contextualised');
    expect(row).toHaveTextContent('81');
    expect(row).not.toHaveTextContent('%');
  });

  it('lists both segments with zero counts when includeZeroSegments is set', () => {
    render(
      <DimensionPopulationLegend
        contextualised={0}
        remaining={801}
        total={801}
        includeZeroSegments
        t={(key: string) => translate(key, 'en')}
      />
    );

    expect(screen.getByTestId('dimension-population-legend-contextualised')).toHaveTextContent('0');
    expect(screen.getByTestId('dimension-population-legend-contextualised')).toHaveTextContent('0%');
    expect(screen.getByTestId('dimension-population-legend-remaining')).toHaveTextContent('801');
    expect(screen.getByTestId('dimension-population-legend-remaining')).toHaveTextContent('100%');
  });
});
