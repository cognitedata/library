import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';

import type { DimensionCardCounts } from './dimensionCardCounts';
import { DimensionOverviewCard } from './DimensionOverviewCard';
import type { DimensionPresentation } from './overviewPresentation';

describe(DimensionOverviewCard.name, () => {
  it('shows coverage in the header and population breakdown with trend', () => {
    renderCard();

    expect(screen.getByText('Time Series')).toBeInTheDocument();
    expect(screen.getByTestId('dimension-card-coverage')).toHaveTextContent('62% coverage');
    expect(screen.getByText('4,960 / 8,000')).toBeInTheDocument();
    expect(screen.queryByTestId('dimension-population-legend-excluded')).not.toBeInTheDocument();
    expect(screen.getByTestId('dimension-population-bar')).toBeInTheDocument();
    expect(screen.getByTestId('dimension-population-legend')).toBeInTheDocument();
    expect(screen.getByTestId('dimension-population-legend-contextualised')).toBeInTheDocument();
  });

  it('shows annotations as a count with an approved / not approved split and a trend, and no coverage share', () => {
    render(
      <DimensionOverviewCard
        dimension={{ ...sampleDimension(), dimensionId: 'file-annotation', nameKey: 'dimension.file-annotation' }}
        counts={{ ...sampleCounts(), dimensionId: 'file-annotation' }}
        trendPoints={[]}
        trendLoading={false}
        t={(key: string) => translate(key, 'en')}
        onOpen={() => undefined}
      />
    );

    expect(screen.getByTestId('dimension-card-count')).toHaveTextContent('8,000 annotations');
    expect(screen.queryByTestId('dimension-card-coverage')).not.toBeInTheDocument();
    expect(screen.queryByText('4,960 / 8,000')).not.toBeInTheDocument();
    expect(screen.getByTestId('dimension-population-legend-contextualised')).toHaveTextContent('Approved');
    expect(screen.getByTestId('dimension-population-legend-remaining')).toHaveTextContent('Not approved');
    expect(screen.getByTestId('dimension-trend-footer')).toBeInTheDocument();
  });

  it('does not show since-last-run copy', () => {
    renderCard();

    expect(screen.queryByText('since last run')).not.toBeInTheDocument();
  });

  it('renders the trend footer with area chart', () => {
    renderCard();

    expect(screen.getByTestId('dimension-trend-footer')).toBeInTheDocument();
    expect(screen.getByTestId('dimension-trend-chart-time-series')).toBeInTheDocument();
    expect(screen.queryByTestId('dimension-overview-trend-window-30d')).not.toBeInTheDocument();
  });

  it('places the open control below the trend section', () => {
    renderCard();

    const openButton = screen.getByTestId('dimension-card-open');
    const trendFooter = screen.getByTestId('dimension-trend-footer');

    expect(openButton).toHaveTextContent('Open dimension');
    expect(openButton.className).toContain('mt-auto');
    expect(trendFooter.compareDocumentPosition(openButton) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it('reserves chart height when trend history is empty', () => {
    render(
      <DimensionOverviewCard
        dimension={sampleDimension()}
        counts={sampleCounts()}
        trendPoints={[]}
        trendLoading={false}
        t={(key: string) => translate(key, 'en')}
        onOpen={() => undefined}
      />
    );

    expect(screen.getByText('Not enough history yet')).toBeInTheDocument();
    expect(screen.getByTestId('dimension-trend-chart-time-series')).toHaveClass('h-24');
  });

  it('renders a disabled dimension without a zero score or open action', () => {
    render(
      <DimensionOverviewCard
        dimension={{
          ...sampleDimension(),
          dimensionId: 'model-3d',
          nameKey: 'dimension.model-3d',
          enabled: false,
          overallPercent: null,
          issueCount: null,
        }}
        counts={undefined}
        trendPoints={[]}
        trendLoading={false}
        t={(key: string) => translate(key, 'en')}
        onOpen={() => undefined}
      />
    );

    expect(screen.getByText('3D Model')).toBeInTheDocument();
    expect(screen.getByText('Disabled')).toBeInTheDocument();
    expect(screen.queryByTestId('dimension-trend-footer')).not.toBeInTheDocument();
  });

  it('calls onOpen from the open dimension control', async () => {
    const user = userEvent.setup();
    const onOpen = vi.fn();
    renderCard({ onOpen });

    await user.click(screen.getByRole('button', { name: 'Open dimension' }));

    expect(onOpen).toHaveBeenCalledTimes(1);
  });

});

function renderCard(overrides?: {
  onOpen?: () => void;
}) {
  return render(
    <DimensionOverviewCard
      dimension={sampleDimension()}
      counts={sampleCounts()}
      trendPoints={[
        {
          metricId: 'tsToAssetRate',
          capturedAt: '2026-01-01T00:00:00.000Z',
          valuePercent: 60,
          scope: {},
        },
        {
          metricId: 'tsToAssetRate',
          capturedAt: '2026-01-08T00:00:00.000Z',
          valuePercent: 62,
          scope: {},
        },
      ]}
      trendLoading={false}
      t={(key: string) => translate(key, 'en')}
      onOpen={overrides?.onOpen ?? (() => undefined)}
    />
  );
}

function sampleDimension(): DimensionPresentation {
  return {
    dimensionId: 'time-series',
    nameKey: 'dimension.time-series',
    enabled: true,
    optional: false,
    overallPercent: 62,
    issueCount: 6,
  };
}

function sampleCounts(): DimensionCardCounts {
  return {
    dimensionId: 'time-series',
    nameKey: 'dimension.time-series',
    contextualisedCount: 4960,
    inScopeCount: 8000,
    contextualisedPercent: 62,
    unresolvedCount: 3040,
  };
}
