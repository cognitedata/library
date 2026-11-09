import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { LOADING_HEADER, LoadingFallback } from './LoadingFallback';
import { bootLoadingDescription } from './loadingSteps';

describe(LoadingFallback.name, () => {
  it('shows a centered page loader with the Cognite logo animation and loading copy', () => {
    const { container } = render(<LoadingFallback />);

    expect(screen.getByRole('status', { name: 'Loading' })).toBeInTheDocument();
    expect(screen.getByText(LOADING_HEADER)).toBeInTheDocument();
    expect(container.querySelector('[data-slot="loader-rect"]')).toBeInTheDocument();
    expect(container.querySelector('.min-h-screen')).toBeInTheDocument();
  });

  it('should show the boot step detail as description when a step is provided', () => {
    render(<LoadingFallback step="connect-fusion" />);

    expect(screen.getByText(bootLoadingDescription('connect-fusion'))).toBeInTheDocument();
    expect(screen.queryByTestId('boot-step-trail')).not.toBeInTheDocument();
  });
});
