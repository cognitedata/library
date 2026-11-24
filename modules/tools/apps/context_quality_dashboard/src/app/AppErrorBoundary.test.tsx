import { render, screen } from '@testing-library/react';

import { AppErrorBoundary } from './AppErrorBoundary';

function Broken(): never {
  throw new Error('render failed');
}

describe('AppErrorBoundary', () => {
  it('should render its children when nothing throws', () => {
    render(
      <AppErrorBoundary>
        <p>content</p>
      </AppErrorBoundary>
    );

    expect(screen.getByText('content')).toBeInTheDocument();
  });

  it('should show a reload message instead of a blank page when a child throws', () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    render(
      <AppErrorBoundary>
        <Broken />
      </AppErrorBoundary>
    );

    expect(screen.getByTestId('app-error-boundary')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reload' })).toBeInTheDocument();
    consoleError.mockRestore();
  });
});
