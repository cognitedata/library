import { QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { HostApiProvider } from '../../app/HostApiContext';
import { ProductShell } from '../../app/ProductShell';
import { createMockServices } from '../../services/mock/createMockServices';
import { ServicesProvider } from '../../services/ServicesContext';
import { makeMockHostApi } from '../../test/mockHostApi';
import { createTestQueryClient } from '../../test/renderWithQueryClient';

function renderShell(hostInitialState?: string, api: ReturnType<typeof makeMockHostApi> | null = null) {
  const queryClient = createTestQueryClient();

  return render(
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={createMockServices({ latency: 0 })}>
        <HostApiProvider api={api}>
          <ProductShell hostInitialState={hostInitialState} />
        </HostApiProvider>
      </ServicesProvider>
    </QueryClientProvider>
  );
}

describe('App shell state', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders standalone without a host API', async () => {
    renderShell(undefined, null);

    await waitFor(() => {
      expect(screen.getByRole('tab', { name: 'Overview', selected: true })).toBeInTheDocument();
    });
  });

  it('applies valid host initial state', async () => {
    const hostState = JSON.stringify({
      page: 'dimensions',
      filters: { facilityId: 'demo-central' },
      trendWindow: '7d',
    });

    renderShell(hostState, makeMockHostApi());

    await waitFor(() => {
      expect(screen.getByRole('tab', { name: 'Dimensions', selected: true })).toBeInTheDocument();
    });
  });

  it('updates page navigation state', async () => {
    const user = userEvent.setup();
    renderShell();

    await waitFor(() => {
      expect(screen.getByRole('tab', { name: 'Overview', selected: true })).toBeInTheDocument();
    });

    await user.click(screen.getByRole('tab', { name: 'Issues' }));

    expect(screen.getByRole('tab', { name: 'Issues', selected: true })).toBeInTheDocument();
  });

  it('shows the top of the page after switching pages, not the previous page scroll position', async () => {
    const user = userEvent.setup();
    const scrollTo = vi.spyOn(window, 'scrollTo').mockImplementation(() => undefined);
    renderShell();
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: 'Overview', selected: true })).toBeInTheDocument();
    });
    scrollTo.mockClear();

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));

    await waitFor(() => {
      expect(scrollTo).toHaveBeenCalledWith({ top: 0, left: 0 });
    });
    scrollTo.mockRestore();
  });

  it('hides global filters on the configuration page', async () => {
    const user = userEvent.setup();
    renderShell();

    await waitFor(() => {
      expect(screen.getByLabelText('Global filters')).toBeInTheDocument();
    });

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));

    expect(screen.queryByLabelText('Global filters')).not.toBeInTheDocument();
  });

  it('pins the topbar and global filters together at the top', async () => {
    renderShell();

    await waitFor(() => {
      expect(screen.getByLabelText('Global filters')).toBeInTheDocument();
    });

    const header = screen.getByLabelText('Global filters').parentElement;

    expect(header).toHaveClass('sticky', 'top-0', 'flex', 'flex-col');
    expect(header?.querySelector('[data-slot="topbar"]')).toBeInTheDocument();
  });

  it('supports 7d, 30d, and 90d trend window state', async () => {
    const user = userEvent.setup();
    renderShell();

    await waitFor(() => {
      expect(screen.getByText('Trend window')).toBeInTheDocument();
    });

    const trendWindow = screen.getByRole('combobox', { name: /Trend window/i });
    await user.click(trendWindow);
    await user.click(screen.getByRole('option', { name: '7d' }));
    await user.click(trendWindow);
    await user.click(screen.getByRole('option', { name: '90d' }));

    expect(trendWindow).toHaveTextContent('90d');
  });

  it('syncs host state when host API is available', async () => {
    const api = makeMockHostApi();
    renderShell(undefined, api);

    await waitFor(() => {
      expect(api.syncInternalState).toHaveBeenCalled();
    });

    const calls = vi.mocked(api.syncInternalState).mock.calls;
    const lastCall = calls[calls.length - 1];
    expect(lastCall?.[0]).toContain('"page":"overview"');
  });

  it('starts from defaults when host state is invalid', async () => {
    renderShell('{bad-json', makeMockHostApi());

    await waitFor(() => {
      expect(screen.getByRole('tab', { name: 'Overview', selected: true })).toBeInTheDocument();
    });
  });
});
