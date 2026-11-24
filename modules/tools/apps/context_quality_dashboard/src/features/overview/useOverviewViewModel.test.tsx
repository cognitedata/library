import { QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import type { ComponentType, ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';

import { HostApiProvider } from '../../app/HostApiContext';
import { AppStateProvider } from '../../app/state/AppStateProvider';
import { useAppState } from '../../app/state/useAppState';
import { LanguageProvider } from '../../i18n/LanguageProvider';
import { createMockServices } from '../../services/mock/createMockServices';
import { ServicesProvider } from '../../services/ServicesContext';
import type { makeMockHostApi } from '../../test/mockHostApi';
import { createTestQueryClient } from '../../test/renderWithQueryClient';

import { useOverviewViewModel } from './useOverviewViewModel';

describe(useOverviewViewModel.name, () => {
  it('exposes loading then success presentation values', async () => {
    const { result } = renderHook(() => useOverviewViewModel(), { wrapper: createWrapper() });

    expect(result.current.isLoading).toBe(true);

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.isError).toBe(false);
    expect(result.current.presentation?.dimensions.length).toBeGreaterThan(0);
  });

  it('calls refresh on the quality dashboard service', async () => {
    const services = createMockServices({ latency: 0 });
    const refresh = vi.spyOn(services.qualityDashboardService, 'refresh');
    const { result } = renderHook(() => useOverviewViewModel(), {
      wrapper: createWrapper(services),
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    act(() => {
      result.current.refresh();
    });

    await waitFor(() => {
      expect(refresh).toHaveBeenCalled();
    });
  });

  it('exposes an error state when overview fails', async () => {
    const services = createMockServices({ latency: 0 });
    const { result } = renderHook(() => useOverviewViewModel(), {
      wrapper: createWrapper({
        ...services,
        qualityDashboardService: {
          ...services.qualityDashboardService,
          getOverview: () => Promise.reject(new Error('overview failed')),
        },
      }),
    });

    await waitFor(() => {
      expect(result.current.isError).toBe(true);
    });

    expect(result.current.presentation).toBeUndefined();
  });

  it('navigates to the dimensions page without selecting a dimension', async () => {
    const { result } = renderHook(
      () => {
        const viewModel = useOverviewViewModel();
        const { state } = useAppState();
        return { viewModel, state };
      },
      { wrapper: createWrapper() }
    );

    await waitFor(() => {
      expect(result.current.viewModel.isLoading).toBe(false);
    });

    act(() => {
      result.current.viewModel.navigateToDimensions();
    });

    expect(result.current.state.page).toBe('dimensions');
  });
});

function createWrapper(
  services = createMockServices({ latency: 0 }),
  api: ReturnType<typeof makeMockHostApi> | null = null
): ComponentType<{ children: ReactNode }> {
  const queryClient = createTestQueryClient();
  return ({ children }) => (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>
        <HostApiProvider api={api}>
          <LanguageProvider>
            <AppStateProvider>{children}</AppStateProvider>
          </LanguageProvider>
        </HostApiProvider>
      </ServicesProvider>
    </QueryClientProvider>
  );
}
