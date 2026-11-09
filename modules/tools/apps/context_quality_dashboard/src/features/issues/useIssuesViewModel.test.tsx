import { QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import type { ComponentType, ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';

import { HostApiProvider } from '../../app/HostApiContext';
import { AppStateProvider } from '../../app/state/AppStateProvider';
import { LanguageProvider } from '../../i18n/LanguageProvider';
import { createMockServices } from '../../services/mock/createMockServices';
import { ServicesProvider } from '../../services/ServicesContext';
import { createTestQueryClient } from '../../test/renderWithQueryClient';

import { useIssuesViewModel } from './useIssuesViewModel';

describe(useIssuesViewModel.name, () => {
  it('exposes loading then success values', async () => {
    const { result } = renderHook(() => useIssuesViewModel(), { wrapper: createWrapper() });

    expect(result.current.isLoading).toBe(true);

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.isError).toBe(false);
    expect(result.current.issues.length).toBeGreaterThan(0);
  });

  it('shows the metrics that have arrived while the others are still loading', async () => {
    const services = createMockServices({ latency: 0 });
    const real = services.qualityDashboardService.getIssuesOfMetric.bind(services.qualityDashboardService);
    vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric').mockImplementation((metricId, filters) =>
      metricId === 'orphansByTier' ? real(metricId, filters) : new Promise(() => undefined)
    );
    const { result } = renderHook(() => useIssuesViewModel(), { wrapper: createWrapper(services) });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.issues.length).toBeGreaterThan(0);
    expect(result.current.isLoadingMore).toBe(true);
    expect(result.current.isError).toBe(false);
  });

  it('reports the metrics that could not be loaded while keeping the others', async () => {
    const services = createMockServices({ latency: 0 });
    const real = services.qualityDashboardService.getIssuesOfMetric.bind(services.qualityDashboardService);
    vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric').mockImplementation((metricId, filters) =>
      metricId === 'staleTimeSeries' ? Promise.reject(new Error('x')) : real(metricId, filters)
    );
    const { result } = renderHook(() => useIssuesViewModel(), { wrapper: createWrapper(services) });

    await waitFor(() => {
      expect(result.current.failedCount).toBe(1);
    });

    expect(result.current.isError).toBe(false);
    expect(result.current.issues.length).toBeGreaterThan(0);
  });

  it('exposes an error state when issues fail to load', async () => {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getIssuesOfMetric').mockRejectedValue(new Error('issues failed'));
    const { result } = renderHook(() => useIssuesViewModel(), { wrapper: createWrapper(services) });

    await waitFor(() => {
      expect(result.current.isError).toBe(true);
    });

    expect(result.current.issues).toEqual([]);
  });
});

function createWrapper(
  services = createMockServices({ latency: 0 })
): ComponentType<{ children: ReactNode }> {
  const queryClient = createTestQueryClient();
  return ({ children }) => (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>
        <HostApiProvider api={null}>
          <LanguageProvider>
            <AppStateProvider hostInitialState={JSON.stringify({ page: 'issues' })}>
              {children}
            </AppStateProvider>
          </LanguageProvider>
        </HostApiProvider>
      </ServicesProvider>
    </QueryClientProvider>
  );
}
