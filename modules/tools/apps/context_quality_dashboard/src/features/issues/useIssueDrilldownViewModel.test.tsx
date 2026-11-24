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
import { QualityOperationsProvider } from '../shell/QualityOperationsProvider';

import { useIssueDrilldownViewModel } from './useIssueDrilldownViewModel';

describe(useIssueDrilldownViewModel.name, () => {
  it('loads drilldown data when a metric is selected on the issues page', async () => {
    const services = createMockServices({ latency: 0 });
    const getDrilldown = vi.spyOn(services.qualityDashboardService, 'getDrilldown');

    const { result } = renderHook(() => useIssueDrilldownViewModel(), {
      wrapper: createWrapper(
        services,
        JSON.stringify({ page: 'issues', dimensionId: 'files', metricId: 'unlinkedByCategory' })
      ),
    });

    await waitFor(() => {
      expect(result.current.isDrilldownLoading).toBe(false);
    });

    expect(getDrilldown).toHaveBeenCalledWith('unlinkedByCategory', {});
    expect(result.current.drilldown).toBeDefined();
    expect(result.current.selectedMetricId).toBe('unlinkedByCategory');
  });

  it('does not expose a selected metric when the issues drilldown is closed', async () => {
    const { result } = renderHook(() => useIssueDrilldownViewModel(), {
      wrapper: createWrapper(createMockServices({ latency: 0 }), JSON.stringify({ page: 'issues' })),
    });

    await waitFor(() => {
      expect(result.current.selectedMetricId).toBeUndefined();
    });
  });
});

function createWrapper(
  services = createMockServices({ latency: 0 }),
  hostInitialState: string
): ComponentType<{ children: ReactNode }> {
  const queryClient = createTestQueryClient();
  return ({ children }) => (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>
        <HostApiProvider api={null}>
          <LanguageProvider>
            <QualityOperationsProvider>
              <AppStateProvider hostInitialState={hostInitialState}>{children}</AppStateProvider>
            </QualityOperationsProvider>
          </LanguageProvider>
        </HostApiProvider>
      </ServicesProvider>
    </QueryClientProvider>
  );
}
