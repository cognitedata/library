import { QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import type { ComponentType, ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { AppStateProvider } from '../../app/state/AppStateProvider';
import { createMockServices } from '../../services/mock/createMockServices';
import { ServicesProvider } from '../../services/ServicesContext';
import { createTestQueryClient } from '../../test/renderWithQueryClient';

import { useScopedFilters } from './useScopedFilters';

function wrapperFor(filters: Record<string, string>): ComponentType<{ children: ReactNode }> {
  const queryClient = createTestQueryClient();
  const services = createMockServices({ latency: 0 });
  return ({ children }) => (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>
        <AppStateProvider hostInitialState={JSON.stringify({ page: 'overview', filters })}>{children}</AppStateProvider>
      </ServicesProvider>
    </QueryClientProvider>
  );
}

describe(useScopedFilters.name, () => {
  it('adds the facilities of the chosen location', async () => {
    const { result } = renderHook(() => useScopedFilters(), { wrapper: wrapperFor({ locationId: 'demo-north-central' }) });

    await waitFor(() => {
      expect(result.current.facilityIds).toEqual(['demo-north', 'demo-central']);
    });
    expect(result.current.locationId).toBe('demo-north-central');
  });

  it('covers no facility, rather than every one, while the locations are loading', () => {
    const { result } = renderHook(() => useScopedFilters(), { wrapper: wrapperFor({ locationId: 'demo-north-central' }) });

    expect(result.current.facilityIds).toEqual([]);
  });

  it('drops a location the run does not offer', async () => {
    const { result } = renderHook(() => useScopedFilters(), { wrapper: wrapperFor({ locationId: 'gone' }) });

    await waitFor(() => {
      expect(result.current.locationId).toBeUndefined();
    });
    expect(result.current.facilityIds).toBeUndefined();
  });

  it('leaves the filters alone when no location is chosen', () => {
    const { result } = renderHook(() => useScopedFilters(), { wrapper: wrapperFor({ facilityId: 'demo-north' }) });

    expect(result.current).toEqual({ facilityId: 'demo-north' });
  });
});
