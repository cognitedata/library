import { QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import type { ComponentType, ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { HostApiProvider } from '../../app/HostApiContext';
import { AppStateProvider } from '../../app/state/AppStateProvider';
import { LanguageProvider } from '../../i18n/LanguageProvider';
import { createMockServices } from '../../services/mock/createMockServices';
import { ServicesProvider } from '../../services/ServicesContext';
import { makeMockHostApi } from '../../test/mockHostApi';
import { createTestQueryClient } from '../../test/renderWithQueryClient';

import { useAppShellViewModel } from './useAppShellViewModel';

describe(useAppShellViewModel.name, () => {
  it('exposes the active page and translated labels from app state', async () => {
    const { result } = renderHook(() => useAppShellViewModel(), {
      wrapper: createWrapper(makeMockHostApi()),
    });

    await waitFor(() => {
      expect(result.current.page).toBe('overview');
    });

    expect(result.current.pageLabel).toBe('Overview');
    expect(result.current.t('app.shortTitle')).toBe('Contextualization Quality');
  });
});

function createWrapper(api: ReturnType<typeof makeMockHostApi> | null): ComponentType<{ children: ReactNode }> {
  const queryClient = createTestQueryClient();
  const services = createMockServices({ latency: 0 });

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
