import { QueryClientProvider } from '@tanstack/react-query';
import { render, type RenderOptions, type RenderResult } from '@testing-library/react';
import type { ReactElement, ReactNode } from 'react';

import type { AppServices } from '../services/createServices';
import { createMockServices } from '../services/mock/createMockServices';
import { ServicesProvider } from '../services/ServicesContext';

import { createTestQueryClient } from './renderWithQueryClient';

export function renderApp(
  ui: ReactElement,
  options: Omit<RenderOptions, 'wrapper'> & { services?: AppServices } = {}
): RenderResult {
  const { services = createMockServices({ latency: 0 }), ...renderOptions } = options;
  const queryClient = createTestQueryClient();

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>{children}</ServicesProvider>
    </QueryClientProvider>
  );

  return render(ui, { wrapper, ...renderOptions });
}
