import { QueryClientProvider } from '@tanstack/react-query';
import { render, type RenderOptions, type RenderResult } from '@testing-library/react';
import type { ReactElement, ReactNode } from 'react';

import { HostApiProvider } from '../app/HostApiContext';
import { AppStateProvider } from '../app/state/AppStateProvider';
import { RunLauncherProvider } from '../features/runs/RunLauncherProvider';
import { QualityOperationsProvider } from '../features/shell/QualityOperationsProvider';
import { RunExecutionProvider } from '../features/shell/RunExecutionProvider';
import { LanguageProvider } from '../i18n/LanguageProvider';
import type { AppServices } from '../services/createServices';
import { createMockServices } from '../services/mock/createMockServices';
import { ServicesProvider } from '../services/ServicesContext';

import { createTestQueryClient } from './renderWithQueryClient';

type RenderOverviewOptions = Omit<RenderOptions, 'wrapper'> & {
  hostInitialState?: string;
  services?: AppServices;
};

export function renderOverview(ui: ReactElement, options: RenderOverviewOptions = {}): RenderResult {
  const { hostInitialState, services = createMockServices({ latency: 0 }), ...renderOptions } =
    options;
  const queryClient = createTestQueryClient();

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>
        <HostApiProvider api={null}>
          <LanguageProvider>
            <QualityOperationsProvider>
              <RunLauncherProvider runningCheckMs={5}>
                <RunExecutionProvider pollMs={5}>
                  <AppStateProvider hostInitialState={hostInitialState}>{children}</AppStateProvider>
                </RunExecutionProvider>
              </RunLauncherProvider>
            </QualityOperationsProvider>
          </LanguageProvider>
        </HostApiProvider>
      </ServicesProvider>
    </QueryClientProvider>
  );

  return render(ui, { wrapper, ...renderOptions });
}
