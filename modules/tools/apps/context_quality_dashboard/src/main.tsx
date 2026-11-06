import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { AppErrorBoundary } from './app/AppErrorBoundary';
import { BackendBootstrap } from './app/BackendBootstrap';
import { applyThemeMode } from './app/useThemeMode';
import App from './App.tsx';
import { resolveBackendConfig } from './services/cdf/streamConfig';
import './styles.css';

const storedTheme = localStorage.getItem('context-quality-dashboard.theme');
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
if (storedTheme === 'dark' || (!storedTheme && prefersDark)) {
  applyThemeMode('dark');
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,
      gcTime: 10 * 60 * 1000,
    },
  },
});

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AppErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <BackendBootstrap config={resolveBackendConfig()}>
          <App />
        </BackendBootstrap>
      </QueryClientProvider>
    </AppErrorBoundary>
  </StrictMode>
);
