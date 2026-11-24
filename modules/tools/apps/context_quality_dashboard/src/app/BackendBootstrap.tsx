import { useEffect, useState, type ReactNode } from 'react';

import { BackendConfigErrorScreen } from '../features/backend/BackendConfigErrorScreen';
import { BackendErrorPanel } from '../features/backend/BackendErrorPanel';
import { LanguageProvider } from '../i18n/LanguageProvider';
import type { BackendConfigResult } from '../services/cdf/streamConfig';
import { loadServices, type AppServices } from '../services/createServices';
import { ServicesProvider } from '../services/ServicesContext';

import { LoadingFallback } from './LoadingFallback';

type BackendBootstrapProps = {
  config: BackendConfigResult;
  /** Injected so the loading path can be tested. */
  load?: typeof loadServices;
  children: ReactNode;
};

type Bootstrap =
  | { state: 'loading' }
  | { state: 'ready'; services: AppServices }
  | { state: 'failed'; error: unknown };

/**
 * Builds the services from the deployment configuration, then renders the app. An invalid configuration shows
 * what to set instead of the app; nothing is read, created or replaced with demo data. The built-in sample data
 * is only fetched when the deployment explicitly asks for it.
 */
export function BackendBootstrap({ config, load = loadServices, children }: BackendBootstrapProps) {
  const [bootstrap, setBootstrap] = useState<Bootstrap>({ state: 'loading' });

  useEffect(() => {
    if (!config.ok) {
      return;
    }
    let cancelled = false;
    load(config.config)
      .then((services) => {
        if (!cancelled) {
          setBootstrap({ state: 'ready', services });
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setBootstrap({ state: 'failed', error });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [config, load]);

  if (!config.ok) {
    return (
      <LanguageProvider>
        <BackendConfigErrorScreen issues={config.issues} />
      </LanguageProvider>
    );
  }
  if (bootstrap.state === 'failed') {
    return (
      <LanguageProvider>
        <BackendErrorPanel error={bootstrap.error} />
      </LanguageProvider>
    );
  }
  if (bootstrap.state === 'loading') {
    return <LoadingFallback step="prepare-services" config={config.config} />;
  }
  return <ServicesProvider services={bootstrap.services}>{children}</ServicesProvider>;
}
