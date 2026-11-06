import type { ReactNode } from 'react';

import type { AppServices } from './createServices';
import { ServicesContext } from './servicesContextValue';

type ServicesProviderProps = {
  services: AppServices;
  children: ReactNode;
};

export function ServicesProvider({ services, children }: ServicesProviderProps) {
  return <ServicesContext.Provider value={services}>{children}</ServicesContext.Provider>;
}
