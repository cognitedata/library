import type { HostAppAPI } from '@cognite/app-sdk';
import type { ReactNode } from 'react';

import { HostApiContext } from './hostApiReactContext';

type HostApiProviderProps = {
  api: HostAppAPI | null;
  children: ReactNode;
};

export function HostApiProvider({ api, children }: HostApiProviderProps) {
  return <HostApiContext.Provider value={api}>{children}</HostApiContext.Provider>;
}
