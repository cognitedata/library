import type { HostAppAPI } from '@cognite/app-sdk';
import { useContext } from 'react';

import { HostApiContext } from './hostApiReactContext';

export function useHostApi(): HostAppAPI | null {
  return useContext(HostApiContext);
}
