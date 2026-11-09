import type { HostAppAPI } from '@cognite/app-sdk';
import { createContext } from 'react';

export const HostApiContext = createContext<HostAppAPI | null>(null);
