import { createContext } from 'react';

import type { AppStateContextValue } from './types';

export const AppStateContext = createContext<AppStateContextValue | null>(null);
