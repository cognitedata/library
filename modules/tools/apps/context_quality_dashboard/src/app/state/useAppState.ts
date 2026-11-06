import { useContext } from 'react';

import { AppStateContext } from './appStateReactContext';
import type { AppStateContextValue } from './types';

export function useAppState(): AppStateContextValue {
  const context = useContext(AppStateContext);
  if (context === null) {
    throw new Error('useAppState must be used within AppStateProvider');
  }
  return context;
}
