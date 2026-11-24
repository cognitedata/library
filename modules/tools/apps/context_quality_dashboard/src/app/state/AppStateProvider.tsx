import { useEffect, useMemo, useReducer, type ReactNode } from 'react';

import { syncInternalStateSafe } from '../syncInternalStateSafe';
import { useHostApi } from '../useHostApi';

import { AppStateContext } from './appStateReactContext';
import { appStateReducer } from './appStateReducer';
import { parseInitialState } from './parseInitialState';

type AppStateProviderProps = {
  hostInitialState?: string;
  children: ReactNode;
};

export function AppStateProvider({ hostInitialState, children }: AppStateProviderProps) {
  const hostApi = useHostApi();
  const [state, dispatch] = useReducer(
    appStateReducer,
    hostInitialState,
    (initialStateFromHost?: string) => parseInitialState(initialStateFromHost)
  );

  useEffect(() => {
    void syncInternalStateSafe(hostApi, JSON.stringify(state));
  }, [hostApi, state]);

  const value = useMemo(
    () => ({
      state,
      dispatch,
    }),
    [state]
  );

  return <AppStateContext.Provider value={value}>{children}</AppStateContext.Provider>;
}
