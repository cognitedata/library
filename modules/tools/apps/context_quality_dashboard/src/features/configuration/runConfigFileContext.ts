import { createContext } from 'react';

import { runConfigFileStore, type RunConfigFileStore } from './runConfigFileStore';

export type RunConfigFileContextValue = {
  store: RunConfigFileStore;
  /** How often a started run is asked how it is going. */
  pollMs?: number;
};

export const DEFAULT_POLL_MS = 3000;

/** Where the uploaded config file is kept. Tests supply their own store and a short poll interval. */
export const RunConfigFileContext = createContext<RunConfigFileContextValue>({ store: runConfigFileStore });
