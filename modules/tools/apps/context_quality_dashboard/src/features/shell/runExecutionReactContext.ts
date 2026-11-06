import { createContext } from 'react';

import type { RunConfigModel } from '../configuration/runConfigFile';
import type { RunPhase } from '../configuration/useRunConfigFileViewModel';

export type RunExecutionStartError = {
  key: string;
  message: string;
};

export type RunExecutionContextValue = {
  phase: RunPhase;
  startError: RunExecutionStartError | null;
  configTerminalDismissed: boolean;
  startRun: (models: readonly RunConfigModel[]) => void;
  dismissConfigTerminal: () => void;
};

export const RunExecutionContext = createContext<RunExecutionContextValue | null>(null);
