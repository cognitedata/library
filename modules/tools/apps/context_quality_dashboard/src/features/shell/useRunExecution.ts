import { useContext } from 'react';

import { RunExecutionContext, type RunExecutionContextValue } from './runExecutionReactContext';

export function useRunExecution(): RunExecutionContextValue {
  const context = useContext(RunExecutionContext);
  if (context === null) {
    throw new Error('useRunExecution must be used within RunExecutionProvider');
  }
  return context;
}

export type { RunExecutionContextValue, RunExecutionStartError } from './runExecutionReactContext';
