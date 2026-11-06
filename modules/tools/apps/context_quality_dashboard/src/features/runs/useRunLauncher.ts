import { useContext } from 'react';

import { RunLauncherContext, type RunLauncher } from './runLauncherContext';

/** The launcher, or null where no run can be started (no workflow is deployed, or no provider is mounted). */
export function useRunLauncher(): RunLauncher | null {
  return useContext(RunLauncherContext);
}
