import { createContext } from 'react';

import type { RunConfigModel } from '../configuration/runConfigFile';

import type { RunRequest } from './runRequest';

/** Where a run started from the dashboard is: nothing yet, starting, in progress, or over. */
export type RunPhase = 'idle' | 'starting' | 'running' | 'completed' | 'failed' | 'not-started';

export type RunLauncher = {
  phase: RunPhase;
  /** What the run started from here is for; null before one was started. */
  request: RunRequest | null;
  /** Why the run could not be started, as a translation key plus the message the API gave. */
  startError: { key: string; message: string } | null;
  /** A run this dashboard did not start (the schedule, another person) is going on. */
  otherRunInProgress: boolean;
  /** No run can be started now: one started here is starting or going on, or one started elsewhere is. */
  busy: boolean;
  start: (request: RunRequest, models: readonly RunConfigModel[] | null) => void;
};

/** One launcher for the whole dashboard, so that a run started on one page is known on every other. */
export const RunLauncherContext = createContext<RunLauncher | null>(null);
