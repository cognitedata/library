import type { DimensionId } from '../types';

export type RunExecutionStatus = 'running' | 'completed' | 'failed';

/** What a run measures: some dimensions or all of them, and the metrics of the config file or not. */
export type RunScopeInput = {
  dimensions: readonly DimensionId[] | 'all';
  configured: boolean;
};

/** What a run is started with: the config file's links, and which part of the measurements it is for. */
export type RunInput = {
  extraLinks: readonly { model: string; links: readonly string[] }[];
  scope: RunScopeInput;
};

/** Starts a metrics run and reports how it is going. A run executes as the signed-in user. */
export interface RunService {
  start(input: RunInput): Promise<{ executionId: string }>;
  getStatus(executionId: string): Promise<RunExecutionStatus>;
  /**
   * Whether a run is going on now, whoever started it (the schedule, another person). The workflow runs one at a time, so
   * a second start would wait behind it. False when it cannot be told: the workflow then queues the run itself.
   */
  findRunning(): Promise<boolean>;
}
