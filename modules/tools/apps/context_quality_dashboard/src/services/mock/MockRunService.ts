import { BackendError } from '../cdf/backendErrors';
import type { RunExecutionStatus, RunInput, RunService } from '../RunService';

/** Demo runs: each one reports "running" once, then "completes". It computes nothing and changes no data. */
export class MockRunService implements RunService {
  private readonly polls = new Map<string, number>();

  async start(_input: RunInput): Promise<{ executionId: string }> {
    const executionId = `mock-execution-${String(this.polls.size + 1)}`;
    this.polls.set(executionId, 0);
    return { executionId };
  }

  async findRunning(): Promise<boolean> {
    return [...this.polls.values()].some((polls) => polls === 0);
  }

  async getStatus(executionId: string): Promise<RunExecutionStatus> {
    const polls = this.polls.get(executionId);
    if (polls === undefined) {
      throw new BackendError('request-failed', `The demo has no run ${executionId}.`);
    }
    this.polls.set(executionId, polls + 1);
    return polls === 0 ? 'running' : 'completed';
  }
}
