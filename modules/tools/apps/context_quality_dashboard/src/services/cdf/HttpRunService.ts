import type { HostAppAPI } from '@cognite/app-sdk';

import type { RunExecutionStatus, RunInput, RunService } from '../RunService';

import { apiMessage, BackendError, networkFailure } from './backendErrors';
import { fetchWithBackoff } from './fetchWithBackoff';
import { resolveHost } from './hostRequest';
import type { WorkflowConfig } from './streamConfig';

type HttpRunServiceDeps = {
  getHostApi: () => HostAppAPI | null;
  fetchFn: typeof fetch;
  workflow: WorkflowConfig;
};

/** Execution states of a workflow (Workflows API) and what the dashboard makes of them. */
const STATUS: Readonly<Record<string, RunExecutionStatus>> = {
  PENDING: 'running',
  RUNNING: 'running',
  COMPLETED: 'completed',
  FAILED: 'failed',
  TERMINATED: 'failed',
  TIMED_OUT: 'failed',
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function failure(action: string, status: number, body: string): BackendError {
  const message = apiMessage(body);
  const summary = `${action} failed (${status})${message === '' ? '' : `: ${message.slice(0, 300)}`}`;
  if (status === 401) {
    return new BackendError('unauthenticated', summary, { status });
  }
  if (status === 403) {
    return new BackendError('permission-denied', summary, { status, subject: action });
  }
  return new BackendError('request-failed', summary, { status });
}

/**
 * Starts the deployed metrics workflow through the Workflows API, as the signed-in user: a session is opened from the
 * user's own token and the execution is bound to it, so the run can do only what that user is allowed to do.
 */
export class HttpRunService implements RunService {
  private readonly deps: HttpRunServiceDeps;

  constructor(deps: HttpRunServiceDeps) {
    this.deps = deps;
  }

  private async send(action: string, url: string, token: string, body?: unknown): Promise<unknown> {
    let response: Response;
    try {
      response = await fetchWithBackoff(this.deps.fetchFn, url, {
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        ...(body === undefined ? { method: 'GET' } : { method: 'POST', body: JSON.stringify(body) }),
      });
    } catch (cause) {
      throw networkFailure(action, cause);
    }
    if (!response.ok) {
      throw failure(action, response.status, await response.text());
    }
    const json: unknown = await response.json();
    return json;
  }

  async start(input: RunInput): Promise<{ executionId: string }> {
    const { baseUrl, token, project } = await resolveHost(this.deps.getHostApi);
    const root = `${baseUrl}/api/v1/projects/${encodeURIComponent(project)}`;
    const { externalId, version } = this.deps.workflow;

    const session = await this.send('Opening a session', `${root}/sessions`, token, {
      items: [{ tokenExchange: true }],
    });
    const first = isRecord(session) && Array.isArray(session.items) ? session.items[0] : undefined;
    const nonce = isRecord(first) && typeof first.nonce === 'string' ? first.nonce : null;
    if (nonce === null) {
      throw new BackendError('request-failed', 'Opening a session returned no nonce, so the run was not started.');
    }

    const execution = await this.send(
      'Starting the run',
      `${root}/workflows/${encodeURIComponent(externalId)}/versions/${encodeURIComponent(version)}/run`,
      token,
      { authentication: { nonce }, input }
    );
    if (!isRecord(execution) || typeof execution.id !== 'string') {
      throw new BackendError('request-failed', 'Starting the run returned no execution id.');
    }
    return { executionId: execution.id };
  }

  async findRunning(): Promise<boolean> {
    try {
      const { baseUrl, token, project } = await resolveHost(this.deps.getHostApi);
      const { externalId, version } = this.deps.workflow;
      const executions = await this.send(
        'Looking for a run in progress',
        `${baseUrl}/api/v1/projects/${encodeURIComponent(project)}/workflows/executions/list`,
        token,
        { filter: { workflowFilters: [{ externalId, version }] }, limit: 25 }
      );
      const items = isRecord(executions) && Array.isArray(executions.items) ? executions.items : [];
      return items.some(
        (item: unknown) =>
          isRecord(item) && typeof item.status === 'string' && STATUS[item.status.toUpperCase()] === 'running'
      );
    } catch {
      // Not being able to look is not a reason to refuse a run: the workflow itself runs one at a time.
      return false;
    }
  }

  async getStatus(executionId: string): Promise<RunExecutionStatus> {
    const { baseUrl, token, project } = await resolveHost(this.deps.getHostApi);
    const execution = await this.send(
      'Reading the run status',
      `${baseUrl}/api/v1/projects/${encodeURIComponent(project)}/workflows/executions/${encodeURIComponent(executionId)}`,
      token
    );
    const status = isRecord(execution) && typeof execution.status === 'string' ? STATUS[execution.status] : undefined;
    if (status === undefined) {
      const raw = isRecord(execution) ? String(execution.status) : 'none';
      throw new BackendError('request-failed', `The run has a status the dashboard does not know: ${raw}.`);
    }
    return status;
  }
}
