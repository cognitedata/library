import type { HostAppAPI } from '@cognite/app-sdk';

import type { ExportService } from '../ExportService';

import { apiMessage, BackendError, networkFailure } from './backendErrors';
import { fetchWithBackoff } from './fetchWithBackoff';
import { resolveHost } from './hostRequest';

type HttpExportServiceDeps = {
  getHostApi: () => HostAppAPI | null;
  fetchFn: typeof fetch;
  /** The id of the latest completed run: the workbook is named after it. */
  latestRunId: () => Promise<string>;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function workbookExternalId(runId: string): string {
  return `cq-unlinked-${runId}.xlsx`;
}

/**
 * Asks Files for a download link to the workbook the latest run wrote. Reads only: the link is a signed URL for a file
 * that already exists, and nothing is created, changed or deleted.
 */
export class HttpExportService implements ExportService {
  private readonly deps: HttpExportServiceDeps;

  constructor(deps: HttpExportServiceDeps) {
    this.deps = deps;
  }

  async getUnlinkedWorkbookUrl(): Promise<string | null> {
    const runId = await this.deps.latestRunId();
    const { baseUrl, token, project } = await resolveHost(this.deps.getHostApi);
    const action = 'Requesting the download link';
    let response: Response;
    try {
      response = await fetchWithBackoff(this.deps.fetchFn, `${baseUrl}/api/v1/projects/${encodeURIComponent(project)}/files/downloadlink`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: [{ externalId: workbookExternalId(runId) }] }),
      });
    } catch (cause) {
      throw networkFailure(action, cause);
    }
    if (response.ok) {
      const body: unknown = await response.json();
      const first = isRecord(body) && Array.isArray(body.items) ? body.items[0] : undefined;
      return isRecord(first) && typeof first.downloadUrl === 'string' ? first.downloadUrl : null;
    }
    const text = await response.text();
    if (response.status === 400 && /missing/i.test(text)) {
      return null; // the run wrote no workbook (older run, export skipped or failed)
    }
    const message = apiMessage(text);
    const summary = `${action} failed (${response.status})${message === '' ? '' : `: ${message.slice(0, 300)}`}`;
    if (response.status === 401) {
      throw new BackendError('unauthenticated', summary, { status: response.status });
    }
    if (response.status === 403) {
      throw new BackendError('permission-denied', summary, { status: response.status, subject: 'files:read' });
    }
    throw new BackendError('request-failed', summary, { status: response.status });
  }
}
