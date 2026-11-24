import type { HostAppAPI } from '@cognite/app-sdk';
import { describe, expect, it, vi } from 'vitest';

import { isBackendError } from './backendErrors';
import { HttpExportService, workbookExternalId } from './HttpExportService';

const host = {
  getBaseUrl: async () => 'https://cdf.example.test',
  getAccessToken: async () => 'token-1',
  getProject: async () => 'demo',
} as Partial<HostAppAPI> as HostAppAPI;

function service(fetchFn: typeof fetch) {
  return new HttpExportService({ getHostApi: () => host, fetchFn, latestRunId: async () => 'r1' });
}

describe(HttpExportService.name, () => {
  it('names the workbook after the run', () => {
    expect(workbookExternalId('r1')).toBe('cq-unlinked-r1.xlsx');
  });

  it('asks Files for a link to the latest run workbook and returns it', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [{ downloadUrl: 'https://dl/x' }] }));

    await expect(service(fetchFn).getUnlinkedWorkbookUrl()).resolves.toBe('https://dl/x');

    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toBe('https://cdf.example.test/api/v1/projects/demo/files/downloadlink');
    expect(JSON.parse(String(init?.body))).toEqual({ items: [{ externalId: 'cq-unlinked-r1.xlsx' }] });
  });

  it('answers null when the run wrote no workbook', async () => {
    const fetchFn = vi.fn<typeof fetch>(
      async () => new Response(JSON.stringify({ error: { message: 'Files missing' } }), { status: 400 })
    );

    await expect(service(fetchFn).getUnlinkedWorkbookUrl()).resolves.toBeNull();
  });

  it('tells a missing permission from other failures', async () => {
    const denied = vi.fn<typeof fetch>(async () => new Response('{}', { status: 403 }));
    const broken = vi.fn<typeof fetch>(async () => new Response('{}', { status: 500 }));

    await expect(service(denied).getUnlinkedWorkbookUrl()).rejects.toSatisfy(
      (e) => isBackendError(e) && e.kind === 'permission-denied'
    );
    await expect(service(broken).getUnlinkedWorkbookUrl()).rejects.toSatisfy(
      (e) => isBackendError(e) && e.kind === 'request-failed'
    );
  });
});
