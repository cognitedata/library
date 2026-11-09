import type { HostAppAPI } from '@cognite/app-sdk';
import { describe, expect, it, vi } from 'vitest';

import { BackendError } from './backendErrors';
import { HttpRecordsClient, RECORDS_PAGE_LIMIT } from './recordsClient';
import type { StreamConfig } from './streamConfig';

const config: StreamConfig = {
  streamId: 'test_stream',
  schemaSpace: 'test_schema',
  relationshipContainer: 'ContextQualityRelationship',
  windowDays: 7,
};
const NOW = Date.parse('2026-10-12T07:00:00.000Z');

function hostApi(): HostAppAPI {
  return {
    getBaseUrl: async () => 'https://cdf.example.test',
    getAccessToken: async () => 'token-123',
    getProject: async () => 'demo project',
  } as Partial<HostAppAPI> as HostAppAPI;
}

function itemFor(props: Record<string, unknown>) {
  return { properties: { [config.schemaSpace]: { [config.relationshipContainer]: props } } };
}

const noSleep = async () => undefined;

function clientWith(
  fetchFn: typeof fetch,
  api: HostAppAPI | null = hostApi(),
  sleep: (ms: number) => Promise<void> = noSleep
) {
  return new HttpRecordsClient({ getHostApi: () => api, fetchFn, config, now: () => NOW, sleep });
}

describe(HttpRecordsClient.name, () => {
  it('posts a bounded filter with the bearer token and parses the property bags', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () =>
      Response.json({ items: [itemFor({ metricId: 'orphanRate' })] })
    );

    const page = await clientWith(fetchFn).query({ equals: { rowKind: 'aggregate', tier: 1 } });

    expect(page).toEqual({ rows: [{ metricId: 'orphanRate' }], truncated: false });
    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toBe(
      `https://cdf.example.test/api/v1/projects/demo%20project/streams/${config.streamId}/records/filter`
    );
    expect(init?.headers).toMatchObject({ Authorization: 'Bearer token-123' });
    const body: unknown = JSON.parse(String(init?.body));
    expect(body).toMatchObject({
      limit: RECORDS_PAGE_LIMIT,
      lastUpdatedTime: { gt: '2026-10-05T07:05:00.000Z', lt: '2026-10-12T07:05:00.000Z' },
      filter: {
        and: [
          { equals: { property: [config.schemaSpace, config.relationshipContainer, 'rowKind'], value: 'aggregate' } },
          { equals: { property: [config.schemaSpace, config.relationshipContainer, 'tier'], value: 1 } },
        ],
      },
    });
  });

  it('asks for rows that lack a property with a not-exists filter', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));

    await clientWith(fetchFn).query({ equals: { rowKind: 'aggregate' }, absent: ['tier'] });

    const body: unknown = JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body));
    expect(body).toMatchObject({
      filter: {
        and: [
          { equals: { property: [config.schemaSpace, config.relationshipContainer, 'rowKind'], value: 'aggregate' } },
          { not: { exists: { property: [config.schemaSpace, config.relationshipContainer, 'tier'] } } },
        ],
      },
    });
  });

  it('never asks for an interval longer than the configured window', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));

    await clientWith(fetchFn).query({ equals: {} });

    const body: unknown = JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body));
    const range = (body as { lastUpdatedTime: { gt: string; lt: string } }).lastUpdatedTime;
    expect(Date.parse(range.lt) - Date.parse(range.gt)).toBe(7 * 86_400_000);
  });

  it('marks a full page as truncated', async () => {
    const items = Array.from({ length: RECORDS_PAGE_LIMIT }, () => itemFor({ a: 1 }));
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items }));

    const page = await clientWith(fetchFn).query({ equals: {} });

    expect(page.truncated).toBe(true);
  });

  it('skips items that do not carry the expected container', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () =>
      Response.json({ items: [{ properties: {} }, itemFor({ a: 1 })] })
    );

    const page = await clientWith(fetchFn).query({ equals: {} });

    expect(page.rows).toEqual([{ a: 1 }]);
  });

  it('throws with the status when the API rejects the request', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => new Response('no records capability', { status: 403 }));

    await expect(clientWith(fetchFn).query({ equals: {} })).rejects.toThrow('403');
  });

  it('retries a rate-limited request after the Retry-After delay', async () => {
    const fetchFn = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response('slow down', { status: 429, headers: { 'Retry-After': '2' } }))
      .mockResolvedValueOnce(Response.json({ items: [itemFor({ a: 1 })] }));
    const sleeps: number[] = [];

    const page = await clientWith(fetchFn, hostApi(), async (ms) => {
      sleeps.push(ms);
    }).query({ equals: {} });

    expect(page.rows).toEqual([{ a: 1 }]);
    expect(sleeps).toEqual([2000]);
  });

  it('backs off exponentially, with jitter, and gives up after five attempts', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => new Response('slow down', { status: 429 }));
    const sleeps: number[] = [];

    await expect(
      clientWith(fetchFn, hostApi(), async (ms) => {
        sleeps.push(ms);
      }).query({ equals: {} })
    ).rejects.toThrow('429');

    expect(fetchFn).toHaveBeenCalledTimes(5);
    sleeps.forEach((slept, index) => {
      const ceiling = 300 * 2 ** index;
      expect(slept).toBeGreaterThanOrEqual(ceiling / 2);
      expect(slept).toBeLessThanOrEqual(ceiling);
    });
  });

  it('never runs more than four requests at once', async () => {
    let running = 0;
    let peak = 0;
    const fetchFn = vi.fn<typeof fetch>(async () => {
      running += 1;
      peak = Math.max(peak, running);
      await new Promise((resolve) => setTimeout(resolve, 5));
      running -= 1;
      return Response.json({ items: [] });
    });
    const client = clientWith(fetchFn);

    await Promise.all(Array.from({ length: 12 }, () => client.query({ equals: {} })));

    expect(fetchFn).toHaveBeenCalledTimes(12);
    expect(peak).toBe(4);
  });

  it('says the Fusion host is not connected, without calling the API', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));

    const failure = clientWith(fetchFn, null).query({ equals: {} });

    await expect(failure).rejects.toMatchObject({ kind: 'host-unavailable' });
    await expect(failure).rejects.toThrow('Fusion host is not connected');
    expect(fetchFn).not.toHaveBeenCalled();
  });

  describe('typed failures', () => {
    function failing(status: number, message: string) {
      return vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ error: { code: status, message } }), { status }));
    }

    it.each([
      [401, 'Unauthorized', 'unauthenticated'],
      [403, 'Forbidden', 'permission-denied'],
      [400, 'Stream test_stream not found', 'stream-missing'],
      [400, 'Unknown space: test_schema', 'schema-missing'],
      [400, 'Unknown property: test_schema.ContextQualityRelationship.rowKind', 'schema-missing'],
      [400, "lastUpdatedTime interval='44d' is to big. Max allowed interval='7d'", 'window-too-long'],
      [500, 'Internal error', 'request-failed'],
    ])('reports HTTP %i "%s" as %s', async (status, message, kind) => {
      await expect(clientWith(failing(status, message)).query({ equals: {} })).rejects.toMatchObject({ kind, status });
    });

    it('names the stream when access is denied', async () => {
      await expect(clientWith(failing(403, 'Forbidden')).query({ equals: {} })).rejects.toMatchObject({
        subject: 'test_stream',
      });
    });

    it('reports a request that never reached the API as a network failure, without retrying', async () => {
      const fetchFn = vi.fn<typeof fetch>(async () => {
        throw new TypeError('Failed to fetch');
      });

      await expect(clientWith(fetchFn).query({ equals: {} })).rejects.toMatchObject({ kind: 'network' });
      expect(fetchFn).toHaveBeenCalledTimes(1);
    });

    it('reports rate limiting that does not clear as a failed request', async () => {
      const fetchFn = vi.fn<typeof fetch>(async () => new Response('slow down', { status: 429 }));

      await expect(clientWith(fetchFn).query({ equals: {} })).rejects.toMatchObject({ kind: 'request-failed', status: 429 });
    });
  });

  describe('history window', () => {
    function spanDays(fetchFn: ReturnType<typeof vi.fn<typeof fetch>>): number {
      const body: unknown = JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body));
      const range = (body as { lastUpdatedTime: { gt: string; lt: string } }).lastUpdatedTime;
      return (Date.parse(range.lt) - Date.parse(range.gt)) / 86_400_000;
    }

    it('uses the window the deployed stream allows when one is provided', async () => {
      const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));
      const client = new HttpRecordsClient({ getHostApi: () => hostApi(), fetchFn, config, now: () => NOW, windowDays: async () => 3 });

      await client.query({ equals: {} });

      expect(spanDays(fetchFn)).toBe(3);
    });

    it('uses the configured window when none is provided', async () => {
      const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));
      const client = new HttpRecordsClient({
        getHostApi: () => hostApi(),
        fetchFn,
        config: { ...config, windowDays: 30 },
        now: () => NOW,
      });

      await client.query({ equals: {} });

      expect(spanDays(fetchFn)).toBe(30);
    });

    it('falls back to the 7-day minimum when nothing is configured or provided', async () => {
      const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));
      const client = new HttpRecordsClient({
        getHostApi: () => hostApi(),
        fetchFn,
        config: { ...config, windowDays: null },
        now: () => NOW,
      });

      await client.query({ equals: {} });

      expect(spanDays(fetchFn)).toBe(7);
    });

    it('does not query when the stream lookup says the stream is missing', async () => {
      const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ items: [] }));
      const client = new HttpRecordsClient({
        getHostApi: () => hostApi(),
        fetchFn,
        config,
        now: () => NOW,
        windowDays: async () => {
          throw new BackendError('stream-missing', 'missing');
        },
      });

      await expect(client.query({ equals: {} })).rejects.toMatchObject({ kind: 'stream-missing' });
      expect(fetchFn).not.toHaveBeenCalled();
    });
  });
});
