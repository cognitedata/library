import type { HostAppAPI } from '@cognite/app-sdk';
import { describe, expect, it, vi } from 'vitest';

import type { StreamConfig } from './streamConfig';
import { parseIsoDurationDays, StreamPreflight } from './streamPreflight';

const config: StreamConfig = {
  streamId: 'cq_stream',
  schemaSpace: 'cq_schema',
  relationshipContainer: 'ContextQualityRelationship',
  windowDays: null,
};

function hostApi(): HostAppAPI {
  return {
    getBaseUrl: async () => 'https://cdf.example.test',
    getAccessToken: async () => 'token-123',
    getProject: async () => 'demo project',
  } as Partial<HostAppAPI> as HostAppAPI;
}

function streamBody(maxFilteringInterval: string) {
  return { externalId: 'cq_stream', settings: { limits: { maxFilteringInterval } } };
}

function preflightWith(fetchFn: typeof fetch, overrides: Partial<StreamConfig> = {}, api: HostAppAPI | null = hostApi()) {
  return new StreamPreflight({ getHostApi: () => api, fetchFn, config: { ...config, ...overrides } });
}

describe(parseIsoDurationDays.name, () => {
  it.each([
    ['PT168H', 7],
    ['P7D', 7],
    ['P1DT12H', 1],
    ['PT8760H', 365],
    ['PT24H', 1],
  ])('reads %s as %i days', (value, days) => {
    expect(parseIsoDurationDays(value)).toBe(days);
  });

  it.each(['', 'P', 'PT', 'P1Y', 'P1M', 'PT12H', 'seven days', 'PT-5H'])('does not understand "%s"', (value) => {
    expect(parseIsoDurationDays(value)).toBeNull();
  });
});

describe(StreamPreflight.name, () => {
  it('reads the stream with the bearer token and never writes', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json(streamBody('PT168H')));

    await preflightWith(fetchFn).windowDays();

    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toBe('https://cdf.example.test/api/v1/projects/demo%20project/streams/cq_stream');
    expect(init?.headers).toMatchObject({ Authorization: 'Bearer token-123' });
    expect(init?.method).toBeUndefined();
    expect(init?.body).toBeUndefined();
  });

  it('limits the window to what the deployed stream allows', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json(streamBody('PT168H')));

    await expect(preflightWith(fetchFn).windowDays()).resolves.toBe(7);
  });

  it('reads as far back as the stream allows, up to the longest trend window', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json(streamBody('PT8760H')));

    await expect(preflightWith(fetchFn).windowDays()).resolves.toBe(90);
  });

  it('never exceeds the configured window', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json(streamBody('PT8760H')));

    await expect(preflightWith(fetchFn, { windowDays: 30 }).windowDays()).resolves.toBe(30);
  });

  it('is capped by the stream even when a longer window is configured', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json(streamBody('PT168H')));

    await expect(preflightWith(fetchFn, { windowDays: 60 }).windowDays()).resolves.toBe(7);
  });

  it('asks the API once and remembers the answer', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json(streamBody('PT168H')));
    const preflight = preflightWith(fetchFn);

    await Promise.all([preflight.windowDays(), preflight.windowDays()]);
    await preflight.windowDays();

    expect(fetchFn).toHaveBeenCalledTimes(1);
  });

  it('reports a stream that does not exist as missing', async () => {
    const fetchFn = vi.fn<typeof fetch>(
      async () => new Response('{"error":{"code":404,"message":"Stream with externalId cq_stream not found"}}', { status: 404 })
    );

    await expect(preflightWith(fetchFn).windowDays()).rejects.toMatchObject({ kind: 'stream-missing', subject: 'cq_stream' });
  });

  it('does not remember a failure, so fixing the deployment and retrying works', async () => {
    const fetchFn = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response('{"error":{"code":404,"message":"not found"}}', { status: 404 }))
      .mockResolvedValueOnce(Response.json(streamBody('PT168H')));
    const preflight = preflightWith(fetchFn);

    await expect(preflight.windowDays()).rejects.toMatchObject({ kind: 'stream-missing' });
    await expect(preflight.windowDays()).resolves.toBe(7);
  });

  it('reports an expired session', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => new Response('{"error":{"code":401,"message":"Unauthorized"}}', { status: 401 }));

    await expect(preflightWith(fetchFn).windowDays()).rejects.toMatchObject({ kind: 'unauthenticated' });
  });

  it('does not block a user who may read records but not stream settings', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => new Response('{"error":{"code":403,"message":"Forbidden"}}', { status: 403 }));

    await expect(preflightWith(fetchFn).windowDays()).resolves.toBe(7);
    await expect(preflightWith(fetchFn, { windowDays: 30 }).windowDays()).resolves.toBe(30);
  });

  it('leaves a network failure to the records call and uses the configured window', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => {
      throw new TypeError('Failed to fetch');
    });

    await expect(preflightWith(fetchFn, { windowDays: 14 }).windowDays()).resolves.toBe(14);
  });

  it('falls back to the safe minimum when the stream reports no usable limit', async () => {
    const fetchFn = vi.fn<typeof fetch>(async () => Response.json({ externalId: 'cq_stream' }));

    await expect(preflightWith(fetchFn).windowDays()).resolves.toBe(7);
  });

  it('says so when the Fusion host is not connected', async () => {
    const fetchFn = vi.fn<typeof fetch>();

    await expect(preflightWith(fetchFn, {}, null).windowDays()).rejects.toMatchObject({ kind: 'host-unavailable' });
    expect(fetchFn).not.toHaveBeenCalled();
  });
});
