import type { HostAppAPI } from '@cognite/app-sdk';
import { describe, expect, it } from 'vitest';

import { BackendError } from './backendErrors';
import { HttpRunService } from './HttpRunService';

const INPUT = {
  extraLinks: [{ model: 'cdf_cdm:CogniteCore:v1', links: ['CogniteAsset.type'] }],
  scope: { dimensions: ['files'] as const, configured: true },
};

type Call = { url: string; method: string; headers: Record<string, string>; body: unknown };

describe(HttpRunService.name, () => {
  it('opens a session as the signed-in user, then starts the workflow bound to it with the input', async () => {
    const { service, calls } = setup([
      json({ items: [{ id: 1, nonce: 'nonce-1', status: 'READY' }] }),
      json({ id: 'exec-1', status: 'RUNNING' }),
    ]);

    await expect(service.start(INPUT)).resolves.toEqual({ executionId: 'exec-1' });

    expect(calls[0]).toMatchObject({
      url: 'https://api.test/api/v1/projects/proj/sessions',
      method: 'POST',
      headers: { Authorization: 'Bearer tok' },
      body: { items: [{ tokenExchange: true }] },
    });
    expect(calls[1]).toMatchObject({
      url: 'https://api.test/api/v1/projects/proj/workflows/cq-wf/versions/v3/run',
      method: 'POST',
      headers: { Authorization: 'Bearer tok' },
      body: { authentication: { nonce: 'nonce-1' }, input: INPUT },
    });
  });

  it('encodes the workflow name and version in the path', async () => {
    const { service, calls } = setup(
      [json({ items: [{ nonce: 'n' }] }), json({ id: 'e' })],
      { externalId: 'a b', version: 'v/1' }
    );

    await service.start(INPUT);

    expect(calls[1]?.url).toBe('https://api.test/api/v1/projects/proj/workflows/a%20b/versions/v%2F1/run');
  });

  it.each([
    ['RUNNING', 'running'],
    ['PENDING', 'running'],
    ['COMPLETED', 'completed'],
    ['FAILED', 'failed'],
    ['TERMINATED', 'failed'],
    ['TIMED_OUT', 'failed'],
  ])('reads the execution status %s as %s', async (status, expected) => {
    const { service, calls } = setup([json({ id: 'exec-1', status })]);

    await expect(service.getStatus('exec-1')).resolves.toBe(expected);

    expect(calls[0]).toMatchObject({
      url: 'https://api.test/api/v1/projects/proj/workflows/executions/exec-1',
      method: 'GET',
    });
  });

  it('starts a run for some dimensions only by sending the scope in the input', async () => {
    const { service, calls } = setup([json({ items: [{ nonce: 'n' }] }), json({ id: 'e' })]);

    await service.start({ extraLinks: [], scope: { dimensions: ['asset-hierarchy'], configured: false } });

    expect(calls[1]?.body).toMatchObject({
      input: { extraLinks: [], scope: { dimensions: ['asset-hierarchy'], configured: false } },
    });
  });

  it.each([
    ['a run is running', [{ status: 'COMPLETED' }, { status: 'RUNNING' }], true],
    ['a run is waiting to start', [{ status: 'PENDING' }], true],
    ['every run is over', [{ status: 'COMPLETED' }, { status: 'FAILED' }], false],
    ['there is no run', [], false],
  ])('says whether a run is going on: %s', async (_name, items, expected) => {
    const { service, calls } = setup([json({ items })]);

    await expect(service.findRunning()).resolves.toBe(expected);

    expect(calls[0]).toMatchObject({
      url: 'https://api.test/api/v1/projects/proj/workflows/executions/list',
      method: 'POST',
      body: { filter: { workflowFilters: [{ externalId: 'cq-wf', version: 'v3' }] }, limit: 25 },
    });
  });

  it('does not refuse a run because it could not look for one in progress', async () => {
    const refused = setup([json({ error: { message: 'no' } }, 403)]);
    const offline = new HttpRunService({
      getHostApi: host,
      fetchFn: () => Promise.reject(new Error('offline')),
      workflow: { externalId: 'cq-wf', version: 'v3' },
    });

    await expect(refused.service.findRunning()).resolves.toBe(false);
    await expect(offline.findRunning()).resolves.toBe(false);
  });

  it('fails clearly on a status it does not know instead of guessing', async () => {
    const { service } = setup([json({ id: 'exec-1', status: 'SOMETHING_NEW' })]);

    await expect(service.getStatus('exec-1')).rejects.toMatchObject({ kind: 'request-failed' });
  });

  it('says the user lacks permission when the session or the run is refused with 403', async () => {
    const forbidden = setup([json({ error: { message: 'missing acl' } }, 403)]);
    const refusedRun = setup([json({ items: [{ nonce: 'n' }] }), json({ error: { message: 'nope' } }, 403)]);

    await expect(forbidden.service.start(INPUT)).rejects.toMatchObject({ kind: 'permission-denied', status: 403 });
    await expect(refusedRun.service.start(INPUT)).rejects.toMatchObject({ kind: 'permission-denied', status: 403 });
  });

  it('reports an expired sign-in and any other failure as such', async () => {
    const expired = setup([json({}, 401)]);
    const broken = setup([json({ items: [{ nonce: 'n' }] }), json({ error: { message: 'boom' } }, 500)]);

    await expect(expired.service.start(INPUT)).rejects.toMatchObject({ kind: 'unauthenticated' });
    await expect(broken.service.start(INPUT)).rejects.toMatchObject({ kind: 'request-failed', status: 500 });
  });

  it('does not start the workflow when no session nonce came back', async () => {
    const { service, calls } = setup([json({ items: [] })]);

    await expect(service.start(INPUT)).rejects.toBeInstanceOf(BackendError);
    expect(calls).toHaveLength(1);
  });

  it('reports a request that never reached the API as a network failure', async () => {
    const service = new HttpRunService({
      getHostApi: host,
      fetchFn: () => Promise.reject(new Error('offline')),
      workflow: { externalId: 'cq-wf', version: 'v3' },
    });

    await expect(service.start(INPUT)).rejects.toMatchObject({ kind: 'network' });
  });

  it('needs the Fusion host', async () => {
    const service = new HttpRunService({
      getHostApi: () => null,
      fetchFn: () => Promise.reject(new Error('unused')),
      workflow: { externalId: 'cq-wf', version: 'v3' },
    });

    await expect(service.start(INPUT)).rejects.toMatchObject({ kind: 'host-unavailable' });
  });
});

function host(): HostAppAPI {
  return {
    getBaseUrl: () => Promise.resolve('https://api.test'),
    getAccessToken: () => Promise.resolve('tok'),
    getProject: () => Promise.resolve('proj'),
  } as Partial<HostAppAPI> as HostAppAPI;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
}

function setup(responses: Response[], workflow = { externalId: 'cq-wf', version: 'v3' }) {
  const calls: Call[] = [];
  const queue = [...responses];
  const fetchFn: typeof fetch = (input, init) => {
    const headers = new Headers(init?.headers);
    calls.push({
      url: String(input),
      method: init?.method ?? 'GET',
      headers: { Authorization: headers.get('Authorization') ?? '' },
      body: typeof init?.body === 'string' ? JSON.parse(init.body) : undefined,
    });
    const next = queue.shift();
    return next === undefined ? Promise.reject(new Error('unexpected request')) : Promise.resolve(next);
  };
  return { service: new HttpRunService({ getHostApi: host, fetchFn, workflow }), calls };
}
