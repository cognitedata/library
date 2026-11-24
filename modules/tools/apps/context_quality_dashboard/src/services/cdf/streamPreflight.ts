import type { HostAppAPI } from '@cognite/app-sdk';

import { classifyFailure } from './backendErrors';
import { fetchWithBackoff } from './fetchWithBackoff';
import { resolveHost } from './hostRequest';
import { FALLBACK_WINDOW_DAYS, MAX_WINDOW_DAYS, type StreamConfig } from './streamConfig';

type StreamPreflightDeps = {
  getHostApi: () => HostAppAPI | null;
  fetchFn: typeof fetch;
  config: StreamConfig;
};

const DURATION = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?$/;

/** Whole days in an ISO-8601 duration such as `PT168H` or `P7D`. Years and months are not supported. */
export function parseIsoDurationDays(value: string): number | null {
  const match = DURATION.exec(value);
  if (match === null || value === 'P' || value.endsWith('T')) {
    return null;
  }
  const [, days, hours, minutes] = match;
  const totalHours = Number(days ?? 0) * 24 + Number(hours ?? 0) + Number(minutes ?? 0) / 60;
  const wholeDays = Math.floor(totalHours / 24);
  return wholeDays >= 1 ? wholeDays : null;
}

function readMaxFilteringInterval(json: unknown): string | null {
  if (typeof json !== 'object' || json === null || !('settings' in json)) {
    return null;
  }
  const { settings } = json;
  if (typeof settings !== 'object' || settings === null || !('limits' in settings)) {
    return null;
  }
  const { limits } = settings;
  if (typeof limits !== 'object' || limits === null || !('maxFilteringInterval' in limits)) {
    return null;
  }
  return typeof limits.maxFilteringInterval === 'string' ? limits.maxFilteringInterval : null;
}

/**
 * One read-only look at the configured stream, made once per session. It answers two things the Records
 * filter call cannot: how far back the deployed stream lets a query reach, and (with a clearer message)
 * whether the stream exists. It never creates or changes anything.
 *
 * A user without permission to read stream settings (403) is not blocked: the dashboard then uses the
 * configured window and lets the Records call report any problem.
 */
export class StreamPreflight {
  private readonly deps: StreamPreflightDeps;
  private cached: Promise<number> | null = null;

  constructor(deps: StreamPreflightDeps) {
    this.deps = deps;
  }

  /** How many days of history a query may span. */
  windowDays(): Promise<number> {
    if (this.cached !== null) {
      return this.cached;
    }
    const pending = this.resolve();
    this.cached = pending;
    pending.catch(() => {
      if (this.cached === pending) {
        this.cached = null;
      }
    });
    return pending;
  }

  private async resolve(): Promise<number> {
    const { config, getHostApi, fetchFn } = this.deps;
    const unreadable = config.windowDays ?? FALLBACK_WINDOW_DAYS;
    const { baseUrl, token, project } = await resolveHost(getHostApi);

    let response: Response;
    try {
      response = await fetchWithBackoff(
        fetchFn,
        `${baseUrl}/api/v1/projects/${encodeURIComponent(project)}/streams/${encodeURIComponent(config.streamId)}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
    } catch {
      return unreadable;
    }

    if (!response.ok) {
      const failure = classifyFailure(response.status, await response.text(), {
        streamId: config.streamId,
        schemaSpace: config.schemaSpace,
        action: 'Stream lookup',
      });
      if (failure.kind === 'unauthenticated' || failure.kind === 'stream-missing') {
        throw failure;
      }
      return unreadable;
    }

    const interval = readMaxFilteringInterval(await response.json().catch(() => null));
    const limitDays = interval === null ? null : parseIsoDurationDays(interval);
    if (limitDays === null) {
      return unreadable;
    }
    return Math.min(config.windowDays ?? MAX_WINDOW_DAYS, MAX_WINDOW_DAYS, limitDays);
  }
}
