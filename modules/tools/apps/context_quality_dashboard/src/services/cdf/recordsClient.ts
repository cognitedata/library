import type { HostAppAPI } from '@cognite/app-sdk';

import { classifyFailure, networkFailure } from './backendErrors';
import { defaultSleep, fetchWithBackoff } from './fetchWithBackoff';
import { resolveHost } from './hostRequest';
import { FALLBACK_WINDOW_DAYS, type StreamConfig } from './streamConfig';

export type PropertyBag = Readonly<Record<string, unknown>>;

export type RecordsQuery = {
  /** Exact-match filters on container properties, combined with AND. */
  equals: Readonly<Record<string, string | number>>;
  /** Properties the row must not have. A row that never wrote a property (for example an untiered row's `tier`) matches. */
  absent?: readonly string[];
  /** At most this many rows (default and maximum: the API page limit). A page that reaches it is `truncated`. */
  limit?: number;
};

export type RecordsPage = {
  rows: readonly PropertyBag[];
  /** True when the response hit the API page limit, so rows may be missing. */
  truncated: boolean;
};

export interface RecordsClient {
  query(query: RecordsQuery): Promise<RecordsPage>;
}

export const RECORDS_PAGE_LIMIT = 1000;

const DAY_MS = 86_400_000;
const CLOCK_SKEW_MS = 5 * 60_000;

type HttpRecordsClientDeps = {
  getHostApi: () => HostAppAPI | null;
  fetchFn: typeof fetch;
  config: StreamConfig;
  now: () => number;
  /**
   * How many days a query may span. Production passes StreamPreflight.windowDays, which reads the deployed
   * stream's filter limit; without it the configured window (or the 7-day minimum) is used.
   */
  windowDays?: () => Promise<number>;
  /** Injected so retries can be tested without waiting. */
  sleep?: (ms: number) => Promise<void>;
};

/** The records filter endpoint allows 15 requests per second per user; stay well under it. */
const MAX_CONCURRENT_REQUESTS = 4;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toIso(ms: number): string {
  return new Date(ms).toISOString();
}

export class HttpRecordsClient implements RecordsClient {
  private readonly deps: HttpRecordsClientDeps;
  private inFlight = 0;
  private readonly waiting: (() => void)[] = [];

  constructor(deps: HttpRecordsClientDeps) {
    this.deps = deps;
  }

  private async acquire(): Promise<void> {
    if (this.inFlight < MAX_CONCURRENT_REQUESTS) {
      this.inFlight += 1;
      return;
    }
    await new Promise<void>((resolve) => {
      this.waiting.push(resolve);
    });
  }

  private release(): void {
    const next = this.waiting.shift();
    if (next === undefined) {
      this.inFlight -= 1;
    } else {
      next();
    }
  }

  /** POST with a concurrency cap, retrying 429s with jittered backoff (honouring Retry-After). */
  private async post(url: string, init: RequestInit): Promise<Response> {
    const sleep = this.deps.sleep ?? defaultSleep;
    await this.acquire();
    try {
      try {
        return await fetchWithBackoff(this.deps.fetchFn, url, init, sleep);
      } catch (cause) {
        throw networkFailure('Records query', cause);
      }
    } finally {
      this.release();
    }
  }

  async query(query: RecordsQuery): Promise<RecordsPage> {
    const { getHostApi, config, now } = this.deps;
    const { baseUrl, token, project } = await resolveHost(getHostApi);
    const windowDays = await (this.deps.windowDays?.() ?? Promise.resolve(config.windowDays ?? FALLBACK_WINDOW_DAYS));

    const propertyPath = (property: string) => [config.schemaSpace, config.relationshipContainer, property];
    const pageLimit = Math.min(query.limit ?? RECORDS_PAGE_LIMIT, RECORDS_PAGE_LIMIT);
    const filters = [
      ...Object.entries(query.equals).map(([property, value]) => ({
        equals: { property: propertyPath(property), value },
      })),
      ...(query.absent ?? []).map((property) => ({ not: { exists: { property: propertyPath(property) } } })),
    ];
    // The API rejects an interval longer than the stream's maximum, so the window ends slightly in the
    // future (clock skew) and spans exactly the resolved number of days.
    const upperMs = now() + CLOCK_SKEW_MS;
    const body = {
      lastUpdatedTime: {
        gt: toIso(upperMs - windowDays * DAY_MS),
        lt: toIso(upperMs),
      },
      sources: [
        {
          source: {
            type: 'container',
            space: config.schemaSpace,
            externalId: config.relationshipContainer,
          },
        },
      ],
      filter: { and: filters },
      limit: pageLimit,
    };

    const url = `${baseUrl}/api/v1/projects/${encodeURIComponent(project)}/streams/${encodeURIComponent(config.streamId)}/records/filter`;
    const response = await this.post(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      throw classifyFailure(response.status, await response.text(), {
        streamId: config.streamId,
        schemaSpace: config.schemaSpace,
        action: 'Records query',
      });
    }

    const json: unknown = await response.json();
    const items = isRecord(json) && Array.isArray(json.items) ? json.items : [];
    const rows: PropertyBag[] = [];
    for (const item of items) {
      if (!isRecord(item) || !isRecord(item.properties)) {
        continue;
      }
      const space = item.properties[config.schemaSpace];
      const bag = isRecord(space) ? space[config.relationshipContainer] : undefined;
      if (isRecord(bag)) {
        rows.push(bag);
      }
    }
    return { rows, truncated: items.length >= pageLimit };
  }
}
