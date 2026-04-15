import { LRUCache } from "lru-cache";

type InstancesListParams = Record<string, unknown>;
type InstancesRetrieveSource = Record<string, unknown>;
type InstancesRetrieveParams = {
  items: Array<Record<string, unknown>>;
  sources?: InstancesRetrieveSource[];
};
type InstancesRetrieveResponse = { items?: Array<Record<string, unknown>> };

type InstancesSdk = {
  project: string;
  instances: {
    list: (params: unknown) => Promise<unknown>;
    retrieve: (params: unknown) => Promise<unknown>;
  };
};

export const INSTANCES_LIST_CACHE_MAX = 2000;
export const INSTANCES_LIST_CACHE_TTL_MS = 15 * 60 * 1000;
export const INSTANCES_BY_IDS_CACHE_MAX = 2000;
export const INSTANCES_BY_IDS_CACHE_TTL_MS = 15 * 60 * 1000;

const cache = new LRUCache<string, Record<string, unknown>>({
  max: INSTANCES_LIST_CACHE_MAX,
  ttl: INSTANCES_LIST_CACHE_TTL_MS,
});

let queue: Promise<unknown> = Promise.resolve(undefined);
const byIdsCache = new LRUCache<string, Record<string, unknown>>({
  max: INSTANCES_BY_IDS_CACHE_MAX,
  ttl: INSTANCES_BY_IDS_CACHE_TTL_MS,
});
let byIdsQueue: Promise<unknown> = Promise.resolve(undefined);

const enqueue = (task: () => Promise<unknown>) => {
  const next = queue.then(task, task);
  queue = next.then(
    () => undefined,
    () => undefined
  );
  return next;
};

export function cachedInstancesList(sdk: InstancesSdk, params: InstancesListParams) {
  const key = `${sdk.project}:${JSON.stringify(params)}`;
  const cached = cache.get(key);
  if (cached) {
    return Promise.resolve(cached);
  }
  return enqueue(async () => {
    const cachedInside = cache.get(key);
    if (cachedInside) return cachedInside;
    const response = await sdk.instances.list(params as never);
    cache.set(key, response as Record<string, unknown>);
    return response;
  });
}

function canonicalInstanceKey(item: Record<string, unknown>): string {
  const space = String(item.space ?? "");
  const externalId = String(item.externalId ?? "");
  const instanceType = String(item.instanceType ?? "");
  return `${space}\x1f${externalId}\x1f${instanceType}`;
}

function sourcesVariantKey(sources: InstancesRetrieveSource[] | undefined): string {
  if (!sources || sources.length === 0) return "";
  return JSON.stringify(sources);
}

function byIdsEntryKey(item: Record<string, unknown>, sourcesKey: string): string {
  const base = canonicalInstanceKey(item);
  return sourcesKey ? `${base}\x1e${sourcesKey}` : base;
}

const enqueueByIds = (task: () => Promise<InstancesRetrieveResponse>) => {
  const next = byIdsQueue.then(task, task);
  byIdsQueue = next.then(
    () => undefined,
    () => undefined
  );
  return next;
};

export async function cachedInstancesByIds(
  sdk: InstancesSdk,
  params: InstancesRetrieveParams
): Promise<InstancesRetrieveResponse> {
  const items = params.items ?? [];
  if (items.length === 0) return { items: [] };

  const sourcesKey = sourcesVariantKey(params.sources);
  const cachedMap = new Map<string, Record<string, unknown>>();
  const missing: Array<Record<string, unknown>> = [];

  for (const item of items) {
    const key = byIdsEntryKey(item, sourcesKey);
    const hit = byIdsCache.get(key);
    if (hit) {
      cachedMap.set(key, hit);
    } else {
      missing.push(item);
    }
  }

  let fetchedMap = new Map<string, Record<string, unknown>>();
  if (missing.length > 0) {
    const response = await enqueueByIds(async () => {
      const body: InstancesRetrieveParams = { items: missing };
      if (params.sources && params.sources.length > 0) {
        body.sources = params.sources;
      }
      return (await sdk.instances.retrieve(body as never)) as InstancesRetrieveResponse;
    });
    for (const item of response.items ?? []) {
      const rec = item as Record<string, unknown>;
      const key = byIdsEntryKey(rec, sourcesKey);
      byIdsCache.set(key, rec);
      fetchedMap.set(key, rec);
    }
  }

  const orderedItems: Array<Record<string, unknown>> = [];
  for (const item of items) {
    const key = byIdsEntryKey(item, sourcesKey);
    const resolved = cachedMap.get(key) ?? fetchedMap.get(key);
    if (resolved) {
      orderedItems.push(resolved);
    }
  }

  return { items: orderedItems };
}

export type InstancesLruStatRow = {
  id: string;
  label: string;
  size: number;
  max: number;
  fillRate: number;
  ttlMs: number;
  calculatedSize: number;
  maxSize: number;
};

export function getInstancesListCacheStats(): InstancesLruStatRow {
  return {
    id: "instances.list",
    label: "Instances list (JSON params)",
    size: cache.size,
    max: cache.max,
    fillRate: cache.max > 0 ? cache.size / cache.max : 0,
    ttlMs: INSTANCES_LIST_CACHE_TTL_MS,
    calculatedSize: cache.calculatedSize ?? 0,
    maxSize: cache.maxSize ?? cache.max,
  };
}

export function getInstancesByIdsCacheStats(): InstancesLruStatRow {
  return {
    id: "instances.byIds",
    label: "Instances retrieve (by id + optional sources)",
    size: byIdsCache.size,
    max: byIdsCache.max,
    fillRate: byIdsCache.max > 0 ? byIdsCache.size / byIdsCache.max : 0,
    ttlMs: INSTANCES_BY_IDS_CACHE_TTL_MS,
    calculatedSize: byIdsCache.calculatedSize ?? 0,
    maxSize: byIdsCache.maxSize ?? byIdsCache.max,
  };
}
