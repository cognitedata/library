import { LRUCache } from "lru-cache";

type InstancesListParams = Record<string, unknown>;
type InstancesListResponse = unknown;
type InstancesRetrieveParams = { items: Array<Record<string, unknown>> };
type InstancesRetrieveResponse = { items?: Array<Record<string, unknown>> };

type InstancesSdk = {
  project: string;
  instances: {
    list: (params: InstancesListParams) => Promise<InstancesListResponse>;
    retrieve: (params: InstancesRetrieveParams) => Promise<InstancesRetrieveResponse>;
  };
};

const cache = new LRUCache<string, InstancesListResponse>({
  max: 2000,
  ttl: 15 * 60 * 1000,
});

let queue: Promise<InstancesListResponse> = Promise.resolve(undefined as InstancesListResponse);
const byIdsCache = new LRUCache<string, Record<string, unknown>>({
  max: 2000,
  ttl: 15 * 60 * 1000,
});
let byIdsQueue: Promise<InstancesRetrieveResponse> = Promise.resolve(
  undefined as InstancesRetrieveResponse
);

const enqueue = (task: () => Promise<InstancesListResponse>) => {
  const next = queue.then(task, task);
  queue = next.then(
    () => undefined as InstancesListResponse,
    () => undefined as InstancesListResponse
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
    const response = await sdk.instances.list(params);
    cache.set(key, response);
    return response;
  });
}

const getTypeKey = (item: Record<string, unknown>) => {
  const typeValue = item.type as { space?: string; externalId?: string } | string | undefined;
  if (typeValue && typeof typeValue === "object") {
    return `${typeValue.space ?? ""}:${typeValue.externalId ?? ""}`;
  }
  return String(typeValue ?? "");
};

const getByIdKey = (item: Record<string, unknown>) => {
  const externalId = String(item.externalId ?? "");
  const instanceType = String(item.instanceType ?? "");
  const typeKey = getTypeKey(item);
  return `${externalId}#${instanceType}#${typeKey}`;
};

const enqueueByIds = (task: () => Promise<InstancesRetrieveResponse>) => {
  const next = byIdsQueue.then(task, task);
  byIdsQueue = next.then(
    () => undefined as InstancesRetrieveResponse,
    () => undefined as InstancesRetrieveResponse
  );
  return next;
};

export async function cachedInstancesByIds(
  sdk: InstancesSdk,
  params: InstancesRetrieveParams
): Promise<InstancesRetrieveResponse> {
  const items = params.items ?? [];
  if (items.length === 0) return { items: [] };

  const cachedMap = new Map<string, Record<string, unknown>>();
  const missing: Array<Record<string, unknown>> = [];

  for (const item of items) {
    const key = getByIdKey(item);
    const cached = byIdsCache.get(key);
    if (cached) {
      cachedMap.set(key, cached);
    } else {
      missing.push(item);
    }
  }

  let fetchedMap = new Map<string, Record<string, unknown>>();
  if (missing.length > 0) {
    const response = await enqueueByIds(async () => {
      const result = (await sdk.instances.retrieve({
        items: missing,
      })) as InstancesRetrieveResponse;
      return result;
    });
    for (const item of response.items ?? []) {
      const key = getByIdKey(item as Record<string, unknown>);
      byIdsCache.set(key, item as Record<string, unknown>);
      fetchedMap.set(key, item as Record<string, unknown>);
    }
  }

  const orderedItems: Array<Record<string, unknown>> = [];
  for (const item of items) {
    const key = getByIdKey(item);
    const resolved = cachedMap.get(key) ?? fetchedMap.get(key);
    if (resolved) {
      orderedItems.push(resolved);
    }
  }

  return { items: orderedItems };
}
