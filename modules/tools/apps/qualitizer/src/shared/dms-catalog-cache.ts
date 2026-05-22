import type { CogniteClient } from "@cognite/sdk";
import { LRUCache } from "lru-cache";
import { isAppCachingEnabled } from "@/shared/app-caching-flag";

export type DmsCatalogCacheStatRow = {
  id: string;
  label: string;
  size: number;
  max: number;
  fillRate: number;
  ttlMs: number;
  calculatedSize: number;
  maxSize: number;
};

type DmsSdk = Pick<CogniteClient, "project" | "dataModels" | "views"> & {
  getBaseUrl?: () => string;
};

const clientNonce = new WeakMap<object, number>();
let clientNonceSeq = 0;

function nonceForSdk(sdk: unknown): number {
  const key = sdk as object;
  let n = clientNonce.get(key);
  if (n === undefined) {
    n = ++clientNonceSeq;
    clientNonce.set(key, n);
  }
  return n;
}

function cacheKeyStem(sdk: DmsSdk): string {
  const base = typeof sdk.getBaseUrl === "function" ? sdk.getBaseUrl() : "";
  return `${nonceForSdk(sdk)}:${base}:${sdk.project}`;
}

function cacheKey(sdk: DmsSdk, segment: string, payload: unknown): string {
  return `${cacheKeyStem(sdk)}:${segment}:${JSON.stringify(payload)}`;
}

function stableListFingerprint(params: Record<string, unknown>): string {
  const { cursor: _c, limit: _l, ...rest } = params;
  const keys = Object.keys(rest).sort();
  return JSON.stringify(keys.map((k) => [k, rest[k as keyof typeof rest]]));
}

type ListEnumeration = {
  orderedKeys: string[];
  keySet: Set<string>;
  complete: boolean;
};

const dataModelEnumerations = new Map<string, Map<string, ListEnumeration>>();
const viewEnumerations = new Map<string, Map<string, ListEnumeration>>();

const dataModelListInflight = new Map<string, Promise<void>>();
const viewListInflight = new Map<string, Promise<void>>();

export type DmsListAllProgress = {
  itemsLoaded: number;
  uniqueCount: number;
};

const dataModelListInflightProgress = new Map<string, DmsListAllProgress>();
const dataModelListProgressListeners = new Map<string, Set<(p: DmsListAllProgress) => void>>();

function countUniqueDataModelsFromItems(items: Array<{ space?: unknown; externalId?: unknown }>): number {
  const s = new Set<string>();
  for (const item of items) {
    s.add(`${String(item.space ?? "")}:${String(item.externalId ?? "")}`);
  }
  return s.size;
}

function countUniqueDataModelsFromEnumeration(stem: string, state: ListEnumeration): number {
  const s = new Set<string>();
  for (const ik of state.orderedKeys) {
    const row = dataModelsListItemCache.get(dmListItemLruKey(stem, ik));
    if (row) s.add(`${String(row.space ?? "")}:${String(row.externalId ?? "")}`);
  }
  return s.size;
}

function emitDataModelListProgress(
  onProgress: ((p: DmsListAllProgress) => void) | undefined,
  itemsLoaded: number,
  uniqueCount: number
) {
  onProgress?.({ itemsLoaded, uniqueCount });
}

function updateDataModelInflightProgress(inflightKey: string, progress: DmsListAllProgress) {
  dataModelListInflightProgress.set(inflightKey, progress);
  const listeners = dataModelListProgressListeners.get(inflightKey);
  if (listeners) {
    for (const cb of listeners) cb(progress);
  }
}

function subscribeDataModelInflightProgress(
  inflightKey: string,
  onProgress: (p: DmsListAllProgress) => void
): () => void {
  let listeners = dataModelListProgressListeners.get(inflightKey);
  if (!listeners) {
    listeners = new Set();
    dataModelListProgressListeners.set(inflightKey, listeners);
  }
  listeners.add(onProgress);
  const latest = dataModelListInflightProgress.get(inflightKey);
  if (latest) onProgress(latest);
  return () => {
    listeners!.delete(onProgress);
    if (listeners!.size === 0) dataModelListProgressListeners.delete(inflightKey);
  };
}

function clearDataModelInflightProgress(inflightKey: string) {
  dataModelListInflightProgress.delete(inflightKey);
  dataModelListProgressListeners.delete(inflightKey);
}

function getEnumeration(
  store: Map<string, Map<string, ListEnumeration>>,
  stem: string,
  fp: string
): ListEnumeration {
  let inner = store.get(stem);
  if (!inner) {
    inner = new Map();
    store.set(stem, inner);
  }
  let row = inner.get(fp);
  if (!row) {
    row = { orderedKeys: [], keySet: new Set(), complete: false };
    inner.set(fp, row);
  }
  return row;
}

function dataModelItemCacheKey(item: { space?: string; externalId?: string; version?: string }) {
  const space = String(item.space ?? "");
  const externalId = String(item.externalId ?? "");
  const version = String(item.version ?? "");
  return `${space}\x1f${externalId}\x1f${version}`;
}

function viewItemCacheKey(item: { space?: string; externalId?: string; version?: string }) {
  return dataModelItemCacheKey(item);
}

export const DMS_DM_LIST_ITEM_CACHE_MAX = 20_000;
export const DMS_DM_LIST_ITEM_CACHE_TTL_MS = 60 * 60 * 1000;
const dataModelsListItemCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_DM_LIST_ITEM_CACHE_MAX,
  ttl: DMS_DM_LIST_ITEM_CACHE_TTL_MS,
});

export const DMS_VIEW_LIST_ITEM_CACHE_MAX = 50_000;
export const DMS_VIEW_LIST_ITEM_CACHE_TTL_MS = 60 * 60 * 1000;
const viewsListItemCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_VIEW_LIST_ITEM_CACHE_MAX,
  ttl: DMS_VIEW_LIST_ITEM_CACHE_TTL_MS,
});

export const DMS_DM_RETRIEVE_CACHE_MAX = 20_000;
export const DMS_DM_RETRIEVE_CACHE_TTL_MS = 60 * 60 * 1000;
const dataModelsRetrieveCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_DM_RETRIEVE_CACHE_MAX,
  ttl: DMS_DM_RETRIEVE_CACHE_TTL_MS,
});

export const DMS_VIEW_RETRIEVE_CACHE_MAX = 50_000;
export const DMS_VIEW_RETRIEVE_CACHE_TTL_MS = 60 * 60 * 1000;
const viewsRetrieveCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_VIEW_RETRIEVE_CACHE_MAX,
  ttl: DMS_VIEW_RETRIEVE_CACHE_TTL_MS,
});

function dmListItemLruKey(stem: string, itemKey: string) {
  return `${stem}\x1edmLI\x1e${itemKey}`;
}

function viewListItemLruKey(stem: string, itemKey: string) {
  return `${stem}\x1evwLI\x1e${itemKey}`;
}

function ingestDataModelsListResponse(
  stem: string,
  fp: string,
  response: { items?: unknown[]; nextCursor?: string | null }
) {
  const state = getEnumeration(dataModelEnumerations, stem, fp);
  const items = response.items ?? [];
  for (const raw of items) {
    if (!raw || typeof raw !== "object") continue;
    const item = raw as Record<string, unknown>;
    const ik = dataModelItemCacheKey({
      space: item.space as string | undefined,
      externalId: item.externalId as string | undefined,
      version: item.version as string | undefined,
    });
    dataModelsListItemCache.set(dmListItemLruKey(stem, ik), item as Record<string, unknown>);
    if (!state.keySet.has(ik)) {
      state.keySet.add(ik);
      state.orderedKeys.push(ik);
    }
  }
  if (!response.nextCursor) {
    state.complete = true;
  }
}

function ingestViewsListResponse(
  stem: string,
  fp: string,
  response: { items?: unknown[]; nextCursor?: string | null }
) {
  const state = getEnumeration(viewEnumerations, stem, fp);
  const items = response.items ?? [];
  for (const raw of items) {
    if (!raw || typeof raw !== "object") continue;
    const item = raw as Record<string, unknown>;
    const ik = viewItemCacheKey({
      space: item.space as string | undefined,
      externalId: item.externalId as string | undefined,
      version: item.version as string | undefined,
    });
    viewsListItemCache.set(viewListItemLruKey(stem, ik), item as Record<string, unknown>);
    if (!state.keySet.has(ik)) {
      state.keySet.add(ik);
      state.orderedKeys.push(ik);
    }
  }
  if (!response.nextCursor) {
    state.complete = true;
  }
}

function assembleDataModelsFromCache(stem: string, state: ListEnumeration): Record<string, unknown>[] | null {
  const out: Record<string, unknown>[] = [];
  for (const ik of state.orderedKeys) {
    const row = dataModelsListItemCache.get(dmListItemLruKey(stem, ik));
    if (!row) return null;
    out.push(row);
  }
  return out;
}

function assembleViewsFromCache(stem: string, state: ListEnumeration): Record<string, unknown>[] | null {
  const out: Record<string, unknown>[] = [];
  for (const ik of state.orderedKeys) {
    const row = viewsListItemCache.get(viewListItemLruKey(stem, ik));
    if (!row) return null;
    out.push(row);
  }
  return out;
}

function invalidateDmEnumeration(stem: string, fp: string) {
  const inner = dataModelEnumerations.get(stem);
  if (!inner) return;
  inner.delete(fp);
}

function invalidateViewEnumeration(stem: string, fp: string) {
  const inner = viewEnumerations.get(stem);
  if (!inner) return;
  inner.delete(fp);
}

export async function listAllCachedDataModels(
  sdk: DmsSdk,
  baseParams: Record<string, unknown>,
  opts?: { pageLimit?: number; onProgress?: (progress: DmsListAllProgress) => void }
): Promise<Record<string, unknown>[]> {
  const onProgress = opts?.onProgress;
  const limit = opts?.pageLimit ?? 100;

  if (!isAppCachingEnabled()) {
    const items: Record<string, unknown>[] = [];
    let cursor: string | undefined;
    do {
      const response = (await sdk.dataModels.list({
        ...baseParams,
        limit,
        cursor,
      } as never)) as unknown as { items?: Record<string, unknown>[]; nextCursor?: string };
      items.push(...(response.items ?? []));
      emitDataModelListProgress(onProgress, items.length, countUniqueDataModelsFromItems(items));
      cursor = response.nextCursor ?? undefined;
    } while (cursor);
    return items;
  }

  const stem = cacheKeyStem(sdk);
  const fp = stableListFingerprint(baseParams);
  const inflightKey = `${stem}\x1edmFull\x1e${fp}`;

  for (let attempt = 0; attempt < 2; attempt++) {
    const state = getEnumeration(dataModelEnumerations, stem, fp);
    if (state.complete) {
      const assembled = assembleDataModelsFromCache(stem, state);
      if (assembled) {
        emitDataModelListProgress(
          onProgress,
          assembled.length,
          countUniqueDataModelsFromItems(assembled)
        );
        return assembled;
      }
      invalidateDmEnumeration(stem, fp);
    }

    let wait = dataModelListInflight.get(inflightKey);
    const unsubProgress = onProgress ? subscribeDataModelInflightProgress(inflightKey, onProgress) : undefined;
    if (!wait) {
      wait = (async () => {
        const s = getEnumeration(dataModelEnumerations, stem, fp);
        s.orderedKeys.length = 0;
        s.keySet.clear();
        s.complete = false;
        updateDataModelInflightProgress(inflightKey, { itemsLoaded: 0, uniqueCount: 0 });
        let cursor: string | undefined;
        do {
          const response = (await sdk.dataModels.list({
            ...baseParams,
            limit,
            cursor,
          } as never)) as { items?: unknown[]; nextCursor?: string | null };
          ingestDataModelsListResponse(stem, fp, response);
          const afterPage = getEnumeration(dataModelEnumerations, stem, fp);
          updateDataModelInflightProgress(inflightKey, {
            itemsLoaded: afterPage.orderedKeys.length,
            uniqueCount: countUniqueDataModelsFromEnumeration(stem, afterPage),
          });
          cursor = response.nextCursor ?? undefined;
        } while (cursor);
      })().finally(() => {
        dataModelListInflight.delete(inflightKey);
        clearDataModelInflightProgress(inflightKey);
      });
      dataModelListInflight.set(inflightKey, wait);
    }
    try {
      await wait;
    } finally {
      unsubProgress?.();
    }

    const finalState = getEnumeration(dataModelEnumerations, stem, fp);
    const assembled = assembleDataModelsFromCache(stem, finalState);
    if (assembled) {
      emitDataModelListProgress(
        onProgress,
        assembled.length,
        countUniqueDataModelsFromItems(assembled)
      );
      return assembled;
    }
    invalidateDmEnumeration(stem, fp);
  }

  const items: Record<string, unknown>[] = [];
  let cursor: string | undefined;
  do {
    const response = (await sdk.dataModels.list({
      ...baseParams,
      limit,
      cursor,
    } as never)) as unknown as { items?: Record<string, unknown>[]; nextCursor?: string };
    items.push(...(response.items ?? []));
    emitDataModelListProgress(onProgress, items.length, countUniqueDataModelsFromItems(items));
    cursor = response.nextCursor ?? undefined;
  } while (cursor);
  return items;
}

export async function listAllCachedViews(
  sdk: DmsSdk,
  baseParams: Record<string, unknown>,
  opts?: { pageLimit?: number }
): Promise<Record<string, unknown>[]> {
  if (!isAppCachingEnabled()) {
    const limit = opts?.pageLimit ?? 100;
    const items: Record<string, unknown>[] = [];
    let cursor: string | undefined;
    do {
      const response = (await sdk.views.list({
        ...baseParams,
        limit,
        cursor,
      } as never)) as unknown as { items?: Record<string, unknown>[]; nextCursor?: string };
      items.push(...(response.items ?? []));
      cursor = response.nextCursor ?? undefined;
    } while (cursor);
    return items;
  }

  const stem = cacheKeyStem(sdk);
  const fp = stableListFingerprint(baseParams);
  const inflightKey = `${stem}\x1evwFull\x1e${fp}`;

  for (let attempt = 0; attempt < 2; attempt++) {
    const state = getEnumeration(viewEnumerations, stem, fp);
    if (state.complete) {
      const assembled = assembleViewsFromCache(stem, state);
      if (assembled) return assembled;
      invalidateViewEnumeration(stem, fp);
    }

    let wait = viewListInflight.get(inflightKey);
    if (!wait) {
      const limit = opts?.pageLimit ?? 100;
      wait = (async () => {
        const s = getEnumeration(viewEnumerations, stem, fp);
        s.orderedKeys.length = 0;
        s.keySet.clear();
        s.complete = false;
        let cursor: string | undefined;
        do {
          const response = (await sdk.views.list({
            ...baseParams,
            limit,
            cursor,
          } as never)) as { items?: unknown[]; nextCursor?: string | null };
          ingestViewsListResponse(stem, fp, response);
          cursor = response.nextCursor ?? undefined;
        } while (cursor);
      })().finally(() => {
        viewListInflight.delete(inflightKey);
      });
      viewListInflight.set(inflightKey, wait);
    }
    await wait;

    const finalState = getEnumeration(viewEnumerations, stem, fp);
    const assembled = assembleViewsFromCache(stem, finalState);
    if (assembled) return assembled;
    invalidateViewEnumeration(stem, fp);
  }

  const limit = opts?.pageLimit ?? 100;
  const items: Record<string, unknown>[] = [];
  let cursor: string | undefined;
  do {
    const response = (await sdk.views.list({
      ...baseParams,
      limit,
      cursor,
    } as never)) as unknown as { items?: Record<string, unknown>[]; nextCursor?: string };
    items.push(...(response.items ?? []));
    cursor = response.nextCursor ?? undefined;
  } while (cursor);
  return items;
}

export async function cachedDataModelsList(
  sdk: DmsSdk,
  params: Record<string, unknown>
): Promise<unknown> {
  if (!isAppCachingEnabled()) {
    return sdk.dataModels.list(params as never);
  }
  const stem = cacheKeyStem(sdk);
  const fp = stableListFingerprint(params);
  const response = (await sdk.dataModels.list(params as never)) as {
    items?: unknown[];
    nextCursor?: string | null;
  };
  ingestDataModelsListResponse(stem, fp, response);
  return response;
}

function retrieveItemCacheKey(
  sdk: DmsSdk,
  segment: "dmRetrieveItem" | "viewRetrieveItem",
  param: Record<string, unknown>,
  options?: Record<string, unknown>
): string {
  return cacheKey(sdk, segment, { param, options: options ?? null });
}

export async function cachedViewsList(
  sdk: DmsSdk,
  params: Record<string, unknown>
): Promise<unknown> {
  if (!isAppCachingEnabled()) {
    return sdk.views.list(params as never);
  }
  const stem = cacheKeyStem(sdk);
  const fp = stableListFingerprint(params);
  const response = (await sdk.views.list(params as never)) as {
    items?: unknown[];
    nextCursor?: string | null;
  };
  ingestViewsListResponse(stem, fp, response);
  return response;
}

export async function cachedDataModelsRetrieve(
  sdk: DmsSdk,
  params: Array<Record<string, unknown>>,
  options?: Record<string, unknown>
): Promise<unknown> {
  if (!isAppCachingEnabled()) {
    return sdk.dataModels.retrieve(params as never, options as never);
  }
  if (params.length === 0) return { items: [] };

  const items: Array<Record<string, unknown> | undefined> = new Array(params.length).fill(undefined);
  const misses: Array<{ index: number; param: Record<string, unknown>; key: string }> = [];

  for (let i = 0; i < params.length; i++) {
    const param = params[i];
    const key = retrieveItemCacheKey(sdk, "dmRetrieveItem", param, options);
    const hit = dataModelsRetrieveCache.get(key);
    if (hit) {
      items[i] = hit;
    } else {
      misses.push({ index: i, param, key });
    }
  }

  if (misses.length > 0) {
    const response = (await sdk.dataModels.retrieve(
      misses.map((m) => m.param) as never,
      options as never
    )) as unknown as { items?: Array<Record<string, unknown>> };
    const fetchedItems = response.items ?? [];
    for (let i = 0; i < misses.length; i++) {
      const miss = misses[i];
      const fetched = fetchedItems[i];
      if (!miss || !fetched) continue;
      dataModelsRetrieveCache.set(miss.key, fetched);
      items[miss.index] = fetched;
    }
  }

  return { items };
}

export async function cachedViewsRetrieve(
  sdk: DmsSdk,
  params: Array<Record<string, unknown>>,
  options?: Record<string, unknown>
): Promise<unknown> {
  if (!isAppCachingEnabled()) {
    return sdk.views.retrieve(params as never, options as never);
  }
  if (params.length === 0) return { items: [] };

  const items: Array<Record<string, unknown> | undefined> = new Array(params.length).fill(undefined);
  const misses: Array<{ index: number; param: Record<string, unknown>; key: string }> = [];

  for (let i = 0; i < params.length; i++) {
    const param = params[i];
    const key = retrieveItemCacheKey(sdk, "viewRetrieveItem", param, options);
    const hit = viewsRetrieveCache.get(key);
    if (hit) {
      items[i] = hit;
    } else {
      misses.push({ index: i, param, key });
    }
  }

  if (misses.length > 0) {
    const response = (await sdk.views.retrieve(
      misses.map((m) => m.param) as never,
      options as never
    )) as unknown as { items?: Array<Record<string, unknown>> };
    const fetchedItems = response.items ?? [];
    for (let i = 0; i < misses.length; i++) {
      const miss = misses[i];
      const fetched = fetchedItems[i];
      if (!miss || !fetched) continue;
      viewsRetrieveCache.set(miss.key, fetched);
      items[miss.index] = fetched;
    }
  }

  return { items };
}

export function getDmsDataModelsListItemCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "dms.dataModels.listItems",
    label: "Data models list (per item)",
    size: dataModelsListItemCache.size,
    max: dataModelsListItemCache.max,
    fillRate: dataModelsListItemCache.max > 0 ? dataModelsListItemCache.size / dataModelsListItemCache.max : 0,
    ttlMs: DMS_DM_LIST_ITEM_CACHE_TTL_MS,
    calculatedSize: dataModelsListItemCache.calculatedSize ?? 0,
    maxSize: dataModelsListItemCache.maxSize ?? dataModelsListItemCache.max,
  };
}

export function getDmsViewsListItemCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "dms.views.listItems",
    label: "Views list (per item)",
    size: viewsListItemCache.size,
    max: viewsListItemCache.max,
    fillRate: viewsListItemCache.max > 0 ? viewsListItemCache.size / viewsListItemCache.max : 0,
    ttlMs: DMS_VIEW_LIST_ITEM_CACHE_TTL_MS,
    calculatedSize: viewsListItemCache.calculatedSize ?? 0,
    maxSize: viewsListItemCache.maxSize ?? viewsListItemCache.max,
  };
}

export function getDmsDataModelsRetrieveCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "dms.dataModels.retrieve",
    label: "Data models retrieve (per item)",
    size: dataModelsRetrieveCache.size,
    max: dataModelsRetrieveCache.max,
    fillRate:
      dataModelsRetrieveCache.max > 0 ? dataModelsRetrieveCache.size / dataModelsRetrieveCache.max : 0,
    ttlMs: DMS_DM_RETRIEVE_CACHE_TTL_MS,
    calculatedSize: dataModelsRetrieveCache.calculatedSize ?? 0,
    maxSize: dataModelsRetrieveCache.maxSize ?? dataModelsRetrieveCache.max,
  };
}

export function getDmsViewsRetrieveCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "dms.views.retrieve",
    label: "Views retrieve (per item)",
    size: viewsRetrieveCache.size,
    max: viewsRetrieveCache.max,
    fillRate: viewsRetrieveCache.max > 0 ? viewsRetrieveCache.size / viewsRetrieveCache.max : 0,
    ttlMs: DMS_VIEW_RETRIEVE_CACHE_TTL_MS,
    calculatedSize: viewsRetrieveCache.calculatedSize ?? 0,
    maxSize: viewsRetrieveCache.maxSize ?? viewsRetrieveCache.max,
  };
}
