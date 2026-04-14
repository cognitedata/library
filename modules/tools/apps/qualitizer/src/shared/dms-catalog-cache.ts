import type { CogniteClient } from "@cognite/sdk";
import { LRUCache } from "lru-cache";

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

type DmsSdk = Pick<CogniteClient, "project" | "dataModels" | "views">;

function cacheKey(project: string, segment: string, payload: unknown): string {
  return `${project}:${segment}:${JSON.stringify(payload)}`;
}

export const DMS_DM_LIST_CACHE_MAX = 300;
export const DMS_DM_LIST_CACHE_TTL_MS = 10 * 60 * 1000;
const dataModelsListCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_DM_LIST_CACHE_MAX,
  ttl: DMS_DM_LIST_CACHE_TTL_MS,
});

export const DMS_VIEW_LIST_CACHE_MAX = 300;
export const DMS_VIEW_LIST_CACHE_TTL_MS = 10 * 60 * 1000;
const viewsListCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_VIEW_LIST_CACHE_MAX,
  ttl: DMS_VIEW_LIST_CACHE_TTL_MS,
});

export const DMS_DM_RETRIEVE_CACHE_MAX = 500;
export const DMS_DM_RETRIEVE_CACHE_TTL_MS = 10 * 60 * 1000;
const dataModelsRetrieveCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_DM_RETRIEVE_CACHE_MAX,
  ttl: DMS_DM_RETRIEVE_CACHE_TTL_MS,
});

export const DMS_VIEW_RETRIEVE_CACHE_MAX = 500;
export const DMS_VIEW_RETRIEVE_CACHE_TTL_MS = 10 * 60 * 1000;
const viewsRetrieveCache = new LRUCache<string, Record<string, unknown>>({
  max: DMS_VIEW_RETRIEVE_CACHE_MAX,
  ttl: DMS_VIEW_RETRIEVE_CACHE_TTL_MS,
});

export async function cachedDataModelsList(
  sdk: DmsSdk,
  params: Record<string, unknown>
): Promise<unknown> {
  const key = cacheKey(sdk.project, "dmList", params);
  const hit = dataModelsListCache.get(key);
  if (hit) return hit;
  const response = await sdk.dataModels.list(params as never);
  dataModelsListCache.set(key, response as unknown as Record<string, unknown>);
  return response;
}

export async function cachedViewsList(
  sdk: DmsSdk,
  params: Record<string, unknown>
): Promise<unknown> {
  const key = cacheKey(sdk.project, "viewList", params);
  const hit = viewsListCache.get(key);
  if (hit) return hit;
  const response = await sdk.views.list(params as never);
  viewsListCache.set(key, response as unknown as Record<string, unknown>);
  return response;
}

export async function cachedDataModelsRetrieve(
  sdk: DmsSdk,
  params: Array<Record<string, unknown>>,
  options?: Record<string, unknown>
): Promise<unknown> {
  const key = cacheKey(sdk.project, "dmRetrieve", { params, options: options ?? null });
  const hit = dataModelsRetrieveCache.get(key);
  if (hit) return hit;
  const response = await sdk.dataModels.retrieve(params as never, options as never);
  dataModelsRetrieveCache.set(key, response as unknown as Record<string, unknown>);
  return response;
}

export async function cachedViewsRetrieve(
  sdk: DmsSdk,
  params: Array<Record<string, unknown>>,
  options?: Record<string, unknown>
): Promise<unknown> {
  const key = cacheKey(sdk.project, "viewRetrieve", { params, options: options ?? null });
  const hit = viewsRetrieveCache.get(key);
  if (hit) return hit;
  const response = await sdk.views.retrieve(params as never, options as never);
  viewsRetrieveCache.set(key, response as unknown as Record<string, unknown>);
  return response;
}

export function getDmsDataModelsListCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "dms.dataModels.list",
    label: "Data models list",
    size: dataModelsListCache.size,
    max: dataModelsListCache.max,
    fillRate: dataModelsListCache.max > 0 ? dataModelsListCache.size / dataModelsListCache.max : 0,
    ttlMs: DMS_DM_LIST_CACHE_TTL_MS,
    calculatedSize: dataModelsListCache.calculatedSize ?? 0,
    maxSize: dataModelsListCache.maxSize ?? dataModelsListCache.max,
  };
}

export function getDmsViewsListCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "dms.views.list",
    label: "Views list",
    size: viewsListCache.size,
    max: viewsListCache.max,
    fillRate: viewsListCache.max > 0 ? viewsListCache.size / viewsListCache.max : 0,
    ttlMs: DMS_VIEW_LIST_CACHE_TTL_MS,
    calculatedSize: viewsListCache.calculatedSize ?? 0,
    maxSize: viewsListCache.maxSize ?? viewsListCache.max,
  };
}

export function getDmsDataModelsRetrieveCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "dms.dataModels.retrieve",
    label: "Data models retrieve (batch)",
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
    label: "Views retrieve (batch)",
    size: viewsRetrieveCache.size,
    max: viewsRetrieveCache.max,
    fillRate: viewsRetrieveCache.max > 0 ? viewsRetrieveCache.size / viewsRetrieveCache.max : 0,
    ttlMs: DMS_VIEW_RETRIEVE_CACHE_TTL_MS,
    calculatedSize: viewsRetrieveCache.calculatedSize ?? 0,
    maxSize: viewsRetrieveCache.maxSize ?? viewsRetrieveCache.max,
  };
}
