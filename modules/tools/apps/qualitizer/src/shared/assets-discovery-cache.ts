import { LRUCache } from "lru-cache";
import type { DmsCatalogCacheStatRow } from "@/shared/dms-catalog-cache";

/** Mirrors `ResolvedView` in `asset-types` for cached payloads (shared layer must not import internal). */
export type CachedResolvedView = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
  implementsCogniteAsset: boolean;
  implements?: Array<{ space: string; externalId: string; version?: string; name?: string }>;
};

export type CachedAssetDataModel = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
  description?: string;
  assetViews: CachedResolvedView[];
};

export type CachedReferencingModel = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
};

export type CachedAssetView = CachedResolvedView & {
  description?: string;
  usedFor?: "node" | "edge" | "all";
  referencingModels: CachedReferencingModel[];
};

export type AssetDiscoveryPayload = {
  models: CachedAssetDataModel[];
  standalone: CachedAssetView[];
};

export const ASSETS_DISCOVERY_CACHE_MAX = 32;
export const ASSETS_DISCOVERY_CACHE_TTL_MS = 60 * 60 * 1000;

const assetsDiscoveryLru = new LRUCache<string, AssetDiscoveryPayload>({
  max: ASSETS_DISCOVERY_CACHE_MAX,
  ttl: ASSETS_DISCOVERY_CACHE_TTL_MS,
});

export function assetsDiscoveryCacheKey(
  project: string,
  models: ReadonlyArray<{ space: string; externalId: string; version?: string }>
): string {
  const body = models
    .map((m) => `${m.space}:${m.externalId}:${m.version ?? ""}`)
    .sort()
    .join("|");
  return `${project}\t${body}`;
}

export function getCachedAssetDiscovery(cacheKey: string): AssetDiscoveryPayload | undefined {
  return assetsDiscoveryLru.get(cacheKey);
}

export function setCachedAssetDiscovery(cacheKey: string, payload: AssetDiscoveryPayload): void {
  assetsDiscoveryLru.set(cacheKey, payload);
}

export function getAssetsDiscoveryCacheStats(): DmsCatalogCacheStatRow {
  return {
    id: "assets.discovery",
    label: "Assets CogniteAsset discovery",
    size: assetsDiscoveryLru.size,
    max: assetsDiscoveryLru.max,
    fillRate: assetsDiscoveryLru.max > 0 ? assetsDiscoveryLru.size / assetsDiscoveryLru.max : 0,
    ttlMs: ASSETS_DISCOVERY_CACHE_TTL_MS,
    calculatedSize: assetsDiscoveryLru.calculatedSize ?? 0,
    maxSize: assetsDiscoveryLru.maxSize ?? assetsDiscoveryLru.max,
  };
}
