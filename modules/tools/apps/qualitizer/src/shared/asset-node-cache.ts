import { LRUCache } from "lru-cache";

export type CachedNodeSummary = {
  externalId: string;
  space: string;
  properties?: Record<string, Record<string, unknown>>;
};

const DEFAULT_MAX = 30_000;
const TTL = 30 * 60 * 1000;

let cache = new LRUCache<string, CachedNodeSummary>({ max: DEFAULT_MAX, ttl: TTL });

function nodeKey(space: string, externalId: string): string {
  return `${space}:${externalId}`;
}

export function resizeAssetNodeCache(assetLimit: number) {
  const desired = Math.max(assetLimit * 3, 1000);
  if (desired === cache.max) return;
  const old = cache;
  cache = new LRUCache<string, CachedNodeSummary>({ max: desired, ttl: TTL });
  for (const [k, v] of old.entries()) {
    cache.set(k, v);
  }
}

export function getAssetNode(space: string, externalId: string): CachedNodeSummary | undefined {
  return cache.get(nodeKey(space, externalId));
}

export function setAssetNode(node: CachedNodeSummary): void {
  cache.set(nodeKey(node.space, node.externalId), node);
}

export function setAssetNodes(nodes: CachedNodeSummary[]): void {
  for (const node of nodes) {
    cache.set(nodeKey(node.space, node.externalId), node);
  }
}

export function getAssetNodes(keys: Array<{ space: string; externalId: string }>): {
  hits: CachedNodeSummary[];
  misses: Array<{ space: string; externalId: string }>;
} {
  const hits: CachedNodeSummary[] = [];
  const misses: Array<{ space: string; externalId: string }> = [];
  for (const k of keys) {
    const cached = cache.get(nodeKey(k.space, k.externalId));
    if (cached) {
      hits.push(cached);
    } else {
      misses.push(k);
    }
  }
  return { hits, misses };
}

export function clearAssetNodeCache(): void {
  cache.clear();
}

export function assetNodeCacheSize(): number {
  return cache.size;
}
