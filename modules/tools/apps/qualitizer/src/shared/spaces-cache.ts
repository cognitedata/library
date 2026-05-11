import type { CogniteClient } from "@cognite/sdk";
import { LRUCache } from "lru-cache";
import { isAppCachingEnabled } from "@/shared/app-caching-flag";

type SpacesSdk = Pick<CogniteClient, "project" | "spaces"> & {
  getBaseUrl?: () => string;
};

type ListEnumeration = {
  orderedKeys: string[];
  keySet: Set<string>;
  complete: boolean;
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

function cacheKeyStem(sdk: SpacesSdk): string {
  const base = typeof sdk.getBaseUrl === "function" ? sdk.getBaseUrl() : "";
  return `${nonceForSdk(sdk)}:${base}:${sdk.project}`;
}

function stableListFingerprint(params: Record<string, unknown>): string {
  const { cursor: _c, limit: _l, ...rest } = params;
  const keys = Object.keys(rest).sort();
  return JSON.stringify(keys.map((k) => [k, rest[k as keyof typeof rest]]));
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

function spaceItemCacheKey(item: { space?: string }): string {
  return String(item.space ?? "");
}

function spaceListItemLruKey(stem: string, itemKey: string) {
  return `${stem}\x1espLI\x1e${itemKey}`;
}

const spaceEnumerations = new Map<string, Map<string, ListEnumeration>>();
const spacesListInflight = new Map<string, Promise<void>>();

export const SPACES_LIST_ITEM_CACHE_MAX = 5_000;
export const SPACES_LIST_ITEM_CACHE_TTL_MS = 60 * 60 * 1000;

const spacesListItemCache = new LRUCache<string, Record<string, unknown>>({
  max: SPACES_LIST_ITEM_CACHE_MAX,
  ttl: SPACES_LIST_ITEM_CACHE_TTL_MS,
});

function ingestSpacesListResponse(
  stem: string,
  fp: string,
  response: { items?: unknown[]; nextCursor?: string | null }
) {
  const state = getEnumeration(spaceEnumerations, stem, fp);
  const items = response.items ?? [];
  for (const raw of items) {
    if (!raw || typeof raw !== "object") continue;
    const item = raw as Record<string, unknown>;
    const ik = spaceItemCacheKey({ space: item.space as string | undefined });
    spacesListItemCache.set(spaceListItemLruKey(stem, ik), item);
    if (!state.keySet.has(ik)) {
      state.keySet.add(ik);
      state.orderedKeys.push(ik);
    }
  }
  if (!response.nextCursor) {
    state.complete = true;
  }
}

function assembleSpacesFromCache(stem: string, state: ListEnumeration): Record<string, unknown>[] | null {
  const out: Record<string, unknown>[] = [];
  for (const ik of state.orderedKeys) {
    const row = spacesListItemCache.get(spaceListItemLruKey(stem, ik));
    if (!row) return null;
    out.push(row);
  }
  return out;
}

function invalidateSpacesEnumeration(stem: string, fp: string) {
  const inner = spaceEnumerations.get(stem);
  if (!inner) return;
  inner.delete(fp);
}

export async function listAllCachedSpaces(
  sdk: SpacesSdk,
  baseParams: Record<string, unknown>,
  opts?: { pageLimit?: number }
): Promise<Record<string, unknown>[]> {
  if (!isAppCachingEnabled()) {
    const items: Record<string, unknown>[] = [];
    const limit = opts?.pageLimit ?? 100;
    let cursor: string | undefined;
    do {
      const response = (await sdk.spaces.list({
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
  const inflightKey = `${stem}\x1espFull\x1e${fp}`;

  for (let attempt = 0; attempt < 2; attempt++) {
    const state = getEnumeration(spaceEnumerations, stem, fp);
    if (state.complete) {
      const assembled = assembleSpacesFromCache(stem, state);
      if (assembled) return assembled;
      invalidateSpacesEnumeration(stem, fp);
    }

    let wait = spacesListInflight.get(inflightKey);
    if (!wait) {
      const limit = opts?.pageLimit ?? 100;
      wait = (async () => {
        const s = getEnumeration(spaceEnumerations, stem, fp);
        s.orderedKeys.length = 0;
        s.keySet.clear();
        s.complete = false;
        let cursor: string | undefined;
        do {
          const response = (await sdk.spaces.list({
            ...baseParams,
            limit,
            cursor,
          } as never)) as { items?: unknown[]; nextCursor?: string | null };
          ingestSpacesListResponse(stem, fp, response);
          cursor = response.nextCursor ?? undefined;
        } while (cursor);
      })().finally(() => {
        spacesListInflight.delete(inflightKey);
      });
      spacesListInflight.set(inflightKey, wait);
    }

    await wait;
    const finalState = getEnumeration(spaceEnumerations, stem, fp);
    const assembled = assembleSpacesFromCache(stem, finalState);
    if (assembled) return assembled;
    invalidateSpacesEnumeration(stem, fp);
  }

  const items: Record<string, unknown>[] = [];
  const limit = opts?.pageLimit ?? 100;
  let cursor: string | undefined;
  do {
    const response = (await sdk.spaces.list({
      ...baseParams,
      limit,
      cursor,
    } as never)) as unknown as { items?: Record<string, unknown>[]; nextCursor?: string };
    items.push(...(response.items ?? []));
    cursor = response.nextCursor ?? undefined;
  } while (cursor);
  return items;
}
