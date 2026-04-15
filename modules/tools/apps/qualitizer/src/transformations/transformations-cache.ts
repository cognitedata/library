import type { CogniteClient } from "@cognite/sdk";
import { LRUCache } from "lru-cache";

export type TransformationByIdsRow = {
  id?: number | string;
  name?: string;
  query?: string;
  destination?: {
    view?: { space?: string; externalId?: string; version?: string };
    dataModel?: {
      space?: string;
      externalId?: string;
      version?: string;
      destinationType?: string;
    };
  };
};

export type TransformationCacheStatRow = {
  id: string;
  label: string;
  size: number;
  max: number;
  fillRate: number;
  ttlMs: number;
  calculatedSize: number;
  maxSize: number;
};

type TxGetSdk = Pick<CogniteClient, "project" | "get">;
type TxPostSdk = Pick<CogniteClient, "project" | "post">;

function stableParamKey(params: Record<string, string>): string {
  const keys = Object.keys(params).sort();
  return keys.map((k) => `${encodeURIComponent(k)}=${encodeURIComponent(params[k] ?? "")}`).join("&");
}

function rowKey(project: string, id: string): string {
  return `${project}\x1f${id}`;
}

export const TX_LIST_CACHE_MAX = 80;
export const TX_LIST_CACHE_TTL_MS = 5 * 60 * 1000;
const listCache = new LRUCache<string, Record<string, unknown>>({
  max: TX_LIST_CACHE_MAX,
  ttl: TX_LIST_CACHE_TTL_MS,
});

export const TX_JOBS_CACHE_MAX = 3000;
export const TX_JOBS_CACHE_TTL_MS = 5 * 60 * 1000;
const jobsCache = new LRUCache<string, Record<string, unknown>>({
  max: TX_JOBS_CACHE_MAX,
  ttl: TX_JOBS_CACHE_TTL_MS,
});

export const TX_JOB_METRICS_CACHE_MAX = 4000;
export const TX_JOB_METRICS_CACHE_TTL_MS = 2 * 60 * 1000;
const jobMetricsCache = new LRUCache<string, Record<string, unknown>>({
  max: TX_JOB_METRICS_CACHE_MAX,
  ttl: TX_JOB_METRICS_CACHE_TTL_MS,
});

export const TX_BY_ID_ROW_CACHE_MAX = 8000;
export const TX_BY_ID_ROW_CACHE_TTL_MS = 10 * 60 * 1000;
const byIdRowCache = new LRUCache<string, TransformationByIdsRow>({
  max: TX_BY_ID_ROW_CACHE_MAX,
  ttl: TX_BY_ID_ROW_CACHE_TTL_MS,
});

export async function cachedTransformationsList(
  sdk: TxGetSdk,
  params: Record<string, string>
): Promise<unknown> {
  const key = `${sdk.project}:txList:${stableParamKey(params)}`;
  const hit = listCache.get(key);
  if (hit) return hit;
  const response = await sdk.get(`/api/v1/projects/${sdk.project}/transformations`, { params });
  listCache.set(key, response as unknown as Record<string, unknown>);
  return response;
}

export async function cachedTransformationJobs(
  sdk: TxGetSdk,
  transformationId: string,
  limit: string
): Promise<unknown> {
  const key = `${sdk.project}:txJobs:${transformationId}:${limit}`;
  const hit = jobsCache.get(key);
  if (hit) return hit;
  const response = await sdk.get(`/api/v1/projects/${sdk.project}/transformations/jobs`, {
    params: { limit, transformationId },
  });
  jobsCache.set(key, response as unknown as Record<string, unknown>);
  return response;
}

export async function cachedTransformationJobMetrics(
  sdk: TxGetSdk,
  jobId: string
): Promise<unknown> {
  const key = `${sdk.project}:txJobMetrics:${jobId}`;
  const hit = jobMetricsCache.get(key);
  if (hit) return hit;
  const response = await sdk.get(
    `/api/v1/projects/${sdk.project}/transformations/jobs/${jobId}/metrics`
  );
  jobMetricsCache.set(key, response as unknown as Record<string, unknown>);
  return response;
}

const BATCH_SIZE = 100;

function buildByIdsPayload(ids: string[]): Array<{ id: number } | { id: string }> {
  const items: Array<{ id: number } | { id: string }> = [];
  for (const raw of ids) {
    if (!raw) continue;
    const n = Number(raw);
    if (Number.isFinite(n) && String(n) === raw) items.push({ id: n });
    else items.push({ id: raw });
  }
  return items;
}

export async function fetchTransformationsByIds(
  sdk: TxPostSdk,
  project: string,
  ids: string[]
): Promise<Map<string, TransformationByIdsRow>> {
  const out = new Map<string, TransformationByIdsRow>();
  const unique = [...new Set(ids.filter(Boolean))];
  const missing: string[] = [];
  for (const id of unique) {
    const k = rowKey(project, id);
    const row = byIdRowCache.get(k);
    if (row) out.set(id, row);
    else missing.push(id);
  }

  for (let i = 0; i < missing.length; i += BATCH_SIZE) {
    const chunk = missing.slice(i, i + BATCH_SIZE);
    const items = buildByIdsPayload(chunk);
    if (items.length === 0) continue;
    try {
      const response = (await sdk.post(
        `/api/v1/projects/${project}/transformations/byids`,
        { data: { items } }
      )) as { data?: { items?: TransformationByIdsRow[] } };
      for (const row of response.data?.items ?? []) {
        if (row.id == null) continue;
        const idStr = String(row.id);
        byIdRowCache.set(rowKey(project, idStr), row);
        out.set(idStr, row);
      }
    } catch {
      /* skip batch */
    }
  }
  return out;
}

export function getTransformationsListCacheStats(): TransformationCacheStatRow {
  return {
    id: "transformations.list",
    label: "Transformations list (GET)",
    size: listCache.size,
    max: listCache.max,
    fillRate: listCache.max > 0 ? listCache.size / listCache.max : 0,
    ttlMs: TX_LIST_CACHE_TTL_MS,
    calculatedSize: listCache.calculatedSize ?? 0,
    maxSize: listCache.maxSize ?? listCache.max,
  };
}

export function getTransformationJobsCacheStats(): TransformationCacheStatRow {
  return {
    id: "transformations.jobs",
    label: "Transformation jobs (GET)",
    size: jobsCache.size,
    max: jobsCache.max,
    fillRate: jobsCache.max > 0 ? jobsCache.size / jobsCache.max : 0,
    ttlMs: TX_JOBS_CACHE_TTL_MS,
    calculatedSize: jobsCache.calculatedSize ?? 0,
    maxSize: jobsCache.maxSize ?? jobsCache.max,
  };
}

export function getTransformationJobMetricsCacheStats(): TransformationCacheStatRow {
  return {
    id: "transformations.jobMetrics",
    label: "Transformation job metrics (GET)",
    size: jobMetricsCache.size,
    max: jobMetricsCache.max,
    fillRate: jobMetricsCache.max > 0 ? jobMetricsCache.size / jobMetricsCache.max : 0,
    ttlMs: TX_JOB_METRICS_CACHE_TTL_MS,
    calculatedSize: jobMetricsCache.calculatedSize ?? 0,
    maxSize: jobMetricsCache.maxSize ?? jobMetricsCache.max,
  };
}

export function getTransformationByIdRowCacheStats(): TransformationCacheStatRow {
  return {
    id: "transformations.byIdRows",
    label: "Transformation by-id rows (POST byids, per id)",
    size: byIdRowCache.size,
    max: byIdRowCache.max,
    fillRate: byIdRowCache.max > 0 ? byIdRowCache.size / byIdRowCache.max : 0,
    ttlMs: TX_BY_ID_ROW_CACHE_TTL_MS,
    calculatedSize: byIdRowCache.calculatedSize ?? 0,
    maxSize: byIdRowCache.maxSize ?? byIdRowCache.max,
  };
}
