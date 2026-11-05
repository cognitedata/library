import { getCdfDeleteTaskRunner, getCdfTaskRunner } from "@/utils/semaphore";

export interface CdfConcurrencyOptions {
  /** Use stricter concurrency limit for instances.delete (DMS). */
  delete?: boolean;
}

/**
 * Limits concurrent SDK calls via QueuedTaskRunner. Transient HTTP failures are
 * retried by @cognite/sdk / @cognite/sdk-core; do not stack app-level retry here.
 */
export function withCdfConcurrency<T>(
  fn: () => Promise<T>,
  opts: CdfConcurrencyOptions = {},
): Promise<T> {
  const { delete: isDelete } = opts;
  const runner = isDelete ? getCdfDeleteTaskRunner() : getCdfTaskRunner();
  return runner.schedule(fn);
}

// ============================================================
// Error introspection helpers
// ============================================================

function collectErrorChain(error: unknown, maxDepth: number): unknown[] {
  const out: unknown[] = [];
  let current: unknown = error;
  for (let i = 0; i < maxDepth && current != null; i++) {
    out.push(current);
    if (typeof current !== "object") break;
    const o = current as Record<string, unknown>;
    current = o.cause ?? o.originalError ?? o.innerError;
  }
  return out;
}

function getHeader(headers: unknown, name: string): string | null {
  if (!headers || typeof headers !== "object") return null;
  const h = headers as Record<string, unknown>;
  const lower = name.toLowerCase();
  const key = Object.keys(h).find((k) => k.toLowerCase() === lower);
  if (key !== undefined) {
    const v = h[key];
    if (typeof v === "string") return v;
    if (Array.isArray(v) && typeof v[0] === "string") return v[0];
  }
  if (
    "get" in h &&
    typeof (h as { get: (n: string) => unknown }).get === "function"
  ) {
    const raw = (h as { get: (n: string) => unknown }).get(name);
    if (typeof raw === "string") return raw;
    if (Array.isArray(raw) && typeof raw[0] === "string") return raw[0];
  }
  return null;
}

export function getHttpStatusFromError(error: unknown): number | undefined {
  for (const e of collectErrorChain(error, 4)) {
    if (e === null || typeof e !== "object") continue;
    const rec = e as Record<string, unknown>;
    if (typeof rec.status === "number") return rec.status;
    const response = rec.response as Record<string, unknown> | undefined;
    if (response && typeof response.status === "number") return response.status;
  }
  return undefined;
}

export function getRequestIdFromError(error: unknown): string | undefined {
  for (const e of collectErrorChain(error, 4)) {
    if (e === null || typeof e !== "object") continue;
    const rec = e as Record<string, unknown>;
    if (typeof rec.requestId === "string" && rec.requestId.trim() !== "") {
      return rec.requestId;
    }
    const response = rec.response as Record<string, unknown> | undefined;
    const headers = response?.headers ?? rec.headers;
    const fromHeader =
      getHeader(headers, "x-request-id") ??
      getHeader(headers, "x-cdf-request-id");
    if (fromHeader) return fromHeader;
    const extra = rec.extra as Record<string, unknown> | undefined;
    if (
      extra &&
      typeof extra.requestId === "string" &&
      extra.requestId.trim() !== ""
    ) {
      return extra.requestId;
    }
  }
  return undefined;
}
