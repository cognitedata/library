/** Default total row cap for new CDM view-query (query_view) nodes. */
export const DEFAULT_VIEW_QUERY_LIMIT = 1000;

export function defaultQueryViewNodeConfig(
  extras?: Record<string, unknown>
): Record<string, unknown> {
  return { limit: DEFAULT_VIEW_QUERY_LIMIT, ...extras };
}

/** Stored total row cap, or the canvas/editor default when unset. */
export function readViewQueryLimit(cfg: Record<string, unknown>): number {
  const raw = cfg.read_limit ?? cfg.limit;
  if (raw === undefined || raw === null || raw === "") return DEFAULT_VIEW_QUERY_LIMIT;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isFinite(n) || n < 0) return DEFAULT_VIEW_QUERY_LIMIT;
  return Math.floor(n);
}
