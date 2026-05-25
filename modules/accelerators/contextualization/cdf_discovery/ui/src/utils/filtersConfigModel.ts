import type { JsonObject } from "../types/jsonConfig";

export function readFilters(cfg: Record<string, unknown>): JsonObject[] {
  const raw = cfg.filters;
  if (Array.isArray(raw) && raw.length > 0) {
    return raw.filter((x) => x && typeof x === "object" && !Array.isArray(x)) as JsonObject[];
  }
  return [];
}

export function mergeFilters(cfg: Record<string, unknown>, filters: JsonObject[]): Record<string, unknown> {
  const next = { ...cfg };
  if (!filters.length) {
    delete next.filters;
    return next;
  }
  next.filters = filters;
  return next;
}
