/**
 * CDF filter DSL on canvas nodes and scope rows — same ``filters`` array shape as ``query_view``.
 */
import type { JsonObject } from "../types/scopeConfig";
import { emptyLeaf } from "../components/SourceViewFiltersEditor";

/** Read ``filters`` from config (query_view, filter node, etc.). */
export function readFilters(cfg: Record<string, unknown>): JsonObject[] {
  const raw = cfg.filters;
  if (Array.isArray(raw) && raw.length > 0) {
    return raw.filter((x) => x && typeof x === "object" && !Array.isArray(x)) as JsonObject[];
  }
  return [];
}

/** @deprecated Use ``readFilters``. */
export const readCohortFilters = readFilters;

export function mergeFilters(cfg: Record<string, unknown>, filters: JsonObject[]): Record<string, unknown> {
  const next = { ...cfg };
  if (!filters.length) {
    delete next.filters;
    return next;
  }
  next.filters = filters;
  delete next.persistence_filters;
  delete next.row_match;
  return next;
}

/** @deprecated Use ``mergeFilters``. */
export const mergeCohortFilters = mergeFilters;

export function defaultFilterNodeFilters(): JsonObject[] {
  return [
    {
      ...emptyLeaf(),
      operator: "EXISTS",
      target_property: "aliases",
      property_scope: "view",
    },
  ];
}
