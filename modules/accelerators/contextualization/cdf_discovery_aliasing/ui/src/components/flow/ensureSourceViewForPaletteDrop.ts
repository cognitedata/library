import type { JsonObject } from "../../types/scopeConfig";

/** Same empty row shape as ``SourceViewsControls`` / scope YAML authoring. */
export function emptySourceViewRow(): JsonObject {
  return {
    view_external_id: "",
    view_space: "",
    view_version: "",
    filters: [],
    include_properties: [],
  };
}

function asSourceViewList(doc: Record<string, unknown>): JsonObject[] {
  const raw = doc.source_views;
  if (!Array.isArray(raw)) return [];
  return raw.filter((x): x is JsonObject => x !== null && typeof x === "object" && !Array.isArray(x));
}

/**
 * Append ``source_views[]`` with a blank row (palette drop onto canvas).
 * Returns the new zero-based index.
 */
export function appendEmptySourceView(doc: Record<string, unknown>): {
  doc: Record<string, unknown>;
  index: number;
} {
  const next = [...asSourceViewList(doc), emptySourceViewRow()];
  return { doc: { ...doc, source_views: next }, index: next.length - 1 };
}
