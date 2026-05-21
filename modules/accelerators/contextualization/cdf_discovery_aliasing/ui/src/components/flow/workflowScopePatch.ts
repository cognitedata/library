import type { JsonObject } from "../../types/scopeConfig";

/** Update `source_views[i].filters` in the workflow scope document. */
export function patchSourceViewFilters(
  doc: Record<string, unknown>,
  index: number,
  filters: JsonObject[]
): Record<string, unknown> {
  const svs = doc.source_views;
  if (!Array.isArray(svs) || index < 0 || index >= svs.length) return doc;
  const nextSvs = svs.map((v, i) => {
    if (i !== index) return v;
    if (!v || typeof v !== "object" || Array.isArray(v)) return { filters };
    return { ...(v as JsonObject), filters };
  });
  return { ...doc, source_views: nextSvs };
}
