import type { JsonObject } from "../types/scopeConfig";

/**
 * Normalized id → rule body for `validation_rule_definitions`
 * (mapping or list-of-objects), aligned with backend `_definitions_as_lookup`.
 */
export function confidenceMatchDefinitionsAsMap(doc: Record<string, unknown>): Record<string, JsonObject> {
  const raw = doc.validation_rule_definitions;
  const out: Record<string, JsonObject> = {};
  if (raw !== null && typeof raw === "object" && !Array.isArray(raw)) {
    for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
      if (v === null || typeof v !== "object" || Array.isArray(v)) continue;
      const o = { ...(v as JsonObject) };
      const key = String(k).trim();
      if (!key) continue;
      const nm = o.name;
      if (nm == null || !String(nm).trim()) {
        o.name = key;
      }
      const name = String(o.name ?? key).trim();
      if (name) out[name] = o;
    }
    return out;
  }
  if (Array.isArray(raw)) {
    for (const v of raw) {
      if (v === null || typeof v !== "object" || Array.isArray(v)) continue;
      const o = { ...(v as JsonObject) };
      const name = String(o.name ?? "").trim();
      if (name) out[name] = o;
    }
  }
  return out;
}

/**
 * List definition ids from top-level `validation_rule_definitions`
 * (mapping or list-of-objects form), sorted for stable palette order.
 * Ids match runtime lookup (resolved `name`, defaulting map key when `name` is absent).
 */
export function confidenceMatchDefinitionIds(scopeDoc: Record<string, unknown>): string[] {
  return Object.keys(confidenceMatchDefinitionsAsMap(scopeDoc)).sort();
}
