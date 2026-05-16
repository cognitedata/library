import YAML from "yaml";

export type ScopeFiltersParts = {
  entityTypesCsv: string;
  otherYaml: string;
};

/**
 * Split scope_filters YAML into comma-separated entity types and remaining keys (YAML).
 */
export function splitScopeFiltersYaml(scopeFiltersYaml: string): ScopeFiltersParts {
  const trimmed = (scopeFiltersYaml ?? "").trim();
  if (!trimmed || trimmed === "{}" || trimmed === "null") {
    return { entityTypesCsv: "", otherYaml: "" };
  }
  try {
    const o = YAML.parse(scopeFiltersYaml) as Record<string, unknown> | null;
    if (!o || typeof o !== "object" || Array.isArray(o)) {
      return { entityTypesCsv: "", otherYaml: trimmed ? trimmed + "\n" : "" };
    }
    const { entity_type: et, ...rest } = o;
    let csv = "";
    if (Array.isArray(et)) csv = et.map((x) => String(x).trim()).filter(Boolean).join(", ");
    else if (typeof et === "string" && et.trim()) csv = et.trim();

    const restKeys = Object.keys(rest);
    const otherYaml =
      restKeys.length > 0 ? YAML.stringify(rest, { lineWidth: 0 }) + "\n" : "";
    return { entityTypesCsv: csv, otherYaml };
  } catch {
    return { entityTypesCsv: "", otherYaml: trimmed ? trimmed + "\n" : "" };
  }
}

/**
 * Merge comma-separated entity types and optional extra scope_filters YAML into one object.
 */
export function mergeScopeFiltersYaml(entityTypesCsv: string, otherYaml: string): string {
  const types = entityTypesCsv
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  let rest: Record<string, unknown> = {};
  const oy = otherYaml.trim();
  if (oy && oy !== "{}" && oy !== "null") {
    try {
      const p = YAML.parse(otherYaml) as unknown;
      if (p && typeof p === "object" && !Array.isArray(p)) rest = { ...(p as Record<string, unknown>) };
    } catch {
      rest = {};
    }
  }

  if (types.length > 0) {
    const merged = { ...rest, entity_type: types };
    return YAML.stringify(merged, { lineWidth: 0 }) + "\n";
  }

  const keys = Object.keys(rest);
  if (keys.length === 0) return "{}\n";
  return YAML.stringify(rest, { lineWidth: 0 }) + "\n";
}

/** For entity bucket sidebar / matching — canonical YAML string from parts. */
export function scopeFiltersYamlFromParts(parts: ScopeFiltersParts): string {
  return mergeScopeFiltersYaml(parts.entityTypesCsv, parts.otherYaml);
}
