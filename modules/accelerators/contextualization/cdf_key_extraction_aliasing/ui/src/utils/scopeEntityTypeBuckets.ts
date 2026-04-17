import YAML from "yaml";

/** Bucket id: rules with no `entity_type` in scope_filters (empty / missing). */
export const ENTITY_BUCKET_UNSCOPED = "__unscoped__";
/** Bucket id: show every rule (for reordering across the full list). */
export const ENTITY_BUCKET_ALL = "__all__";

const STANDARD_ENTITY_TYPES = ["asset", "file", "timeseries"] as const;

/**
 * Parse `entity_type` from scope_filters YAML (string or string[]).
 */
export function parseEntityTypesFromScopeFiltersYaml(scopeFiltersYaml: string): string[] {
  const trimmed = scopeFiltersYaml.trim();
  if (!trimmed || trimmed === "{}" || trimmed === "null") return [];
  try {
    const o = YAML.parse(scopeFiltersYaml) as Record<string, unknown> | null;
    if (!o || typeof o !== "object" || Array.isArray(o)) return [];
    const et = o.entity_type;
    if (Array.isArray(et)) {
      return et.map((x) => String(x).trim()).filter(Boolean);
    }
    if (typeof et === "string" && et.trim()) return [et.trim()];
    return [];
  } catch {
    return [];
  }
}

export function ruleMatchesEntityBucket(scopeFiltersYaml: string, bucket: string): boolean {
  if (bucket === ENTITY_BUCKET_ALL) return true;
  const types = parseEntityTypesFromScopeFiltersYaml(scopeFiltersYaml);
  if (bucket === ENTITY_BUCKET_UNSCOPED) return types.length === 0;
  return types.includes(bucket);
}

export function scopeFiltersYamlForEntityType(entityType: string): string {
  return `entity_type:\n  - ${entityType}\n`;
}

/** Empty scope_filters object — applies without entity_type constraint. */
export const EMPTY_SCOPE_FILTERS_YAML = "{}\n";

export function defaultScopeFiltersYamlForBucket(bucket: string, fallbackEntity: string): string {
  if (bucket === ENTITY_BUCKET_UNSCOPED) return EMPTY_SCOPE_FILTERS_YAML;
  if (bucket === ENTITY_BUCKET_ALL) return scopeFiltersYamlForEntityType(fallbackEntity);
  return scopeFiltersYamlForEntityType(bucket);
}

export type EntityBucketRow = { id: string; count: number };

/**
 * Build sidebar rows: standard types (with counts), extra types sorted, unscoped, then "all".
 */
export function buildEntityBucketSidebar(
  scopeYamlList: string[],
  totalRules: number
): EntityBucketRow[] {
  const counts = new Map<string, number>();
  let unscoped = 0;
  for (const yaml of scopeYamlList) {
    const types = parseEntityTypesFromScopeFiltersYaml(yaml);
    if (types.length === 0) unscoped += 1;
    else for (const t of types) counts.set(t, (counts.get(t) ?? 0) + 1);
  }

  const extra = [...counts.keys()]
    .filter((t) => !STANDARD_ENTITY_TYPES.includes(t as (typeof STANDARD_ENTITY_TYPES)[number]))
    .sort();

  const rows: EntityBucketRow[] = [];
  for (const id of STANDARD_ENTITY_TYPES) {
    rows.push({ id, count: counts.get(id) ?? 0 });
  }
  for (const id of extra) {
    rows.push({ id, count: counts.get(id) ?? 0 });
  }
  rows.push({ id: ENTITY_BUCKET_UNSCOPED, count: unscoped });
  rows.push({ id: ENTITY_BUCKET_ALL, count: totalRules });
  return rows;
}

/** First bucket that has at least one rule, else `asset`. */
export function pickInitialEntityBucket(rows: EntityBucketRow[]): string {
  const withRules = rows.find((r) => r.count > 0 && r.id !== ENTITY_BUCKET_ALL);
  return withRules?.id ?? "asset";
}
