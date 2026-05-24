export type PropertyValueKind = "null" | "primitive" | "object" | "array";

export type PropertyChild = {
  key: string;
  path: string;
  value: unknown;
};

const DEFAULT_PREFERRED_KEYS = [
  "kind",
  "instance_kind",
  "id",
  "label",
  "name",
  "external_id",
  "externalId",
  "space",
  "version",
  "type",
  "domain",
  "entity",
  "description",
  "query_hint",
  "queryable",
  "usedFor",
  "properties",
  "indexes",
  "constraints",
  "start_node",
  "end_node",
  "created_time",
  "last_updated_time",
  "open_target",
];

export function sortPropertyKeys(keys: string[], preferredKeys?: string[]): string[] {
  const pref = preferredKeys ?? DEFAULT_PREFERRED_KEYS;
  const prefSet = new Set(pref);
  const prefOrdered = pref.filter((k) => keys.includes(k));
  const rest = keys.filter((k) => !prefSet.has(k)).sort((a, b) => a.localeCompare(b));
  return [...prefOrdered, ...rest];
}

export function propertyValueKind(value: unknown): PropertyValueKind {
  if (value === null || value === undefined) return "null";
  if (Array.isArray(value)) return "array";
  if (typeof value === "object") return "object";
  return "primitive";
}

export function childPropertyEntries(
  value: unknown,
  path: string,
  preferredKeys?: string[]
): PropertyChild[] {
  if (Array.isArray(value)) {
    return value.map((entry, index) => ({
      key: `[${index}]`,
      path: `${path}[${index}]`,
      value: entry,
    }));
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return sortPropertyKeys(Object.keys(record), preferredKeys).map((key) => ({
      key,
      path: path ? `${path}.${key}` : key,
      value: record[key],
    }));
  }
  return [];
}

export function childCount(value: unknown): number {
  if (Array.isArray(value)) return value.length;
  if (value && typeof value === "object") return Object.keys(value as object).length;
  return 0;
}

export function normalizePropertyPayload(value: unknown, preferredKeys?: string[]): Record<string, unknown> {
  if (value === null || value === undefined) return {};
  if (typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const ordered = sortPropertyKeys(Object.keys(record), preferredKeys);
    const out: Record<string, unknown> = {};
    for (const key of ordered) out[key] = record[key];
    return out;
  }
  return { value };
}
