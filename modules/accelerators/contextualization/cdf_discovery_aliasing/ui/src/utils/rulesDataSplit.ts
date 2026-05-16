import type { JsonObject } from "../types/scopeConfig";

export const EXTRACTION_RULES_KEY = "extraction_rules";
export const ALIASING_RULES_KEY = "aliasing_rules";

/** Remove `key` from a copy of `data` and return its array value (or []). */
export function splitRulesList(data: JsonObject, key: string): { rest: JsonObject; list: unknown[] } {
  const raw = data[key];
  const rest = { ...data };
  delete rest[key];
  return { rest, list: Array.isArray(raw) ? raw : [] };
}

export function mergeRulesList(rest: JsonObject, key: string, list: unknown[]): JsonObject {
  return { ...rest, [key]: list };
}
