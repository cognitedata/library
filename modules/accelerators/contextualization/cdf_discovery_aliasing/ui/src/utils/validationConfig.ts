import type { JsonObject } from "../types/scopeConfig";

/** Key Discovery validation no longer supports deprecated `regexp_match`; drop it on save. */
export function withoutRegexpMatch(validation: JsonObject): JsonObject {
  const v = { ...validation };
  delete (v as JsonObject).regexp_match;
  return v;
}
