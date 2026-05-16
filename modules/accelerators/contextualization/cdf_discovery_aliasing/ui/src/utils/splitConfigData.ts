import type { JsonObject } from "../types/scopeConfig";

const VALIDATION_KEY = "validation";

/** Split `config.data` into rule bodies vs top-level `validation` (engine merges both). */
export function splitDataByValidation(data: JsonObject): {
  withoutValidation: JsonObject;
  validation: JsonObject;
} {
  const raw = data[VALIDATION_KEY];
  const rest = { ...data };
  delete rest[VALIDATION_KEY];
  const validation =
    raw !== null && typeof raw === "object" && !Array.isArray(raw) ? (raw as JsonObject) : {};
  return { withoutValidation: rest, validation };
}

export function mergeDataWithValidation(withoutValidation: JsonObject, validation: JsonObject): JsonObject {
  return { ...withoutValidation, [VALIDATION_KEY]: validation };
}
