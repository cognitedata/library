import type { JsonObject } from "../../types/scopeConfig";
import { confidenceMatchDefinitionsAsMap } from "../../utils/confidenceMatchDefinitionIds";
import { sanitizeRuleNamePrefix } from "../../utils/ruleNaming";
import {
  defaultMatchRuleDefinition,
  serializeSingleMatchRuleDefinition,
} from "../../utils/confidenceMatchRuleDefModel";

function serializedStubForId(id: string): JsonObject {
  const blank = defaultMatchRuleDefinition([]);
  blank.name = id;
  return serializeSingleMatchRuleDefinition(blank, 1);
}

/**
 * Next unique id of the form `{namePrefix}_n` not present as a key in `validation_rule_definitions`.
 * Default prefix `rule` yields `rule_1`, `rule_2`, …
 */
export function nextUniqueMatchDefinitionId(
  doc: Record<string, unknown>,
  namePrefix: string = "rule"
): string {
  const base = sanitizeRuleNamePrefix(namePrefix, "rule");
  const defMap = confidenceMatchDefinitionsAsMap(doc);
  const existing = new Set(
    Object.keys(defMap)
      .map((k) => k.trim().toLowerCase())
      .filter(Boolean)
  );
  for (let n = 1; n < 10000; n++) {
    const c = `${base}_${n}`;
    if (!existing.has(c.toLowerCase())) return c;
  }
  return `${base}_${Date.now()}`;
}

/**
 * Ensure `validation_rule_definitions[id]` exists with a minimal editable stub.
 * If a definition for `id` already exists, returns `doc` unchanged.
 */
export function ensureConfidenceMatchRuleDefinitionStub(
  doc: Record<string, unknown>,
  id: string
): Record<string, unknown> {
  const trimmed = id.trim();
  if (!trimmed) return doc;
  const defMap = confidenceMatchDefinitionsAsMap(doc);
  if (defMap[trimmed]) return doc;
  return {
    ...doc,
    validation_rule_definitions: {
      ...defMap,
      [trimmed]: serializedStubForId(trimmed),
    },
  };
}

/**
 * Append a new stub with a generated unique id. Names start with `namePrefix` (default `rule`).
 */
export function appendUniqueMatchDefinitionStub(
  doc: Record<string, unknown>,
  namePrefix: string = "rule"
): {
  doc: Record<string, unknown>;
  newId: string;
} {
  const newId = nextUniqueMatchDefinitionId(doc, namePrefix);
  return {
    doc: ensureConfidenceMatchRuleDefinitionStub(doc, newId),
    newId,
  };
}
