import type { JsonObject } from "../types/scopeConfig";
import {
  defaultMatchRuleDefinition,
  parseExpressionMatch,
  parseMatchRuleDefinitionsArray,
  parseSingleMatchRuleDefinition,
  serializeMatchRuleDefinitionsArray,
  serializeSingleMatchRuleDefinition,
  type ExpressionMatchOpt,
  type MatchRuleDefinition,
} from "./confidenceMatchRuleDefModel";

export type ValidationDefinitionEntry = {
  id: string;
  rule: MatchRuleDefinition;
};

const KNOWN_TOP_LEVEL = new Set([
  "description",
  "min_confidence",
  "expression_match",
  "validation_rule_definitions",
  "validation_rules",
]);

function extrasFrom(value: JsonObject): JsonObject {
  const o: JsonObject = {};
  for (const [k, v] of Object.entries(value)) {
    if (!KNOWN_TOP_LEVEL.has(k)) o[k] = v;
  }
  return o;
}

function numOr(v: unknown, fallback: number): number {
  if (typeof v === "number" && !Number.isNaN(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (!Number.isNaN(n)) return n;
  }
  return fallback;
}

export type ParsedValidationNodeConfig = {
  description: string;
  minConfidence: string;
  expressionMatch: ExpressionMatchOpt;
  definitionEntries: ValidationDefinitionEntry[];
  inlineRules: MatchRuleDefinition[];
  extras: JsonObject;
};

function definitionEntriesFromConfig(value: JsonObject): ValidationDefinitionEntry[] {
  const raw = value.validation_rule_definitions;
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return [];
  const entries: ValidationDefinitionEntry[] = [];
  for (const [key, v] of Object.entries(raw as Record<string, unknown>)) {
    if (v === null || typeof v !== "object" || Array.isArray(v)) continue;
    const id = String(key).trim();
    if (!id) continue;
    entries.push({ id, rule: parseSingleMatchRuleDefinition(v) });
  }
  return entries.sort((a, b) => a.id.localeCompare(b.id));
}

export function parseValidationNodeConfig(value: JsonObject): ParsedValidationNodeConfig {
  const definitionEntries = definitionEntriesFromConfig(value);
  return {
    description: String(value.description ?? "").trim(),
    minConfidence: String(numOr(value.min_confidence, 0.5)),
    expressionMatch: parseExpressionMatch(value.expression_match),
    definitionEntries,
    inlineRules: parseMatchRuleDefinitionsArray(value.validation_rules),
    extras: extrasFrom(value),
  };
}

export function serializeValidationNodeConfig(parts: {
  description: string;
  minConfidence: string;
  expressionMatch: ExpressionMatchOpt;
  definitionEntries: ValidationDefinitionEntry[];
  inlineRules: MatchRuleDefinition[];
  extras: JsonObject;
}): JsonObject {
  const out: JsonObject = { ...parts.extras };
  const desc = parts.description.trim();
  if (desc) out.description = desc;

  const min = Number(parts.minConfidence);
  if (!Number.isNaN(min)) out.min_confidence = min;

  if (parts.expressionMatch) {
    out.expression_match = parts.expressionMatch;
  }

  if (parts.definitionEntries.length > 0) {
    const defs: Record<string, JsonObject> = {};
    for (const entry of parts.definitionEntries) {
      const id = entry.id.trim() || entry.rule.name.trim() || "rule_1";
      defs[id] = serializeSingleMatchRuleDefinition(
        { ...entry.rule, name: entry.rule.name.trim() || id },
        1
      );
    }
    out.validation_rule_definitions = defs;
  }

  if (parts.inlineRules.length > 0) {
    out.validation_rules = serializeMatchRuleDefinitionsArray(parts.inlineRules) as unknown[];
  }

  return out;
}

export function defaultValidationDefinitionEntry(existingIds: string[]): ValidationDefinitionEntry {
  const rule = defaultMatchRuleDefinition([]);
  let id = rule.name;
  const used = new Set(existingIds.map((x) => x.toLowerCase()));
  for (let n = 1; n < 10000; n++) {
    const c = `validation_rule_${n}`;
    if (!used.has(c.toLowerCase())) {
      id = c;
      break;
    }
  }
  rule.name = id;
  return { id, rule };
}
