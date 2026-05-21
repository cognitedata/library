import type { JsonObject } from "../types/scopeConfig";
import {
  defaultMatchRuleDefinition,
  parseExpressionMatch,
  parseMatchRuleDefinitionsArray,
  parseSingleMatchRuleDefinition,
  serializeSingleMatchRuleDefinition,
  type ExpressionMatchOpt,
  type MatchRuleDefinition,
} from "./confidenceMatchRuleDefModel";
import { parseExecutionMode, parseStepsArray, serializeExecution, serializeSteps } from "./pipelineStepsModel";

const KNOWN_TOP_LEVEL = new Set([
  "description",
  "min_confidence",
  "expression_match",
  "execution",
  "steps",
  "validation_rule_definitions",
  "validation_rules",
  "max_keys_per_type",
  "max_aliases_per_tag",
  "validate_fields",
  "initial_confidence",
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
  executionMode: ReturnType<typeof parseExecutionMode>;
  steps: MatchRuleDefinition[];
  extras: JsonObject;
};

function migrateLegacyToSteps(value: JsonObject): MatchRuleDefinition[] {
  const fromSteps = parseStepsArray(value);
  if (fromSteps.length > 0) {
    return fromSteps.map((s) => parseSingleMatchRuleDefinition(s));
  }
  const migrated: MatchRuleDefinition[] = [];
  const defs = value.validation_rule_definitions;
  if (defs && typeof defs === "object" && !Array.isArray(defs)) {
    for (const [key, body] of Object.entries(defs as Record<string, unknown>)) {
      if (body && typeof body === "object" && !Array.isArray(body)) {
        const rule = parseSingleMatchRuleDefinition(body);
        if (!rule.name.trim()) rule.name = String(key).trim();
        migrated.push(rule);
      }
    }
  }
  const inline = parseMatchRuleDefinitionsArray(value.validation_rules);
  migrated.push(...inline);
  return migrated;
}

export function parseValidationNodeConfig(value: JsonObject): ParsedValidationNodeConfig {
  return {
    description: String(value.description ?? "").trim(),
    minConfidence: String(numOr(value.min_confidence, 0.5)),
    expressionMatch: parseExpressionMatch(value.expression_match),
    executionMode: parseExecutionMode(value),
    steps: migrateLegacyToSteps(value),
    extras: extrasFrom(value),
  };
}

export function serializeValidationNodeConfig(parts: {
  description: string;
  minConfidence: string;
  expressionMatch: ExpressionMatchOpt;
  executionMode: ReturnType<typeof parseExecutionMode>;
  steps: MatchRuleDefinition[];
  extras: JsonObject;
}): JsonObject {
  const out: JsonObject = { ...parts.extras, ...serializeExecution(parts.executionMode) };
  const desc = parts.description.trim();
  if (desc) out.description = desc;

  const min = Number(parts.minConfidence);
  if (!Number.isNaN(min)) out.min_confidence = min;

  if (parts.expressionMatch) {
    out.expression_match = parts.expressionMatch;
  }

  if (parts.steps.length > 0) {
    Object.assign(
      out,
      serializeSteps(
        parts.steps.map((r, i) =>
          serializeSingleMatchRuleDefinition(r, i + 1)
        ) as JsonObject[]
      )
    );
  }

  return out;
}

export function defaultValidationStep(existing: MatchRuleDefinition[]): MatchRuleDefinition {
  return defaultMatchRuleDefinition(existing);
}
