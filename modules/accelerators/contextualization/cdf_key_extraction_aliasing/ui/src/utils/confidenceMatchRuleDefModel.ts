/**
 * Shared model for `confidence_match_rules` inline rule bodies (YAML objects with match, confidence_modifier, …).
 */
import type { JsonObject } from "../types/scopeConfig";
import { commaJoinSegments, splitCommaSegments } from "./commaDelimited";

export type ExpressionMatchOpt = "" | "search" | "fullmatch";
export type ModMode = "explicit" | "offset";

export type MatchRuleDefinition = {
  name: string;
  enabled: boolean;
  priority: string;
  expressionMatch: ExpressionMatchOpt;
  keywordsText: string;
  expressions: Array<{ pattern: string; description: string }>;
  modMode: ModMode;
  modValue: string;
};

export function parseExpressionMatch(v: unknown): ExpressionMatchOpt {
  if (v === "search" || v === "fullmatch") return v;
  return "";
}

export function ruleNameOrDefault(raw: string, indexOneBased: number): string {
  const s = raw.trim();
  return s || `rule_${indexOneBased}`;
}

export function nextDefaultRuleName(rules: MatchRuleDefinition[]): string {
  const used = new Set(rules.map((r) => r.name.trim().toLowerCase()).filter(Boolean));
  for (let n = 1; n < 10000; n++) {
    const c = `rule_${n}`;
    if (!used.has(c.toLowerCase())) return c;
  }
  return `rule_${Date.now()}`;
}

export function parseMatchRuleDefinitionsArray(raw: unknown): MatchRuleDefinition[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((r, i) => {
    const rule = r !== null && typeof r === "object" && !Array.isArray(r) ? (r as JsonObject) : {};
    const match =
      rule.match !== null && typeof rule.match === "object" && !Array.isArray(rule.match)
        ? (rule.match as JsonObject)
        : {};
    const kw = match.keywords;
    const keywords: string[] = Array.isArray(kw) ? kw.map((x) => String(x ?? "")) : [];
    const exRaw = match.expressions;
    const expressions: Array<{ pattern: string; description: string }> = [];
    if (Array.isArray(exRaw)) {
      for (const e of exRaw) {
        if (typeof e === "string") {
          expressions.push({ pattern: e, description: "" });
        } else if (e !== null && typeof e === "object" && !Array.isArray(e)) {
          const eo = e as JsonObject;
          expressions.push({
            pattern: String(eo.pattern ?? ""),
            description: String(eo.description ?? ""),
          });
        }
      }
    }
    if (expressions.length === 0) expressions.push({ pattern: "", description: "" });
    const cm = rule.confidence_modifier;
    let modMode: ModMode = "offset";
    let modValue = "0";
    if (cm !== null && typeof cm === "object" && !Array.isArray(cm)) {
      const cmo = cm as JsonObject;
      modMode = cmo.mode === "explicit" ? "explicit" : "offset";
      modValue = String(cmo.value ?? "0");
    }
    return {
      name: ruleNameOrDefault(String(rule.name ?? ""), i + 1),
      enabled: rule.enabled !== false,
      priority: rule.priority === null || rule.priority === undefined ? "" : String(rule.priority),
      expressionMatch: parseExpressionMatch(rule.expression_match),
      keywordsText: commaJoinSegments(keywords),
      expressions,
      modMode,
      modValue,
    };
  });
}

/** Parse a single inline rule object (e.g. one entry from `confidence_match_rule_definitions`). */
export function parseSingleMatchRuleDefinition(raw: unknown): MatchRuleDefinition {
  const arr = parseMatchRuleDefinitionsArray([raw]);
  return arr[0] ?? defaultMatchRuleDefinition([]);
}

export function serializeMatchRuleDefinitionsArray(rules: MatchRuleDefinition[]): unknown[] {
  return rules.map((r, ruleIdx) => {
    const keywords = splitCommaSegments(r.keywordsText);
    const expressions = r.expressions
      .map((e) => ({
        pattern: e.pattern.trim(),
        description: e.description.trim() || undefined,
      }))
      .filter((e) => e.pattern.length > 0)
      .map((e) => (e.description ? { pattern: e.pattern, description: e.description } : { pattern: e.pattern }));

    const priorityTrim = r.priority.trim();
    const priority = priorityTrim === "" ? undefined : Number(priorityTrim);
    const pr = priority !== undefined && !Number.isNaN(priority) ? priority : undefined;

    const out: JsonObject = {
      name: ruleNameOrDefault(r.name, ruleIdx + 1),
      enabled: r.enabled,
      match: {
        expressions,
        keywords,
      },
      confidence_modifier: {
        mode: r.modMode,
        value: Number(r.modValue) || 0,
      },
    };
    if (pr !== undefined) out.priority = pr;
    if (r.expressionMatch) out.expression_match = r.expressionMatch;
    return out;
  });
}

export function serializeSingleMatchRuleDefinition(r: MatchRuleDefinition, indexOneBased: number): JsonObject {
  const [one] = serializeMatchRuleDefinitionsArray([{ ...r, name: r.name || `rule_${indexOneBased}` }]);
  return (one !== null && typeof one === "object" && !Array.isArray(one) ? one : {}) as JsonObject;
}

export function defaultMatchRuleDefinition(existing: MatchRuleDefinition[]): MatchRuleDefinition {
  return {
    name: nextDefaultRuleName(existing),
    enabled: true,
    priority: "",
    expressionMatch: "",
    keywordsText: "",
    expressions: [{ pattern: "", description: "" }],
    modMode: "offset",
    modValue: "0",
  };
}

export function validationRuleCollapsedSummary(rule: MatchRuleDefinition): string {
  const desc = rule.expressions.map((e) => e.description.trim()).find(Boolean);
  if (desc) return desc;
  const kw = rule.keywordsText.trim();
  if (kw) return kw.length > 200 ? `${kw.slice(0, 200)}…` : kw;
  const pat = rule.expressions.map((e) => e.pattern.trim()).find(Boolean);
  if (pat) return pat.length > 120 ? `${pat.slice(0, 120)}…` : pat;
  return "";
}
