import type { JsonObject } from "../types/jsonConfig";
import { commaJoinSegments, splitCommaSegments } from "./commaDelimited";
import { enrichExpressionDescriptions } from "./scorePatternCatalog";

export type ExpressionMatchOpt = "" | "search" | "fullmatch";
export type ScoreModMode = "explicit" | "offset";

export type ScoringRuleRow = {
  name: string;
  enabled: boolean;
  priority: string;
  expressionMatch: ExpressionMatchOpt;
  keywordsText: string;
  expressions: Array<{ pattern: string; description: string }>;
  modMode: ScoreModMode;
  modValue: string;
  noMatchEnabled: boolean;
  noMatchModMode: ScoreModMode;
  noMatchModValue: string;
};

export function parseExpressionMatch(v: unknown): ExpressionMatchOpt {
  if (v === "search" || v === "fullmatch") return v;
  return "";
}

function ruleNameOrDefault(raw: string, indexOneBased: number): string {
  const s = raw.trim();
  return s || `rule_${indexOneBased}`;
}

export function nextDefaultScoringRuleName(rules: ScoringRuleRow[]): string {
  const used = new Set(rules.map((r) => r.name.trim().toLowerCase()).filter(Boolean));
  for (let n = 1; n < 10000; n++) {
    const c = `rule_${n}`;
    if (!used.has(c.toLowerCase())) return c;
  }
  return `rule_${Date.now()}`;
}

export function readScoringRules(cfg: Record<string, unknown>): unknown[] {
  const raw = cfg.scoring_rules ?? cfg.score_rules ?? cfg.steps;
  return Array.isArray(raw) ? raw : [];
}

export function parseScoringRuleRows(raw: unknown): ScoringRuleRow[] {
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
    const enrichedExpressions = enrichExpressionDescriptions(expressions);

    const sm = rule.score_modifier;
    let modMode: ScoreModMode = "offset";
    let modValue = "0";
    if (sm !== null && typeof sm === "object" && !Array.isArray(sm)) {
      const smo = sm as JsonObject;
      modMode = smo.mode === "explicit" ? "explicit" : "offset";
      modValue = String(smo.value ?? "0");
    }

    const nm = rule.on_no_match;
    let noMatchEnabled = false;
    let noMatchModMode: ScoreModMode = "offset";
    let noMatchModValue = "0";
    if (nm !== null && typeof nm === "object" && !Array.isArray(nm)) {
      const nmo = nm as JsonObject;
      const nsm = nmo.score_modifier;
      if (nsm !== null && typeof nsm === "object" && !Array.isArray(nsm)) {
        const nsmo = nsm as JsonObject;
        noMatchEnabled = true;
        noMatchModMode = nsmo.mode === "explicit" ? "explicit" : "offset";
        noMatchModValue = String(nsmo.value ?? "0");
      }
    }

    return {
      name: ruleNameOrDefault(String(rule.name ?? ""), i + 1),
      enabled: rule.enabled !== false,
      priority: rule.priority === null || rule.priority === undefined ? "" : String(rule.priority),
      expressionMatch: parseExpressionMatch(rule.expression_match),
      keywordsText: commaJoinSegments(keywords),
      expressions: enrichedExpressions,
      modMode,
      modValue,
      noMatchEnabled,
      noMatchModMode,
      noMatchModValue,
    };
  });
}

export function serializeScoringRuleRows(rules: ScoringRuleRow[]): unknown[] {
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
      match: { expressions, keywords },
      score_modifier: {
        mode: r.modMode,
        value: Number(r.modValue) || 0,
      },
    };
    if (pr !== undefined) out.priority = pr;
    if (r.expressionMatch) out.expression_match = r.expressionMatch;
    if (r.noMatchEnabled) {
      out.on_no_match = {
        score_modifier: {
          mode: r.noMatchModMode,
          value: Number(r.noMatchModValue) || 0,
        },
      };
    }
    return out;
  });
}

export function defaultScoringRuleRow(existing: ScoringRuleRow[]): ScoringRuleRow {
  return {
    name: nextDefaultScoringRuleName(existing),
    enabled: true,
    priority: "",
    expressionMatch: "",
    keywordsText: "",
    expressions: [{ pattern: "", description: "" }],
    modMode: "offset",
    modValue: "0",
    noMatchEnabled: false,
    noMatchModMode: "offset",
    noMatchModValue: "0",
  };
}

export function readScoreFields(cfg: Record<string, unknown>): string[] {
  const raw = cfg.score_fields;
  if (Array.isArray(raw)) {
    return raw.map((x) => String(x ?? "").trim()).filter(Boolean);
  }
  const single = String(cfg.score_field ?? "").trim();
  return single ? [single] : [];
}

export function scoreSummary(cfg: Record<string, unknown>): string {
  const fields = readScoreFields(cfg);
  const rules = readScoringRules(cfg);
  const fieldPart = fields.length ? fields.join(", ") : "";
  const rulePart = rules.length ? `${rules.length} rule(s)` : "";
  if (fieldPart && rulePart) return `${fieldPart} · ${rulePart}`;
  return fieldPart || rulePart;
}
