import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { commaJoinSegments, splitCommaSegments } from "../utils/commaDelimited";
import { reorderListAtIndex } from "../utils/ruleListReorder";

export type ValidationEditorVariant = "keyExtraction" | "aliasing";

type Props = {
  variant: ValidationEditorVariant;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

const KNOWN_KEY_EXTRACTION = new Set([
  "min_confidence",
  "max_keys_per_type",
  "expression_match",
  "confidence_match_rules",
]);

const KNOWN_ALIASING = new Set([
  "max_aliases_per_tag",
  "min_confidence",
  "expression_match",
  "confidence_match_rules",
]);

type ExpressionMatchOpt = "" | "search" | "fullmatch";
type ModMode = "explicit" | "offset";

type UiRule = {
  name: string;
  enabled: boolean;
  priority: string;
  expressionMatch: ExpressionMatchOpt;
  keywordsText: string;
  expressions: Array<{ pattern: string; description: string }>;
  modMode: ModMode;
  modValue: string;
};

function extrasFrom(value: JsonObject, known: Set<string>): JsonObject {
  const o: JsonObject = {};
  for (const [k, v] of Object.entries(value)) {
    if (!known.has(k)) o[k] = v;
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

function parseExpressionMatch(v: unknown): ExpressionMatchOpt {
  if (v === "search" || v === "fullmatch") return v;
  return "";
}

function ruleNameOrDefault(raw: string, indexOneBased: number): string {
  const s = raw.trim();
  return s || `rule_${indexOneBased}`;
}

function nextDefaultRuleName(rules: UiRule[]): string {
  const used = new Set(rules.map((r) => r.name.trim().toLowerCase()).filter(Boolean));
  for (let n = 1; n < 10000; n++) {
    const c = `rule_${n}`;
    if (!used.has(c.toLowerCase())) return c;
  }
  return `rule_${Date.now()}`;
}

function parseRules(raw: unknown): UiRule[] {
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

function serializeRules(rules: UiRule[]): unknown[] {
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
    const priority =
      priorityTrim === "" ? undefined : Number(priorityTrim);
    const pr =
      priority !== undefined && !Number.isNaN(priority) ? priority : undefined;

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

function defaultRule(existing: UiRule[]): UiRule {
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

/** First non-empty expression description, else keywords, else first pattern (for collapsed card summary). */
function validationRuleCollapsedSummary(rule: UiRule): string {
  const desc = rule.expressions.map((e) => e.description.trim()).find(Boolean);
  if (desc) return desc;
  const kw = rule.keywordsText.trim();
  if (kw) return kw.length > 200 ? `${kw.slice(0, 200)}…` : kw;
  const pat = rule.expressions.map((e) => e.pattern.trim()).find(Boolean);
  if (pat) return pat.length > 120 ? `${pat.slice(0, 120)}…` : pat;
  return "";
}

export function ValidationStructuredEditor({ variant, value, onChange }: Props) {
  const { t } = useAppSettings();
  const known = variant === "keyExtraction" ? KNOWN_KEY_EXTRACTION : KNOWN_ALIASING;
  const lastWrittenFingerprintRef = useRef<string | null>(null);
  const [ruleCardExpanded, setRuleCardExpanded] = useState<Record<string, boolean>>({});
  const [dragRuleFrom, setDragRuleFrom] = useState<number | null>(null);
  const [dragRuleOver, setDragRuleOver] = useState<number | null>(null);

  const ui = useMemo(() => {
    const rules = parseRules(value.confidence_match_rules);
    return {
      minConfidence: String(numOr(value.min_confidence, variant === "aliasing" ? 0.01 : 0.5)),
      maxKeysPerType:
        variant === "keyExtraction" ? String(numOr(value.max_keys_per_type, 1000)) : "",
      maxAliasesPerTag:
        variant === "aliasing" ? String(numOr(value.max_aliases_per_tag, 50)) : "",
      expressionMatch: parseExpressionMatch(value.expression_match),
      rules,
    };
  }, [value, variant]);

  const extras = useMemo(() => extrasFrom(value, known), [value, known]);

  const commitValue = (next: JsonObject) => {
    lastWrittenFingerprintRef.current = JSON.stringify(next);
    onChange(next);
  };

  const push = (patch: Partial<JsonObject> & { confidence_match_rules?: unknown[] }) => {
    commitValue({ ...value, ...patch });
  };

  const updateRules = (nextRules: UiRule[]) => {
    commitValue({ ...value, confidence_match_rules: serializeRules(nextRules) as unknown[] });
  };

  useEffect(() => {
    const fp = JSON.stringify(value);
    if (lastWrittenFingerprintRef.current !== null && fp === lastWrittenFingerprintRef.current) {
      lastWrittenFingerprintRef.current = null;
      return;
    }
    setRuleCardExpanded({});
  }, [value]);

  return (
    <div className="kea-validation-editor">
      <h4 className="kea-section-title" style={{ fontSize: "0.95rem" }}>
        {t("validationEditor.section.thresholds")}
      </h4>
      <div className="kea-filter-row" style={{ gridTemplateColumns: "repeat(auto-fit,minmax(10rem,1fr))", gap: "0.75rem" }}>
        <label className="kea-label">
          {t("validationEditor.minConfidence")}
          <input
            className="kea-input"
            type="number"
            step="any"
            value={ui.minConfidence}
            onChange={(e) => push({ min_confidence: Number(e.target.value) || 0 })}
          />
        </label>
        {variant === "keyExtraction" && (
          <label className="kea-label">
            {t("validationEditor.maxKeysPerType")}
            <input
              className="kea-input"
              type="number"
              step={1}
              min={1}
              value={ui.maxKeysPerType}
              onChange={(e) => push({ max_keys_per_type: Math.max(1, Math.floor(Number(e.target.value) || 1000)) })}
            />
          </label>
        )}
        {variant === "aliasing" && (
          <label className="kea-label">
            {t("validationEditor.maxAliasesPerTag")}
            <input
              className="kea-input"
              type="number"
              step={1}
              min={1}
              value={ui.maxAliasesPerTag}
              onChange={(e) =>
                push({ max_aliases_per_tag: Math.max(1, Math.floor(Number(e.target.value) || 50)) })
              }
            />
          </label>
        )}
        <label className="kea-label">
          {t("validationEditor.defaultExpressionMatch")}
          <select
            className="kea-input"
            value={ui.expressionMatch}
            onChange={(e) => {
              const v = e.target.value as ExpressionMatchOpt;
              if (v === "") {
                const next = { ...value };
                delete (next as JsonObject).expression_match;
                onChange(next);
              } else {
                push({ expression_match: v });
              }
            }}
          >
            <option value="">{t("validationEditor.expressionMatch.inherit")}</option>
            <option value="search">{t("validationEditor.expressionMatch.search")}</option>
            <option value="fullmatch">{t("validationEditor.expressionMatch.fullmatch")}</option>
          </select>
        </label>
      </div>

      {Object.keys(extras).length > 0 && (
        <p className="kea-hint" style={{ marginTop: "0.75rem" }}>
          {t("validationEditor.extraKeysPreserved", { keys: Object.keys(extras).join(", ") })}
        </p>
      )}

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1.25rem" }}>
        {t("validationEditor.section.rules")}
      </h4>
      <p className="kea-hint">{t("validationEditor.rulesHint")}</p>
      <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
        {t("rulesEntity.dragReorderRules")}
      </p>

      <div className="kea-validation-rules">
        {ui.rules.map((rule, idx) => {
          const isCardExpanded = ruleCardExpanded[rule.name] === true;
          const dropActive = dragRuleOver === idx;
          const cardClass = [
            "kea-validation-rule",
            dropActive ? "kea-validation-rule--drop" : "",
            dragRuleFrom === idx ? "kea-validation-rule--dragging" : "",
          ]
            .filter(Boolean)
            .join(" ");
          const summary = validationRuleCollapsedSummary(rule);
          return (
          <div
            key={`${rule.name}-${idx}`}
            className={cardClass}
            style={{ border: "1px solid var(--kea-border)", borderRadius: "var(--kea-radius-sm)", padding: "0.75rem", marginBottom: "0.75rem", background: "var(--kea-surface)" }}
            onDragOver={(e: DragEvent<HTMLDivElement>) => {
              e.preventDefault();
              e.dataTransfer.dropEffect = "move";
              setDragRuleOver(idx);
            }}
            onDragLeave={(e) => {
              if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
                setDragRuleOver(null);
              }
            }}
            onDrop={(e: DragEvent<HTMLDivElement>) => {
              e.preventDefault();
              const raw = e.dataTransfer.getData("text/plain");
              const from = parseInt(raw, 10);
              if (Number.isNaN(from) || from === idx) {
                setDragRuleFrom(null);
                setDragRuleOver(null);
                return;
              }
              updateRules(reorderListAtIndex(ui.rules, from, idx));
              setDragRuleFrom(null);
              setDragRuleOver(null);
            }}
          >
            <div
              className="kea-filter-row"
              style={{ gridTemplateColumns: "auto auto 1fr auto", gap: "0.5rem", alignItems: "end" }}
            >
              <span
                className="kea-drag-handle"
                draggable
                onDragStart={(e: DragEvent<HTMLSpanElement>) => {
                  e.dataTransfer.setData("text/plain", String(idx));
                  e.dataTransfer.effectAllowed = "move";
                  setDragRuleFrom(idx);
                }}
                onDragEnd={() => {
                  setDragRuleFrom(null);
                  setDragRuleOver(null);
                }}
                aria-label={t("rulesEntity.dragHandle")}
                title={t("rulesEntity.dragHandle")}
              >
                <span className="kea-drag-handle__grip" aria-hidden>
                  ⋮⋮
                </span>
              </span>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                aria-expanded={isCardExpanded}
                aria-label={isCardExpanded ? t("rulesEntity.ruleCollapseDetails") : t("rulesEntity.ruleExpandDetails")}
                onClick={() => setRuleCardExpanded((m) => ({ ...m, [rule.name]: !isCardExpanded }))}
                style={{ minWidth: 36 }}
              >
                <span aria-hidden>{isCardExpanded ? "▼" : "▶"}</span>
              </button>
              <label className="kea-label">
                {t("validationEditor.rule.name")}
                <input
                  className="kea-input"
                  type="text"
                  required
                  aria-required={true}
                  value={rule.name}
                  onChange={(e) => {
                    const newName = e.target.value;
                    setRuleCardExpanded((m) => {
                      if (!(rule.name in m)) return m;
                      const v = m[rule.name]!;
                      const { [rule.name]: _, ...rest } = m;
                      return { ...rest, [newName]: v };
                    });
                    const next = [...ui.rules];
                    next[idx] = { ...rule, name: newName };
                    updateRules(next);
                  }}
                  onBlur={() => {
                    if (!rule.name.trim()) {
                      const newName = ruleNameOrDefault("", idx + 1);
                      setRuleCardExpanded((m) => {
                        if (!(rule.name in m)) return m;
                        const v = m[rule.name]!;
                        const { [rule.name]: _, ...rest } = m;
                        return { ...rest, [newName]: v };
                      });
                      const next = [...ui.rules];
                      next[idx] = { ...rule, name: newName };
                      updateRules(next);
                    }
                  }}
                />
              </label>
              <label
                className="kea-label"
                style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginBottom: 0, whiteSpace: "nowrap" }}
              >
                <input
                  type="checkbox"
                  checked={rule.enabled}
                  onChange={(e) => {
                    const next = [...ui.rules];
                    next[idx] = { ...rule, enabled: e.target.checked };
                    updateRules(next);
                  }}
                />
                {t("validationEditor.rule.enabled")}
              </label>
            </div>
            <p className="kea-hint" style={{ marginTop: "0.5rem", marginBottom: 0 }}>
              {summary || "—"}
            </p>

            {isCardExpanded && (
              <>
            <div
              style={{
                display: "flex",
                justifyContent: "flex-end",
                alignItems: "center",
                flexWrap: "wrap",
                gap: "0.5rem",
                marginTop: "0.5rem",
              }}
            >
              <div style={{ display: "flex", gap: "0.25rem" }}>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  disabled={idx === 0}
                  onClick={() => {
                    const next = [...ui.rules];
                    [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
                    updateRules(next);
                  }}
                  aria-label={t("validationEditor.rule.moveUp")}
                >
                  ↑
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  disabled={idx >= ui.rules.length - 1}
                  onClick={() => {
                    const next = [...ui.rules];
                    [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
                    updateRules(next);
                  }}
                  aria-label={t("validationEditor.rule.moveDown")}
                >
                  ↓
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  onClick={() => {
                    const next = ui.rules.filter((_, i) => i !== idx);
                    updateRules(next);
                  }}
                >
                  {t("validationEditor.rule.remove")}
                </button>
              </div>
            </div>
            <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
              {t("validationEditor.orderSetsPriority")}
            </p>
            <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}>
              <label className="kea-label">
                {t("validationEditor.rule.priority")}
                <input
                  className="kea-input"
                  placeholder={t("validationEditor.rule.priorityPlaceholder")}
                  value={rule.priority}
                  onChange={(e) => {
                    const next = [...ui.rules];
                    next[idx] = { ...rule, priority: e.target.value };
                    updateRules(next);
                  }}
                />
              </label>
              <label className="kea-label">
                {t("validationEditor.rule.expressionMatch")}
                <select
                  className="kea-input"
                  value={rule.expressionMatch}
                  onChange={(e) => {
                    const next = [...ui.rules];
                    next[idx] = { ...rule, expressionMatch: e.target.value as ExpressionMatchOpt };
                    updateRules(next);
                  }}
                >
                  <option value="">{t("validationEditor.expressionMatch.inherit")}</option>
                  <option value="search">{t("validationEditor.expressionMatch.search")}</option>
                  <option value="fullmatch">{t("validationEditor.expressionMatch.fullmatch")}</option>
                </select>
              </label>
            </div>
            <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
              {t("validationEditor.rule.keywords")}
              <input
                className="kea-input"
                type="text"
                value={rule.keywordsText}
                onChange={(e) => {
                  const next = [...ui.rules];
                  next[idx] = { ...rule, keywordsText: e.target.value };
                  updateRules(next);
                }}
                spellCheck={false}
                placeholder={t("validationEditor.rule.keywordsPlaceholder")}
                autoComplete="off"
              />
            </label>
            <div style={{ marginTop: "0.5rem" }}>
              <span className="kea-label" style={{ display: "block", marginBottom: "0.25rem" }}>
                {t("validationEditor.rule.expressions")}
              </span>
              {rule.expressions.map((ex, j) => (
                <div key={j} className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr auto", gap: "0.35rem", marginBottom: "0.35rem" }}>
                  <input
                    className="kea-input"
                    placeholder={t("validationEditor.rule.pattern")}
                    value={ex.pattern}
                    onChange={(e) => {
                      const next = [...ui.rules];
                      const exprs = [...next[idx].expressions];
                      exprs[j] = { ...exprs[j], pattern: e.target.value };
                      next[idx] = { ...next[idx], expressions: exprs };
                      updateRules(next);
                    }}
                  />
                  <input
                    className="kea-input"
                    placeholder={t("validationEditor.rule.description")}
                    value={ex.description}
                    onChange={(e) => {
                      const next = [...ui.rules];
                      const exprs = [...next[idx].expressions];
                      exprs[j] = { ...exprs[j], description: e.target.value };
                      next[idx] = { ...next[idx], expressions: exprs };
                      updateRules(next);
                    }}
                  />
                  <button
                    type="button"
                    className="kea-btn kea-btn--ghost kea-btn--sm"
                    disabled={rule.expressions.length <= 1}
                    onClick={() => {
                      const next = [...ui.rules];
                      const exprs = next[idx].expressions.filter((_, k) => k !== j);
                      next[idx] = { ...next[idx], expressions: exprs.length ? exprs : [{ pattern: "", description: "" }] };
                      updateRules(next);
                    }}
                  >
                    ×
                  </button>
                </div>
              ))}
              <button
                type="button"
                className="kea-btn kea-btn--sm"
                style={{ marginTop: "0.25rem" }}
                onClick={() => {
                  const next = [...ui.rules];
                  next[idx] = {
                    ...next[idx],
                    expressions: [...next[idx].expressions, { pattern: "", description: "" }],
                  };
                  updateRules(next);
                }}
              >
                {t("validationEditor.rule.addExpression")}
              </button>
            </div>
            <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}>
              <label className="kea-label">
                {t("validationEditor.rule.modifierMode")}
                <select
                  className="kea-input"
                  value={rule.modMode}
                  onChange={(e) => {
                    const next = [...ui.rules];
                    next[idx] = { ...rule, modMode: e.target.value as ModMode };
                    updateRules(next);
                  }}
                >
                  <option value="offset">{t("validationEditor.rule.modifierOffset")}</option>
                  <option value="explicit">{t("validationEditor.rule.modifierExplicit")}</option>
                </select>
              </label>
              <label className="kea-label">
                {t("validationEditor.rule.modifierValue")}
                <input
                  className="kea-input"
                  type="number"
                  step="any"
                  value={rule.modValue}
                  onChange={(e) => {
                    const next = [...ui.rules];
                    next[idx] = { ...rule, modValue: e.target.value };
                    updateRules(next);
                  }}
                />
              </label>
            </div>
              </>
            )}
          </div>
          );
        })}
      </div>
      <button
        type="button"
        className="kea-btn kea-btn--sm"
        onClick={() => {
          const nr = defaultRule(ui.rules);
          setRuleCardExpanded((m) => ({ ...m, [nr.name]: true }));
          updateRules([...ui.rules, nr]);
        }}
      >
        {t("validationEditor.rule.addRule")}
      </button>
    </div>
  );
}
