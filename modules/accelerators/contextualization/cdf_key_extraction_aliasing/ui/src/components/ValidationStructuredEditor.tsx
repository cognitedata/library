import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import {
  defaultMatchRuleDefinition,
  parseExpressionMatch,
  parseMatchRuleDefinitionsArray,
  serializeMatchRuleDefinitionsArray,
  type MatchRuleDefinition,
} from "../utils/confidenceMatchRuleDefModel";
import { focusTargetDomId } from "../utils/focusTargetDomId";
import { reorderListAtIndex } from "../utils/ruleListReorder";
import { MatchRuleDefinitionCard } from "./MatchRuleDefinitionCard";
import { MatchValidationRefsEditor } from "./MatchValidationRefsEditor";

export type ValidationEditorVariant = "keyExtraction" | "aliasing";

type Props = {
  variant: ValidationEditorVariant;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  /** Full scope YAML for resolving rule definitions / sequences (Key Discovery / Aliasing / Source views). */
  scopeDocument?: Record<string, unknown>;
  /** Scroll to this rule: inline list matches `name`; scope-refs mode matches definition id on wiring steps. */
  initialFocusedMatchRuleName?: string;
};

const KNOWN_KEY_EXTRACTION = new Set([
  "min_confidence",
  "max_keys_per_type",
  "expression_match",
  "validation_rules",
]);

const KNOWN_ALIASING = new Set([
  "max_aliases_per_tag",
  "min_confidence",
  "expression_match",
  "validation_rules",
]);

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

export function ValidationStructuredEditor({
  variant,
  value,
  onChange,
  scopeDocument,
  initialFocusedMatchRuleName,
}: Props) {
  const { t } = useAppSettings();
  const known = variant === "keyExtraction" ? KNOWN_KEY_EXTRACTION : KNOWN_ALIASING;
  const lastWrittenFingerprintRef = useRef<string | null>(null);
  const lastAppliedMatchFocusRef = useRef<string | null>(null);
  const [dragRuleFrom, setDragRuleFrom] = useState<number | null>(null);
  const [dragRuleOver, setDragRuleOver] = useState<number | null>(null);

  const useScopeRefs = Boolean(scopeDocument);

  const ui = useMemo(() => {
    const rules = parseMatchRuleDefinitionsArray(value.validation_rules);
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

  useEffect(() => {
    const target = initialFocusedMatchRuleName?.trim();
    if (!target) return;
    if (lastAppliedMatchFocusRef.current === target) return;

    if (useScopeRefs) {
      requestAnimationFrame(() => {
        const nodes = document.querySelectorAll("[data-kea-match-ref-rule]");
        for (const node of nodes) {
          if (node instanceof HTMLElement && node.getAttribute("data-kea-match-ref-rule") === target) {
            node.scrollIntoView({ block: "nearest", behavior: "smooth" });
            lastAppliedMatchFocusRef.current = target;
            break;
          }
        }
      });
      return;
    }

    if (!ui.rules.some((r) => r.name === target)) return;
    lastAppliedMatchFocusRef.current = target;
    requestAnimationFrame(() => {
      document.getElementById(focusTargetDomId("kea-val-match", target))?.scrollIntoView({
        block: "nearest",
        behavior: "smooth",
      });
    });
  }, [initialFocusedMatchRuleName, ui.rules, useScopeRefs, value.validation_rules]);

  const commitValue = (next: JsonObject) => {
    lastWrittenFingerprintRef.current = JSON.stringify(next);
    onChange(next);
  };

  const push = (patch: Partial<JsonObject> & { validation_rules?: unknown[] }) => {
    commitValue({ ...value, ...patch });
  };

  const updateRules = (nextRules: MatchRuleDefinition[]) => {
    commitValue({ ...value, validation_rules: serializeMatchRuleDefinitionsArray(nextRules) as unknown[] });
  };

  useEffect(() => {
    const fp = JSON.stringify(value);
    if (lastWrittenFingerprintRef.current !== null && fp === lastWrittenFingerprintRef.current) {
      lastWrittenFingerprintRef.current = null;
      return;
    }
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
              const v = e.target.value;
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

      {useScopeRefs && scopeDocument ? (
        <>
          <p className="kea-hint">{t("validationEditor.scopeRefsModeHint")}</p>
          <MatchValidationRefsEditor value={value} onChange={commitValue} scopeDocument={scopeDocument} />
        </>
      ) : (
        <>
          <p className="kea-hint">{t("validationEditor.inlineRulesModeHint")}</p>
          <p className="kea-hint">{t("validationEditor.rulesHint")}</p>
          <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
            {t("rulesEntity.dragReorderRules")}
          </p>

          <div className="kea-validation-rules">
            {ui.rules.map((rule, idx) => {
              const dropActive = dragRuleOver === idx;
              const cardClass = [
                "kea-validation-rule",
                dropActive ? "kea-validation-rule--drop" : "",
                dragRuleFrom === idx ? "kea-validation-rule--dragging" : "",
              ]
                .filter(Boolean)
                .join(" ");
              return (
                <div
                  key={idx}
                  id={focusTargetDomId("kea-val-match", rule.name)}
                  className={cardClass}
                  style={{ marginBottom: "0.75rem" }}
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
                  <MatchRuleDefinitionCard
                    rule={rule}
                    ruleIndex={idx}
                    defaultExpanded
                    showCollapsedSummary
                    dragProps={{
                      draggable: true,
                      onDragStart: (e: DragEvent<HTMLSpanElement>) => {
                        e.dataTransfer.setData("text/plain", String(idx));
                        e.dataTransfer.effectAllowed = "move";
                        setDragRuleFrom(idx);
                      },
                      onDragEnd: () => {
                        setDragRuleFrom(null);
                        setDragRuleOver(null);
                      },
                    }}
                    onChange={(r) => {
                      const next = [...ui.rules];
                      next[idx] = r;
                      updateRules(next);
                    }}
                  />
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "flex-end",
                      gap: "0.25rem",
                      marginTop: "0.35rem",
                    }}
                  >
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
                      onClick={() => updateRules(ui.rules.filter((_, i) => i !== idx))}
                    >
                      {t("validationEditor.rule.remove")}
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
          <button
            type="button"
            className="kea-btn kea-btn--sm"
            style={{ marginTop: "0.25rem" }}
            onClick={() => {
              const nr = defaultMatchRuleDefinition(ui.rules);
              updateRules([...ui.rules, nr]);
            }}
          >
            {t("validationEditor.rule.addRule")}
          </button>
        </>
      )}
    </div>
  );
}
