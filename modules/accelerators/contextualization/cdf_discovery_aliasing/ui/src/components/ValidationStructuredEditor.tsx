import { useEffect, useMemo, useRef, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { parseExpressionMatch } from "../utils/confidenceMatchRuleDefModel";
import { focusTargetDomId } from "../utils/focusTargetDomId";
import {
  defaultValidationStep,
  parseValidationNodeConfig,
  serializeValidationNodeConfig,
} from "../utils/validationNodeConfigModel";
import { MatchValidationRefsEditor } from "./MatchValidationRefsEditor";
import { PipelineExecutionFields } from "./PipelineExecutionFields";
import { ValidationStepsList } from "./ValidationStepsList";

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
  "execution",
  "steps",
  "validation_rules",
  "validation_rule_definitions",
]);

const KNOWN_ALIASING = new Set([
  "max_aliases_per_tag",
  "min_confidence",
  "expression_match",
  "execution",
  "steps",
  "validation_rules",
  "validation_rule_definitions",
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
  const useScopeRefs = Boolean(scopeDocument);

  const ui = useMemo(() => {
    const parsed = parseValidationNodeConfig(value);
    return {
      minConfidence: String(numOr(value.min_confidence, variant === "aliasing" ? 0.01 : 0.5)),
      maxKeysPerType:
        variant === "keyExtraction" ? String(numOr(value.max_keys_per_type, 1000)) : "",
      maxAliasesPerTag:
        variant === "aliasing" ? String(numOr(value.max_aliases_per_tag, 50)) : "",
      expressionMatch: parseExpressionMatch(value.expression_match),
      executionMode: parsed.executionMode,
      steps: parsed.steps,
    };
  }, [value, variant]);

  const extras = useMemo(() => extrasFrom(value, known), [value, known]);

  useEffect(() => {
    const target = initialFocusedMatchRuleName?.trim();
    if (!target) return;
    if (lastAppliedMatchFocusRef.current === target) return;

    if (useScopeRefs) {
      requestAnimationFrame(() => {
        const nodes = document.querySelectorAll("[data-discovery-match-ref-rule]");
        for (const node of nodes) {
          if (node instanceof HTMLElement && node.getAttribute("data-discovery-match-ref-rule") === target) {
            node.scrollIntoView({ block: "nearest", behavior: "smooth" });
            lastAppliedMatchFocusRef.current = target;
            break;
          }
        }
      });
      return;
    }

    if (!ui.steps.some((r) => r.name === target)) return;
    lastAppliedMatchFocusRef.current = target;
    requestAnimationFrame(() => {
      document.getElementById(focusTargetDomId("discovery-val-match", target))?.scrollIntoView({
        block: "nearest",
        behavior: "smooth",
      });
    });
  }, [initialFocusedMatchRuleName, ui.steps, useScopeRefs, value.validation_rules]);

  const commitValue = (next: JsonObject) => {
    lastWrittenFingerprintRef.current = JSON.stringify(next);
    onChange(next);
  };

  const push = (patch: Partial<JsonObject>) => {
    commitValue({ ...value, ...patch });
  };

  const commitValidationPipeline = (
    patch: Partial<{
      minConfidence: string;
      expressionMatch: ReturnType<typeof parseExpressionMatch>;
      executionMode: typeof ui.executionMode;
      steps: typeof ui.steps;
    }>
  ) => {
    const extras: JsonObject = { ...parseValidationNodeConfig(value).extras };
    if (variant === "keyExtraction") {
      extras.max_keys_per_type = Math.max(1, Math.floor(Number(ui.maxKeysPerType) || 1000));
    }
    if (variant === "aliasing") {
      extras.max_aliases_per_tag = Math.max(1, Math.floor(Number(ui.maxAliasesPerTag) || 50));
    }
    commitValue(
      serializeValidationNodeConfig({
        description: String(value.description ?? "").trim(),
        minConfidence: patch.minConfidence ?? ui.minConfidence,
        expressionMatch: patch.expressionMatch ?? ui.expressionMatch,
        executionMode: patch.executionMode ?? ui.executionMode,
        steps: patch.steps ?? ui.steps,
        extras,
      })
    );
  };

  useEffect(() => {
    const fp = JSON.stringify(value);
    if (lastWrittenFingerprintRef.current !== null && fp === lastWrittenFingerprintRef.current) {
      lastWrittenFingerprintRef.current = null;
      return;
    }
  }, [value]);

  return (
    <div className="discovery-validation-editor">
      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem" }}>
        {t("validationEditor.section.thresholds")}
      </h4>
      <div className="discovery-filter-row discovery-filter-row--thresholds">
        <label className="discovery-label">
          {t("validationEditor.minConfidence")}
          <input
            className="discovery-input"
            type="number"
            step="any"
            value={ui.minConfidence}
            onChange={(e) => commitValidationPipeline({ minConfidence: e.target.value })}
          />
        </label>
        {variant === "keyExtraction" && (
          <label className="discovery-label">
            {t("validationEditor.maxKeysPerType")}
            <input
              className="discovery-input"
              type="number"
              step={1}
              min={1}
              value={ui.maxKeysPerType}
              onChange={(e) => {
                push({ max_keys_per_type: Math.max(1, Math.floor(Number(e.target.value) || 1000)) });
              }}
            />
          </label>
        )}
        {variant === "aliasing" && (
          <label className="discovery-label">
            {t("validationEditor.maxAliasesPerTag")}
            <input
              className="discovery-input"
              type="number"
              step={1}
              min={1}
              value={ui.maxAliasesPerTag}
              onChange={(e) => {
                push({ max_aliases_per_tag: Math.max(1, Math.floor(Number(e.target.value) || 50)) });
              }}
            />
          </label>
        )}
        <label className="discovery-label">
          {t("validationEditor.defaultExpressionMatch")}
          <select
            className="discovery-input"
            value={ui.expressionMatch}
            onChange={(e) => {
              const v = e.target.value;
              if (v === "") {
                commitValidationPipeline({ expressionMatch: "" });
              } else {
                commitValidationPipeline({
                  expressionMatch: v === "search" || v === "fullmatch" ? v : "",
                });
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
        <p className="discovery-hint" style={{ marginTop: "0.75rem" }}>
          {t("validationEditor.extraKeysPreserved", { keys: Object.keys(extras).join(", ") })}
        </p>
      )}

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "1.25rem" }}>
        {t("validationEditor.section.rules")}
      </h4>

      {useScopeRefs && scopeDocument ? (
        <>
          <p className="discovery-hint">{t("validationEditor.scopeRefsModeHint")}</p>
          <MatchValidationRefsEditor value={value} onChange={commitValue} scopeDocument={scopeDocument} />
        </>
      ) : (
        <>
          <div className="discovery-editor-hint-stack">
            <p className="discovery-hint">{t("validationEditor.inlineRulesModeHint")}</p>
            <p className="discovery-hint">{t("validationEditor.rulesHint")}</p>
          </div>

          <PipelineExecutionFields
            t={t}
            executionMode={ui.executionMode}
            onExecutionModeChange={(mode) => commitValidationPipeline({ executionMode: mode })}
          />

          <ValidationStepsList
            t={t}
            steps={ui.steps}
            onChange={(steps) => commitValidationPipeline({ steps })}
            focusIdPrefix="discovery-val-match"
          />
          <button
            type="button"
            className="discovery-btn discovery-btn--sm"
            style={{ marginTop: "0.25rem" }}
            onClick={() => {
              const nr = defaultValidationStep(ui.steps);
              commitValidationPipeline({ steps: [...ui.steps, nr] });
            }}
          >
            {t("pipelineSteps.addStep")}
          </button>
        </>
      )}
    </div>
  );
}
