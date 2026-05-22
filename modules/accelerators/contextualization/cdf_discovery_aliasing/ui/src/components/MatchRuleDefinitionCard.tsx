import { useState, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import type {
  ExpressionMatchOpt,
  MatchRuleDefinition,
  ModMode,
} from "../utils/confidenceMatchRuleDefModel";
import { ruleNameOrDefault, validationRuleCollapsedSummary } from "../utils/confidenceMatchRuleDefModel";

type Props = {
  rule: MatchRuleDefinition;
  ruleIndex: number;
  onChange: (r: MatchRuleDefinition) => void;
  /** Optional drag handle for reordering multiple cards. */
  dragProps?: {
    draggable: boolean;
    onDragStart: (e: DragEvent<HTMLSpanElement>) => void;
    onDragEnd: () => void;
  };
  showCollapsedSummary?: boolean;
  defaultExpanded?: boolean;
};

export function MatchRuleDefinitionCard({
  rule,
  ruleIndex,
  onChange,
  dragProps,
  showCollapsedSummary = true,
  defaultExpanded = true,
}: Props) {
  const { t } = useAppSettings();
  const [expanded, setExpanded] = useState(defaultExpanded);

  const summary = validationRuleCollapsedSummary(rule);

  return (
    <div
      className="discovery-validation-rule"
      style={{
        border: "1px solid var(--discovery-border)",
        borderRadius: "var(--discovery-radius-sm)",
        padding: "0.75rem",
        marginBottom: "0.75rem",
        background: "var(--discovery-surface)",
      }}
    >
      <div
        className="discovery-filter-row discovery-filter-row--rule-header"
        style={{ gap: "0.5rem", alignItems: "end" }}
      >
        {dragProps && (
          <span
            className="discovery-drag-handle"
            draggable={dragProps.draggable}
            onDragStart={dragProps.onDragStart}
            onDragEnd={dragProps.onDragEnd}
            aria-label={t("rulesEntity.dragHandle")}
            title={t("rulesEntity.dragHandle")}
          >
            <span className="discovery-drag-handle__grip" aria-hidden>
              ⋮⋮
            </span>
          </span>
        )}
        <button
          type="button"
          className="discovery-btn discovery-btn--ghost discovery-btn--sm"
          aria-expanded={expanded}
          onClick={() => setExpanded((x) => !x)}
          style={{ minWidth: 36 }}
        >
          <span aria-hidden>{expanded ? "▼" : "▶"}</span>
        </button>
        <label className="discovery-label">
          {t("validationEditor.rule.name")}
          <DeferredCommitInput
            className="discovery-input"
            type="text"
            required
            aria-required={true}
            committedValue={rule.name}
            syncKey={ruleIndex}
            onCommit={(raw) => {
              const trimmed = raw.trim();
              const nextName = trimmed ? trimmed : ruleNameOrDefault("", ruleIndex + 1);
              if (nextName !== rule.name) {
                onChange({ ...rule, name: nextName });
              }
            }}
          />
        </label>
        <label className="discovery-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginBottom: 0, whiteSpace: "nowrap" }}>
          <input type="checkbox" checked={rule.enabled} onChange={(e) => onChange({ ...rule, enabled: e.target.checked })} />
          {t("validationEditor.rule.enabled")}
        </label>
      </div>
      {showCollapsedSummary && !expanded && (
        <p className="discovery-hint" style={{ marginTop: "0.5rem", marginBottom: 0 }}>
          {summary || "—"}
        </p>
      )}
      {expanded && (
        <>
          <div className="discovery-filter-row discovery-filter-row--pair discovery-filter-row--gap-md" style={{ marginTop: "0.5rem" }}>
            <label className="discovery-label">
              {t("validationEditor.rule.priority")}
              <input
                className="discovery-input"
                placeholder={t("validationEditor.rule.priorityPlaceholder")}
                value={rule.priority}
                onChange={(e) => onChange({ ...rule, priority: e.target.value })}
              />
            </label>
            <label className="discovery-label">
              {t("validationEditor.rule.expressionMatch")}
              <select
                className="discovery-input"
                value={rule.expressionMatch}
                onChange={(e) => onChange({ ...rule, expressionMatch: e.target.value as ExpressionMatchOpt })}
              >
                <option value="">{t("validationEditor.expressionMatch.inherit")}</option>
                <option value="search">{t("validationEditor.expressionMatch.search")}</option>
                <option value="fullmatch">{t("validationEditor.expressionMatch.fullmatch")}</option>
              </select>
            </label>
          </div>
          <label className="discovery-label discovery-label--block" style={{ marginTop: "0.5rem" }}>
            {t("validationEditor.rule.keywords")}
            <input
              className="discovery-input"
              type="text"
              value={rule.keywordsText}
              onChange={(e) => onChange({ ...rule, keywordsText: e.target.value })}
              spellCheck={false}
              placeholder={t("validationEditor.rule.keywordsPlaceholder")}
            />
          </label>
          <div style={{ marginTop: "0.5rem" }}>
            <span className="discovery-label" style={{ display: "block", marginBottom: "0.25rem" }}>
              {t("validationEditor.rule.expressions")}
            </span>
            {rule.expressions.map((ex, j) => (
              <div key={j} className="discovery-filter-row discovery-filter-row--field-pair discovery-filter-row--gap-sm" style={{ marginBottom: "0.35rem" }}>
                <input
                  className="discovery-input"
                  placeholder={t("validationEditor.rule.pattern")}
                  value={ex.pattern}
                  onChange={(e) => {
                    const exprs = [...rule.expressions];
                    exprs[j] = { ...exprs[j]!, pattern: e.target.value };
                    onChange({ ...rule, expressions: exprs });
                  }}
                />
                <input
                  className="discovery-input"
                  placeholder={t("validationEditor.rule.description")}
                  value={ex.description}
                  onChange={(e) => {
                    const exprs = [...rule.expressions];
                    exprs[j] = { ...exprs[j]!, description: e.target.value };
                    onChange({ ...rule, expressions: exprs });
                  }}
                />
                <button
                  type="button"
                  className="discovery-btn discovery-btn--ghost discovery-btn--sm"
                  disabled={rule.expressions.length <= 1}
                  onClick={() => {
                    const exprs = rule.expressions.filter((_, k) => k !== j);
                    onChange({
                      ...rule,
                      expressions: exprs.length ? exprs : [{ pattern: "", description: "" }],
                    });
                  }}
                >
                  ×
                </button>
              </div>
            ))}
            <button
              type="button"
              className="discovery-btn discovery-btn--sm"
              style={{ marginTop: "0.25rem" }}
              onClick={() => onChange({ ...rule, expressions: [...rule.expressions, { pattern: "", description: "" }] })}
            >
              {t("validationEditor.rule.addExpression")}
            </button>
          </div>
          <div className="discovery-filter-row discovery-filter-row--pair discovery-filter-row--gap-md" style={{ marginTop: "0.5rem" }}>
            <label className="discovery-label">
              {t("validationEditor.rule.modifierMode")}
              <select
                className="discovery-input"
                value={rule.modMode}
                onChange={(e) => onChange({ ...rule, modMode: e.target.value as ModMode })}
              >
                <option value="offset">{t("validationEditor.rule.modifierOffset")}</option>
                <option value="explicit">{t("validationEditor.rule.modifierExplicit")}</option>
              </select>
            </label>
            <label className="discovery-label">
              {t("validationEditor.rule.modifierValue")}
              <input
                className="discovery-input"
                type="number"
                step="any"
                value={rule.modValue}
                onChange={(e) => onChange({ ...rule, modValue: e.target.value })}
              />
            </label>
          </div>
        </>
      )}
    </div>
  );
}
