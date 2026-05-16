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
      className="kea-validation-rule"
      style={{
        border: "1px solid var(--kea-border)",
        borderRadius: "var(--kea-radius-sm)",
        padding: "0.75rem",
        marginBottom: "0.75rem",
        background: "var(--kea-surface)",
      }}
    >
      <div
        className="kea-filter-row"
        style={{ gridTemplateColumns: dragProps ? "auto auto 1fr auto" : "auto 1fr auto", gap: "0.5rem", alignItems: "end" }}
      >
        {dragProps && (
          <span
            className="kea-drag-handle"
            draggable={dragProps.draggable}
            onDragStart={dragProps.onDragStart}
            onDragEnd={dragProps.onDragEnd}
            aria-label={t("rulesEntity.dragHandle")}
            title={t("rulesEntity.dragHandle")}
          >
            <span className="kea-drag-handle__grip" aria-hidden>
              ⋮⋮
            </span>
          </span>
        )}
        <button
          type="button"
          className="kea-btn kea-btn--ghost kea-btn--sm"
          aria-expanded={expanded}
          onClick={() => setExpanded((x) => !x)}
          style={{ minWidth: 36 }}
        >
          <span aria-hidden>{expanded ? "▼" : "▶"}</span>
        </button>
        <label className="kea-label">
          {t("validationEditor.rule.name")}
          <DeferredCommitInput
            className="kea-input"
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
        <label className="kea-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginBottom: 0, whiteSpace: "nowrap" }}>
          <input type="checkbox" checked={rule.enabled} onChange={(e) => onChange({ ...rule, enabled: e.target.checked })} />
          {t("validationEditor.rule.enabled")}
        </label>
      </div>
      {showCollapsedSummary && !expanded && (
        <p className="kea-hint" style={{ marginTop: "0.5rem", marginBottom: 0 }}>
          {summary || "—"}
        </p>
      )}
      {expanded && (
        <>
          <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}>
            <label className="kea-label">
              {t("validationEditor.rule.priority")}
              <input
                className="kea-input"
                placeholder={t("validationEditor.rule.priorityPlaceholder")}
                value={rule.priority}
                onChange={(e) => onChange({ ...rule, priority: e.target.value })}
              />
            </label>
            <label className="kea-label">
              {t("validationEditor.rule.expressionMatch")}
              <select
                className="kea-input"
                value={rule.expressionMatch}
                onChange={(e) => onChange({ ...rule, expressionMatch: e.target.value as ExpressionMatchOpt })}
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
              onChange={(e) => onChange({ ...rule, keywordsText: e.target.value })}
              spellCheck={false}
              placeholder={t("validationEditor.rule.keywordsPlaceholder")}
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
                    const exprs = [...rule.expressions];
                    exprs[j] = { ...exprs[j]!, pattern: e.target.value };
                    onChange({ ...rule, expressions: exprs });
                  }}
                />
                <input
                  className="kea-input"
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
                  className="kea-btn kea-btn--ghost kea-btn--sm"
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
              className="kea-btn kea-btn--sm"
              style={{ marginTop: "0.25rem" }}
              onClick={() => onChange({ ...rule, expressions: [...rule.expressions, { pattern: "", description: "" }] })}
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
                onChange={(e) => onChange({ ...rule, modMode: e.target.value as ModMode })}
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
                onChange={(e) => onChange({ ...rule, modValue: e.target.value })}
              />
            </label>
          </div>
        </>
      )}
    </div>
  );
}
