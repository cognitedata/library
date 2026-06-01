import { useAppSettings } from "../../context/AppSettingsContext";
import {
  defaultScoringRuleRow,
  type ScoringRuleRow,
} from "../../utils/scoreNodeConfigModel";
import { lookupScorePatternDescription } from "../../utils/scorePatternCatalog";

type Props = {
  rules: ScoringRuleRow[];
  onChange: (next: ScoringRuleRow[]) => void;
};

function ScoringRuleCard({
  rule,
  onChange,
}: {
  rule: ScoringRuleRow;
  onChange: (r: ScoringRuleRow) => void;
}) {
  const { t } = useAppSettings();
  const patch = (p: Partial<ScoringRuleRow>) => onChange({ ...rule, ...p });
  const setExpressions = (next: ScoringRuleRow["expressions"]) => patch({ expressions: next });

  return (
    <div
      className="transform-score-rule-card"
      style={{
        border: "1px solid var(--discovery-border, #ccc)",
        borderRadius: 6,
        padding: "0.75rem",
        marginBottom: "0.65rem",
        background: "var(--discovery-surface-2, rgba(0,0,0,0.02))",
      }}
    >
      <div
        className="transform-flow-inspector__field transform-flow-inspector__field--field-pair"
        style={{ flexWrap: "wrap" }}
      >
        <label className="gov-label">
          {t("transform.score.ruleName")}
          <input
            className="gov-input"
            value={rule.name}
            onChange={(e) => patch({ name: e.target.value })}
            spellCheck={false}
          />
        </label>
        <label className="gov-label">
          {t("transform.score.rulePriority")}
          <input
            className="gov-input"
            value={rule.priority}
            onChange={(e) => patch({ priority: e.target.value })}
            placeholder={t("transform.score.rulePriorityPlaceholder")}
            spellCheck={false}
          />
        </label>
        <label className="gov-label" style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 0 }}>
          <input type="checkbox" checked={rule.enabled} onChange={(e) => patch({ enabled: e.target.checked })} />
          {t("transform.score.ruleEnabled")}
        </label>
      </div>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transform.score.ruleKeywords")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={rule.keywordsText}
          onChange={(e) => patch({ keywordsText: e.target.value })}
          placeholder={t("transform.score.ruleKeywordsPlaceholder")}
          spellCheck={false}
        />
      </label>

      <div style={{ marginTop: "0.65rem" }}>
        <span className="discovery-handler-fieldset-legend">{t("transform.score.ruleExpressions")}</span>
        {rule.expressions.map((row, i) => (
          <div
            key={i}
            className="transform-flow-inspector__field transform-flow-inspector__field--field-pair transform-flow-inspector__field--align-end"
            style={{ marginTop: "0.35rem", flexWrap: "wrap" }}
          >
            <label className="gov-label">
              {t("transform.score.rulePattern")}
              <input
                className="gov-input"
                value={row.pattern}
                onChange={(e) => {
                  const next = [...rule.expressions];
                  next[i] = { ...row, pattern: e.target.value };
                  setExpressions(next);
                }}
                onBlur={() => {
                  const trimmed = row.pattern.trim();
                  if (!trimmed || row.description.trim()) return;
                  const desc = lookupScorePatternDescription(trimmed);
                  if (!desc) return;
                  const next = [...rule.expressions];
                  next[i] = { ...row, description: desc };
                  setExpressions(next);
                }}
                spellCheck={false}
              />
            </label>
            <label className="gov-label">
              {t("validationEditor.rule.description")}
              <input
                className="gov-input"
                value={row.description}
                onChange={(e) => {
                  const next = [...rule.expressions];
                  next[i] = { ...row, description: e.target.value };
                  setExpressions(next);
                }}
                placeholder={t("validationEditor.rule.description")}
                spellCheck={false}
              />
            </label>
            <button
              type="button"
              className="disc-btn disc-btn--ghost disc-btn--sm"
              disabled={rule.expressions.length <= 1}
              onClick={() => {
                const next = rule.expressions.filter((_, j) => j !== i);
                setExpressions(next.length ? next : [{ pattern: "", description: "" }]);
              }}
            >
              ×
            </button>
          </div>
        ))}
        <button
          type="button"
          className="disc-btn disc-btn--sm"
          style={{ marginTop: "0.35rem" }}
          onClick={() => setExpressions([...rule.expressions, { pattern: "", description: "" }])}
        >
          {t("transform.score.ruleAddExpression")}
        </button>
      </div>

      <div
        className="transform-flow-inspector__field transform-flow-inspector__field--field-pair"
        style={{ marginTop: "0.75rem", flexWrap: "wrap" }}
      >
        <label className="gov-label">
          {t("transform.score.ruleModifierMode")}
          <select
            className="gov-input"
            value={rule.modMode}
            onChange={(e) => patch({ modMode: e.target.value === "explicit" ? "explicit" : "offset" })}
          >
            <option value="offset">{t("transform.score.ruleModifierOffset")}</option>
            <option value="explicit">{t("transform.score.ruleModifierExplicit")}</option>
          </select>
        </label>
        <label className="gov-label">
          {t("transform.score.ruleModifierValue")}
          <input
            className="gov-input"
            type="number"
            step="any"
            value={rule.modValue}
            onChange={(e) => patch({ modValue: e.target.value })}
          />
        </label>
      </div>

      <label className="gov-label" style={{ marginTop: "0.5rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={rule.noMatchEnabled}
          onChange={(e) => patch({ noMatchEnabled: e.target.checked })}
        />
        {t("transform.score.ruleNoMatchEnabled")}
      </label>
      {rule.noMatchEnabled ? (
        <div
          className="transform-flow-inspector__field transform-flow-inspector__field--field-pair"
          style={{ marginTop: "0.35rem", flexWrap: "wrap" }}
        >
          <label className="gov-label">
            {t("transform.score.ruleNoMatchMode")}
            <select
              className="gov-input"
              value={rule.noMatchModMode}
              onChange={(e) =>
                patch({ noMatchModMode: e.target.value === "explicit" ? "explicit" : "offset" })
              }
            >
              <option value="offset">{t("transform.score.ruleModifierOffset")}</option>
              <option value="explicit">{t("transform.score.ruleModifierExplicit")}</option>
            </select>
          </label>
          <label className="gov-label">
            {t("transform.score.ruleModifierValue")}
            <input
              className="gov-input"
              type="number"
              step="any"
              value={rule.noMatchModValue}
              onChange={(e) => patch({ noMatchModValue: e.target.value })}
            />
          </label>
        </div>
      ) : null}
    </div>
  );
}

export function ScoringRulesEditor({ rules, onChange }: Props) {
  const { t } = useAppSettings();

  return (
    <div className="transform-score-rules-editor">
      <h4 className="gov-modal__title" style={{ fontSize: "0.95rem", marginTop: 0 }}>
        {t("transform.score.rulesSection")}
      </h4>
      <p className="transform-node-editor-modal__hint">{t("transform.score.rulesHint")}</p>
      {rules.map((rule, idx) => (
        <div key={idx}>
          <ScoringRuleCard
            rule={rule}
            onChange={(r) => {
              const next = [...rules];
              next[idx] = r;
              onChange(next);
            }}
          />
          <div style={{ display: "flex", justifyContent: "flex-end", gap: "0.25rem", marginBottom: "0.5rem" }}>
            <button
              type="button"
              className="disc-btn disc-btn--ghost disc-btn--sm"
              disabled={idx === 0}
              onClick={() => {
                const next = [...rules];
                [next[idx - 1], next[idx]] = [next[idx]!, next[idx - 1]!];
                onChange(next);
              }}
              aria-label={t("transform.score.ruleMoveUp")}
            >
              ↑
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--ghost disc-btn--sm"
              disabled={idx >= rules.length - 1}
              onClick={() => {
                const next = [...rules];
                [next[idx], next[idx + 1]] = [next[idx + 1]!, next[idx]!];
                onChange(next);
              }}
              aria-label={t("transform.score.ruleMoveDown")}
            >
              ↓
            </button>
            <button
              type="button"
              className="disc-btn disc-btn--ghost disc-btn--sm"
              onClick={() => onChange(rules.filter((_, i) => i !== idx))}
            >
              {t("transform.score.ruleRemove")}
            </button>
          </div>
        </div>
      ))}
      <button
        type="button"
        className="disc-btn disc-btn--sm"
        onClick={() => onChange([...rules, defaultScoringRuleRow(rules)])}
      >
        {t("transform.score.ruleAdd")}
      </button>
    </div>
  );
}
