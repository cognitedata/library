import { useEffect, useMemo, useState, type DragEvent } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { defaultMatchRuleDefinition } from "../utils/confidenceMatchRuleDefModel";
import {
  serializeValidationNodeConfig,
  type ValidationDefinitionEntry,
  parseValidationNodeConfig,
  defaultValidationDefinitionEntry,
} from "../utils/validationNodeConfigModel";
import { reorderListAtIndex } from "../utils/ruleListReorder";
import { focusTargetDomId } from "../utils/focusTargetDomId";
import { MatchRuleDefinitionCard } from "./MatchRuleDefinitionCard";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function ValidationNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const [showAdvancedYaml, setShowAdvancedYaml] = useState(false);
  const [configYaml, setConfigYaml] = useState(() => YAML.stringify(value, { lineWidth: 0 }));
  const [configError, setConfigError] = useState<string | null>(null);
  const [dragRuleFrom, setDragRuleFrom] = useState<number | null>(null);
  const [dragRuleOver, setDragRuleOver] = useState<number | null>(null);

  const ui = useMemo(() => parseValidationNodeConfig(value), [value]);

  useEffect(() => {
    if (!showAdvancedYaml) {
      setConfigYaml(YAML.stringify(value, { lineWidth: 0 }));
    }
  }, [value, showAdvancedYaml]);

  const commit = (patch: Partial<ReturnType<typeof parseValidationNodeConfig>>) => {
    const next = serializeValidationNodeConfig({
      description: patch.description ?? ui.description,
      minConfidence: patch.minConfidence ?? ui.minConfidence,
      expressionMatch: patch.expressionMatch ?? ui.expressionMatch,
      definitionEntries: patch.definitionEntries ?? ui.definitionEntries,
      inlineRules: patch.inlineRules ?? ui.inlineRules,
      extras: ui.extras,
    });
    onChange(next);
    setConfigYaml(YAML.stringify(next, { lineWidth: 0 }));
    setConfigError(null);
  };

  const commitYaml = (raw: string) => {
    setConfigYaml(raw);
    try {
      const parsed = YAML.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        setConfigError(t("validations.yamlMustBeObject"));
        return;
      }
      setConfigError(null);
      onChange(parsed as JsonObject);
    } catch (e) {
      setConfigError(String(e));
    }
  };

  const updateDefinition = (idx: number, entry: ValidationDefinitionEntry) => {
    const next = [...ui.definitionEntries];
    next[idx] = entry;
    commit({ definitionEntries: next });
  };

  const renameDefinitionId = (idx: number, nextId: string) => {
    const trimmed = nextId.trim();
    if (!trimmed) return;
    const next = [...ui.definitionEntries];
    const cur = next[idx];
    if (!cur) return;
    next[idx] = { id: trimmed, rule: { ...cur.rule, name: cur.rule.name || trimmed } };
    commit({ definitionEntries: next });
  };

  const removeDefinition = (idx: number) => {
    commit({ definitionEntries: ui.definitionEntries.filter((_, i) => i !== idx) });
  };

  const addDefinition = () => {
    const entry = defaultValidationDefinitionEntry(ui.definitionEntries.map((e) => e.id));
    commit({ definitionEntries: [...ui.definitionEntries, entry] });
  };

  const updateInlineRules = (rules: typeof ui.inlineRules) => {
    commit({ inlineRules: rules });
  };

  return (
    <div className="kea-validation-editor">
      <label className="kea-label kea-label--block">
        {t("validations.description")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={ui.description}
          onChange={(e) => commit({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("validationEditor.section.thresholds")}
      </h4>
      <div
        className="kea-filter-row"
        style={{ gridTemplateColumns: "repeat(auto-fit,minmax(10rem,1fr))", gap: "0.75rem" }}
      >
        <label className="kea-label">
          {t("validationEditor.minConfidence")}
          <input
            className="kea-input"
            type="number"
            step="any"
            value={ui.minConfidence}
            onChange={(e) => commit({ minConfidence: e.target.value })}
          />
        </label>
        <label className="kea-label">
          {t("validationEditor.defaultExpressionMatch")}
          <select
            className="kea-input"
            value={ui.expressionMatch}
            onChange={(e) => {
              const v = e.target.value;
              commit({
                expressionMatch: v === "search" || v === "fullmatch" ? v : "",
              });
            }}
          >
            <option value="">{t("validationEditor.expressionMatch.inherit")}</option>
            <option value="search">{t("validationEditor.expressionMatch.search")}</option>
            <option value="fullmatch">{t("validationEditor.expressionMatch.fullmatch")}</option>
          </select>
        </label>
      </div>

      {Object.keys(ui.extras).length > 0 ? (
        <p className="kea-hint" style={{ marginTop: "0.75rem" }}>
          {t("validationEditor.extraKeysPreserved", { keys: Object.keys(ui.extras).join(", ") })}
        </p>
      ) : null}

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1.25rem" }}>
        {t("validations.ruleDefinitionsTitle")}
      </h4>
      <p className="kea-hint">{t("validations.ruleDefinitionsHint")}</p>

      {ui.definitionEntries.length === 0 ? (
        <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
          {t("validations.noRuleDefinitions")}
        </p>
      ) : (
        <div className="kea-validation-rules" style={{ marginTop: "0.5rem" }}>
          {ui.definitionEntries.map((entry, idx) => (
            <div
              key={`${idx}-${entry.id}`}
              className="kea-validation-rule"
              style={{
                border: "1px solid var(--kea-border)",
                borderRadius: "var(--kea-radius-sm)",
                padding: "0.65rem",
                marginBottom: "0.75rem",
                background: "var(--kea-bg-elevated)",
              }}
            >
              <label className="kea-label kea-label--block" style={{ marginBottom: "0.5rem" }}>
                {t("validations.ruleDefinitionId")}
                <input
                  className="kea-input"
                  style={{ marginTop: "0.35rem" }}
                  value={entry.id}
                  onChange={(e) => renameDefinitionId(idx, e.target.value)}
                  spellCheck={false}
                />
              </label>
              <MatchRuleDefinitionCard
                rule={entry.rule}
                ruleIndex={idx}
                defaultExpanded
                showCollapsedSummary
                onChange={(rule) => updateDefinition(idx, { ...entry, rule })}
              />
              <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "0.35rem" }}>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  onClick={() => removeDefinition(idx)}
                >
                  {t("validations.removeRuleDefinition")}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
      <button type="button" className="kea-btn kea-btn--sm" style={{ marginTop: "0.25rem" }} onClick={addDefinition}>
        {t("validations.addRuleDefinition")}
      </button>

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1.25rem" }}>
        {t("validationEditor.section.rules")}
      </h4>
      <p className="kea-hint">{t("validations.inlineRulesHint")}</p>
      <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
        {t("rulesEntity.dragReorderRules")}
      </p>

      <div className="kea-validation-rules" style={{ marginTop: "0.5rem" }}>
        {ui.inlineRules.map((rule, idx) => {
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
              id={focusTargetDomId("kea-val-node-inline", rule.name)}
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
                updateInlineRules(reorderListAtIndex(ui.inlineRules, from, idx));
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
                  const next = [...ui.inlineRules];
                  next[idx] = r;
                  updateInlineRules(next);
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
                    const next = [...ui.inlineRules];
                    [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]!];
                    updateInlineRules(next);
                  }}
                  aria-label={t("validationEditor.rule.moveUp")}
                >
                  ↑
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  disabled={idx >= ui.inlineRules.length - 1}
                  onClick={() => {
                    const next = [...ui.inlineRules];
                    [next[idx], next[idx + 1]] = [next[idx + 1]!, next[idx]!];
                    updateInlineRules(next);
                  }}
                  aria-label={t("validationEditor.rule.moveDown")}
                >
                  ↓
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  onClick={() => updateInlineRules(ui.inlineRules.filter((_, i) => i !== idx))}
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
          const nr = defaultMatchRuleDefinition(ui.inlineRules);
          updateInlineRules([...ui.inlineRules, nr]);
        }}
      >
        {t("validationEditor.rule.addRule")}
      </button>

      <details
        style={{ marginTop: "1.25rem" }}
        open={showAdvancedYaml}
        onToggle={(e) => setShowAdvancedYaml((e.target as HTMLDetailsElement).open)}
      >
        <summary className="kea-label" style={{ cursor: "pointer", userSelect: "none" }}>
          {t("validations.advancedYaml")}
        </summary>
        <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
          {t("validations.configHint")}
        </p>
        <textarea
          className="kea-textarea"
          style={{ marginTop: "0.35rem", minHeight: 200, width: "100%" }}
          value={configYaml}
          onChange={(e) => commitYaml(e.target.value)}
          spellCheck={false}
        />
        {configError ? <p className="kea-hint kea-hint--warn">{configError}</p> : null}
      </details>
    </div>
  );
}
