import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import {
  defaultValidationStep,
  parseValidationNodeConfig,
  serializeValidationNodeConfig,
} from "../utils/validationNodeConfigModel";
import { PipelineExecutionFields } from "./PipelineExecutionFields";
import { ValidationStepsList } from "./ValidationStepsList";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function ValidationNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const [showAdvancedYaml, setShowAdvancedYaml] = useState(false);
  const [configYaml, setConfigYaml] = useState(() => YAML.stringify(value, { lineWidth: 0 }));
  const [configError, setConfigError] = useState<string | null>(null);
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
      executionMode: patch.executionMode ?? ui.executionMode,
      steps: patch.steps ?? ui.steps,
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
      <div className="kea-filter-row kea-filter-row--thresholds">
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
        {t("pipelineSteps.validationStepsTitle")}
      </h4>
      <p className="kea-hint">{t("pipelineSteps.validationStepsHint")}</p>

      <PipelineExecutionFields
        t={t}
        executionMode={ui.executionMode}
        onExecutionModeChange={(mode) => commit({ executionMode: mode })}
      />

      <ValidationStepsList
        t={t}
        steps={ui.steps}
        onChange={(steps) => commit({ steps })}
        focusIdPrefix="kea-val-node-inline"
      />
      <button
        type="button"
        className="kea-btn kea-btn--sm"
        style={{ marginTop: "0.25rem" }}
        onClick={() => {
          const nr = defaultValidationStep(ui.steps);
          commit({ steps: [...ui.steps, nr] });
        }}
      >
        {t("pipelineSteps.addStep")}
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
