import { useMemo } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { legacyConfigToStep } from "../utils/transformNodeConfigModel";
import {
  defaultTransformStep,
  parseTransformNodeConfig,
  serializeTransformNodeConfig,
} from "../utils/transformNodeConfigModel";
import { PipelineExecutionFields } from "./PipelineExecutionFields";
import { TransformSingleStepFields } from "./TransformSingleStepFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  /** When true, handler was chosen at node creation (palette / per-handler add) and cannot change. */
  handlerLocked?: boolean;
};

export function TransformNodeConfigFields({ value, onChange, handlerLocked = false }: Props) {
  const { t } = useAppSettings();
  const ui = useMemo(() => parseTransformNodeConfig(value), [value]);

  const commit = (patch: Partial<ReturnType<typeof parseTransformNodeConfig>>) => {
    const next = serializeTransformNodeConfig({
      description: patch.description ?? ui.description,
      executionMode: patch.executionMode ?? ui.executionMode,
      steps: patch.steps ?? ui.steps,
      fieldPolicies: patch.fieldPolicies ?? ui.fieldPolicies,
      multiStep: patch.multiStep ?? ui.multiStep,
      extras: ui.extras,
    });
    onChange(next);
  };

  const setMultiStep = (enabled: boolean) => {
    if (enabled) {
      const steps = ui.steps.length > 0 ? ui.steps : [legacyConfigToStep(value)];
      commit({ multiStep: true, steps });
      return;
    }
    const steps = ui.steps.length > 0 ? ui.steps : [legacyConfigToStep(value)];
    commit({ multiStep: false, steps: [steps[0] ?? legacyConfigToStep(value)] });
  };

  if (!ui.multiStep) {
    const stepValue = ui.steps[0] ?? legacyConfigToStep(value);
    return (
      <div className="kea-loc-fields">
        <label className="kea-label kea-label--block">
          {t("transforms.description")}
          <input
            className="kea-input"
            style={{ marginTop: "0.35rem" }}
            value={ui.description}
            onChange={(e) => commit({ description: e.target.value })}
            spellCheck={false}
            autoComplete="off"
          />
        </label>
        <label className="kea-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={false}
            onChange={(e) => {
              if (e.target.checked) setMultiStep(true);
            }}
          />
          {t("pipelineSteps.useMultiStep")}
        </label>
        <TransformSingleStepFields
          value={{ ...stepValue, description: stepValue.description ?? ui.description }}
          handlerLocked={handlerLocked}
          onChange={(step) => {
            const desc = String(step.description ?? ui.description).trim();
            const { description: _d, ...rest } = step;
            commit({
              description: desc,
              steps: [{ ...rest, description: step.description }],
              multiStep: false,
            });
          }}
        />
      </div>
    );
  }

  return (
    <div className="kea-loc-fields">
      <label className="kea-label kea-label--block">
        {t("transforms.description")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={ui.description}
          onChange={(e) => commit({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="kea-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked
          onChange={(e) => {
            if (!e.target.checked) setMultiStep(false);
          }}
        />
        {t("pipelineSteps.useMultiStep")}
      </label>

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("pipelineSteps.transformStepsTitle")}
      </h4>
      <p className="kea-hint">{t("pipelineSteps.transformStepsHint")}</p>

      <PipelineExecutionFields
        t={t}
        executionMode={ui.executionMode}
        onExecutionModeChange={(mode) => commit({ executionMode: mode })}
        fieldPolicies={ui.fieldPolicies}
        onFieldPoliciesChange={(policies) => commit({ fieldPolicies: policies })}
      />

      {ui.steps.map((step, idx) => (
        <div key={idx} className="kea-step-card">
          <TransformSingleStepFields
            stepIndex={idx}
            value={step}
            handlerLocked={handlerLocked && idx === 0}
            onChange={(next) => {
              const steps = [...ui.steps];
              steps[idx] = next;
              commit({ steps });
            }}
          />
          {ui.steps.length > 1 ? (
            <div className="kea-step-card__actions">
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                disabled={idx === 0}
                onClick={() => {
                  const steps = [...ui.steps];
                  [steps[idx - 1], steps[idx]] = [steps[idx]!, steps[idx - 1]!];
                  commit({ steps });
                }}
                aria-label={t("validationEditor.rule.moveUp")}
              >
                ↑
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                disabled={idx >= ui.steps.length - 1}
                onClick={() => {
                  const steps = [...ui.steps];
                  [steps[idx], steps[idx + 1]] = [steps[idx + 1]!, steps[idx]!];
                  commit({ steps });
                }}
                aria-label={t("validationEditor.rule.moveDown")}
              >
                ↓
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                onClick={() => commit({ steps: ui.steps.filter((_, i) => i !== idx) })}
              >
                {t("validationEditor.rule.remove")}
              </button>
            </div>
          ) : null}
        </div>
      ))}

      <button
        type="button"
        className="kea-btn kea-btn--sm"
        style={{ marginTop: "0.75rem" }}
        onClick={() => commit({ steps: [...ui.steps, defaultTransformStep(ui.steps)] })}
      >
        {t("pipelineSteps.addStep")}
      </button>
    </div>
  );
}
