import { useMemo } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { legacyConfigToStep } from "../../utils/etlTransformNodeConfigModel";
import {
  defaultTransformStep,
  parseTransformNodeConfig,
  serializeTransformNodeConfig,
} from "../../utils/etlTransformNodeConfigModel";
import { EtlPipelineExecutionFields } from "./EtlPipelineExecutionFields";
import { EtlTransformSingleStepFields } from "./EtlTransformSingleStepFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  /** When true, handler was chosen at node creation (palette / per-handler add) and cannot change. */
  handlerLocked?: boolean;
};

export function EtlTransformNodeConfigFields({ value, onChange, handlerLocked = false }: Props) {
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
      <div className="transform-node-editor-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.description")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={ui.description}
            onChange={(e) => commit({ description: e.target.value })}
            spellCheck={false}
            autoComplete="off"
          />
        </label>
        <label className="gov-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={false}
            onChange={(e) => {
              if (e.target.checked) setMultiStep(true);
            }}
          />
          {t("pipelineSteps.useMultiStep")}
        </label>
        <EtlTransformSingleStepFields
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
    <div className="transform-node-editor-fields">
      <label className="gov-label gov-label--block">
        {t("transforms.description")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={ui.description}
          onChange={(e) => commit({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="gov-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked
          onChange={(e) => {
            if (!e.target.checked) setMultiStep(false);
          }}
        />
        {t("pipelineSteps.useMultiStep")}
      </label>

      <h4 className="gov-modal__title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("pipelineSteps.transformStepsTitle")}
      </h4>
      <p className="transform-node-editor-modal__hint">{t("pipelineSteps.transformStepsHint")}</p>

      <EtlPipelineExecutionFields
        t={t}
        executionMode={ui.executionMode}
        onExecutionModeChange={(mode) => commit({ executionMode: mode })}
        fieldPolicies={ui.fieldPolicies}
        onFieldPoliciesChange={(policies) => commit({ fieldPolicies: policies })}
      />

      {ui.steps.map((step, idx) => (
        <div key={idx} className="transform-step-card">
          <EtlTransformSingleStepFields
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
            <div className="transform-step-card__actions">
              <button
                type="button"
                className="disc-btn disc-btn--ghost disc-btn--sm"
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
                className="disc-btn disc-btn--ghost disc-btn--sm"
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
                className="disc-btn disc-btn--ghost disc-btn--sm"
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
        className="disc-btn disc-btn--sm"
        style={{ marginTop: "0.75rem" }}
        onClick={() => commit({ steps: [...ui.steps, defaultTransformStep(ui.steps)] })}
      >
        {t("pipelineSteps.addStep")}
      </button>
    </div>
  );
}
