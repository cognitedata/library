import type { MessageKey } from "../../i18n/types";
import type { ExecutionMode } from "../../utils/etlPipelineStepsModel";
import { FieldPoliciesEditor } from "./FieldPoliciesEditor";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  executionMode: ExecutionMode;
  onExecutionModeChange: (mode: ExecutionMode) => void;
  fieldPolicies?: unknown;
  onFieldPoliciesChange?: (policies: unknown) => void;
  fieldPoliciesHintKey?: MessageKey;
};

export function EtlPipelineExecutionFields({
  t,
  executionMode,
  onExecutionModeChange,
  fieldPolicies,
  onFieldPoliciesChange,
  fieldPoliciesHintKey = "pipelineSteps.fieldPoliciesHintTransform",
}: Props) {
  return (
    <div className="transform-pipeline-execution" style={{ marginTop: "0.75rem" }}>
      <label className="gov-label">
        {t("pipelineSteps.executionMode")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem", display: "block" }}
          value={executionMode}
          onChange={(e) => onExecutionModeChange(e.target.value as ExecutionMode)}
        >
          <option value="ordered">{t("pipelineSteps.modeOrdered")}</option>
          <option value="parallel">{t("pipelineSteps.modeParallel")}</option>
        </select>
      </label>
      {executionMode === "parallel" && onFieldPoliciesChange ? (
        <div style={{ marginTop: "0.75rem" }}>
          <FieldPoliciesEditor
            t={t}
            policies={fieldPolicies}
            onChange={(policies) => onFieldPoliciesChange(policies ?? [])}
            omitWhenEmpty={false}
            emptyHintKey={fieldPoliciesHintKey}
          />
        </div>
      ) : null}
    </div>
  );
}
