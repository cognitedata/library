import type { Node } from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  flowNodes?: readonly Node[];
};

export function EtlDynamicFanoutNodeConfigFields({ value, onChange, flowNodes = [] }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const plannerIds = flowNodes
    .map((n) => n.id)
    .filter((id) => id && id !== "fanout")
    .sort();

  return (
    <div className="transform-node-editor-fields transform-dynamic-fanout-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.dynamicFanout.canvasHint")}</p>

      <label className="gov-label gov-label--block">
        {t("transform.config.description")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.dynamicFanout.generatorTaskId")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.generator_task_id ?? "")}
          onChange={(e) => patch({ generator_task_id: e.target.value })}
        >
          <option value="">{t("transform.dynamicFanout.generatorUnset")}</option>
          {plannerIds.map((id) => (
            <option key={id} value={id}>
              {id}
            </option>
          ))}
        </select>
        <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("transform.dynamicFanout.generatorTaskIdHint")}
        </span>
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.dynamicFanout.childFunction")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.child_function_external_id ?? "")}
          placeholder={t("transform.fanout.functionPlaceholder")}
          onChange={(e) => patch({ child_function_external_id: e.target.value })}
          spellCheck={false}
        />
        <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("transform.dynamicFanout.childFunctionHint")}
        </span>
      </label>

      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.75rem" }}>
        {t("transform.dynamicFanout.pagesConfiguredOnPlan")}
      </p>
    </div>
  );
}
