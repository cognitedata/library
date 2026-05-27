import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";

type Props = {
  kind: Extract<TransformCanvasNodeKind, "function_ref" | "subworkflow" | "simulation" | "cdf_task">;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function EtlOrchestrationNodeConfigFields({ kind, value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const cdfText =
    value.cdf == null
      ? ""
      : typeof value.cdf === "string"
        ? value.cdf
        : JSON.stringify(value.cdf, null, 2);

  return (
    <div className="transform-node-editor-fields transform-orchestration-fields">
      <p className="transform-node-editor-modal__hint">{t(`transform.orchestration.${kind}.hint`)}</p>

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

      {kind === "function_ref" ? (
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.config.functionExternalId")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.function_external_id ?? "")}
            onChange={(e) => patch({ function_external_id: e.target.value })}
            placeholder={t("transform.orchestration.functionPlaceholder")}
            spellCheck={false}
          />
        </label>
      ) : null}

      {kind === "subworkflow" ? (
        <>
          <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
            {t("transform.config.workflowExternalId")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.workflow_external_id ?? "")}
              onChange={(e) => patch({ workflow_external_id: e.target.value })}
              spellCheck={false}
            />
          </label>
          <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
            {t("transform.config.workflowVersion")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem", maxWidth: "8rem" }}
              value={String(value.workflow_version ?? "1")}
              onChange={(e) => patch({ workflow_version: e.target.value })}
              spellCheck={false}
            />
          </label>
        </>
      ) : null}

      {kind === "simulation" ? (
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.orchestration.simulation.externalId")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.simulation_external_id ?? "")}
            onChange={(e) => patch({ simulation_external_id: e.target.value })}
            spellCheck={false}
          />
        </label>
      ) : null}

      {kind === "cdf_task" ? (
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.orchestration.cdfTask.parametersJson")}
          <textarea
            className="gov-input gov-input--mono"
            style={{ marginTop: "0.35rem", minHeight: "10rem" }}
            value={cdfText}
            onChange={(e) => {
              const raw = e.target.value.trim();
              if (!raw) {
                const next = { ...value };
                delete next.cdf;
                onChange(next);
                return;
              }
              try {
                patch({ cdf: JSON.parse(raw) });
              } catch {
                patch({ cdf: raw });
              }
            }}
            spellCheck={false}
          />
        </label>
      ) : null}
    </div>
  );
}
