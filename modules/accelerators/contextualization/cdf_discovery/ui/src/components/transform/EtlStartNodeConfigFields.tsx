import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import type { TransformWorkflowBuildPairing } from "../../api";
import { DataModelingTriggerConfigFields } from "./DataModelingTriggerConfigFields";
import { RecordStreamTriggerConfigFields } from "./RecordStreamTriggerConfigFields";
import { ScheduleEditorControl } from "./ScheduleEditorControl";
import { TriggerRuleDetailsFields } from "./TriggerRuleDetailsFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  buildPairing?: TransformWorkflowBuildPairing | null;
};

export function EtlStartNodeConfigFields({ value, onChange, buildPairing = null }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const triggerType = String(value.trigger_type ?? "schedule");
  const wfExt =
    String(value.workflow_external_id ?? "").trim() ||
    String(buildPairing?.workflow_external_id ?? "").trim();
  const trgExt =
    String(value.trigger_external_id ?? "").trim() ||
    String(buildPairing?.trigger_external_id ?? "").trim();
  const wfBase =
    String(value.workflow_base ?? "").trim() || String(buildPairing?.workflow_base ?? "").trim();

  return (
    <div className="transform-node-editor-fields transform-start-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.start.canvasHint")}</p>

      <label className="gov-label gov-label--block">
        {t("transform.config.description")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
        />
      </label>

      <p className="transform-flow-inspector__field" style={{ marginTop: "0.75rem" }}>
        <span className="transform-flow-inspector__summary-label">{t("transform.config.workflowBase")}</span>
        <code className="transform-flow-inspector__code">{wfBase || "—"}</code>
      </p>
      <p className="transform-flow-inspector__field">
        <span className="transform-flow-inspector__summary-label">{t("transform.config.workflowExternalIdBuilt")}</span>
        <code className="transform-flow-inspector__code">{wfExt || "—"}</code>
      </p>
      <p className="transform-flow-inspector__field">
        <span className="transform-flow-inspector__summary-label">{t("transform.config.triggerExternalIdBuilt")}</span>
        <code className="transform-flow-inspector__code">{trgExt || "—"}</code>
      </p>
      {buildPairing?.pairings && buildPairing.pairings.length > 1 ? (
        <p className="transform-flow-inspector__hint">{t("transform.config.buildPairingScopedHint")}</p>
      ) : null}

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

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.config.workflowVersion")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem", maxWidth: "8rem" }}
          value={String(value.workflow_version ?? "1")}
          onChange={(e) => patch({ workflow_version: e.target.value })}
          spellCheck={false}
        />
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.config.triggerType")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={triggerType}
          onChange={(e) => patch({ trigger_type: e.target.value })}
        >
          <option value="schedule">{t("transform.config.triggerTypeSchedule")}</option>
          <option value="dataModeling">{t("transform.config.triggerTypeDataModeling")}</option>
          <option value="recordStream">{t("transform.config.triggerTypeRecordStream")}</option>
        </select>
      </label>

      {triggerType === "schedule" ? (
        <div style={{ marginTop: "0.5rem" }}>
          <ScheduleEditorControl
            cronExpression={String(value.cron_expression ?? "")}
            onChange={(next) => patch({ cron_expression: next })}
          />
        </div>
      ) : null}

      {triggerType === "recordStream" ? (
        <div style={{ marginTop: "0.75rem" }}>
          <RecordStreamTriggerConfigFields value={value} onChange={onChange} />
        </div>
      ) : null}
      {triggerType === "dataModeling" ? (
        <div style={{ marginTop: "0.75rem" }}>
          <DataModelingTriggerConfigFields value={value} onChange={onChange} />
        </div>
      ) : null}

      <label className="gov-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={value.incremental_change_processing !== false}
          onChange={(e) => patch({ incremental_change_processing: e.target.checked })}
        />
        {t("transform.config.incrementalChangeProcessing")}
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transform.config.runId")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.run_id ?? "")}
          onChange={(e) => patch({ run_id: e.target.value })}
          spellCheck={false}
        />
      </label>

      {triggerType === "schedule" ? (
        <TriggerRuleDetailsFields value={value} onChange={onChange} triggerType={triggerType} />
      ) : null}
      <p className="transform-node-editor-modal__hint">{t("transform.config.triggerBuildHint")}</p>
    </div>
  );
}
