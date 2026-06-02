import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC,
  DEFAULT_MAX_PAGES_PER_DETECT_REQUEST,
  DEFAULT_MAX_PAGES_PER_FILE_REFERENCE,
  DEFAULT_MIN_TOKENS,
  readOptionalPositiveInt,
} from "../../utils/fanoutNodeConfigModel";
import {
  applyEntityTargetPreset,
  type EntityTargetPreset,
} from "../../utils/fileAnnotationNodeConfigModel";
import { fileAnnotationConnectorLabelDefaults } from "../../utils/dualInputConnectorLabels";
import { DualInputConnectorLabelFields } from "./DualInputConnectorLabelFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

function optionalPositiveNumberField(
  value: JsonObject,
  key: string,
  onChange: (next: JsonObject) => void,
  label: string,
  placeholder: string,
  hint?: string
) {
  const raw = value[key];
  const display = raw === undefined || raw === null ? "" : String(raw);
  return (
    <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
      {label}
      <input
        className="gov-input"
        style={{ marginTop: "0.35rem" }}
        type="number"
        min={1}
        placeholder={placeholder}
        value={display}
        onChange={(e) => {
          const v = e.target.value.trim();
          if (!v) {
            const next = { ...value };
            delete next[key];
            onChange(next);
            return;
          }
          const n = parseInt(v, 10);
          if (Number.isFinite(n) && n > 0) onChange({ ...value, [key]: n });
        }}
      />
      {hint ? (
        <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {hint}
        </span>
      ) : null}
    </label>
  );
}

export function EtlFileAnnotationNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const entityTarget = String(value.entity_target ?? "asset") as EntityTargetPreset;
  const rawDiagramDetectConfig = value.diagram_detect_config;
  const diagramDetectConfigText =
    rawDiagramDetectConfig == null
      ? ""
      : typeof rawDiagramDetectConfig === "string"
        ? rawDiagramDetectConfig
        : JSON.stringify(rawDiagramDetectConfig, null, 2);

  return (
    <div className="transform-node-editor-fields transform-file-annotation-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.fileAnnotation.canvasHint")}</p>
      <p className="transform-node-editor-modal__hint">{t("transform.fileAnnotation.wiringHint")}</p>

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

      <DualInputConnectorLabelFields
        value={value}
        onChange={onChange}
        defaults={fileAnnotationConnectorLabelDefaults()}
      />

      <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
        <legend>{t("transform.fileAnnotation.sectionContext")}</legend>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.fileAnnotation.entityTarget")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={entityTarget}
            onChange={(e) =>
              onChange(applyEntityTargetPreset(value, e.target.value as EntityTargetPreset))
            }
          >
            <option value="asset">{t("transform.fileAnnotation.entityTargetAsset")}</option>
            <option value="file">{t("transform.fileAnnotation.entityTargetFile")}</option>
            <option value="custom">{t("transform.fileAnnotation.entityTargetCustom")}</option>
          </select>
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fileAnnotation.patternsProperty")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.patterns_entity_property ?? "")}
            onChange={(e) => patch({ patterns_entity_property: e.target.value })}
            disabled={entityTarget !== "custom"}
          />
        </label>
        <label
          className="gov-label gov-label--block transform-flow-inspector__field--checkbox"
          style={{ marginTop: "0.75rem" }}
        >
          <span>{t("transform.fileAnnotation.patternMode")}</span>
          <input
            type="checkbox"
            checked={value.pattern_mode !== false}
            onChange={(e) => patch({ pattern_mode: e.target.checked })}
          />
        </label>
      </fieldset>

      <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
        <legend>{t("transform.fileAnnotation.sectionFiles")}</legend>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.fileAnnotation.fileIds")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.file_ids ?? "")}
            placeholder={t("transform.fileAnnotation.fileIdsPlaceholder")}
            onChange={(e) => patch({ file_ids: e.target.value })}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.fileAnnotation.fileIdsHint")}
          </span>
        </label>
      </fieldset>

      <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
        <legend>{t("transform.fanoutPlan.sectionDetect")}</legend>
        {optionalPositiveNumberField(
          value,
          "max_pages_per_detect_request",
          onChange,
          t("transform.fanoutPlan.maxPagesPerDetectRequest"),
          String(DEFAULT_MAX_PAGES_PER_DETECT_REQUEST),
          t("transform.fanoutPlan.maxPagesPerDetectRequestHint")
        )}
        {optionalPositiveNumberField(
          value,
          "max_pages_per_file_reference",
          onChange,
          t("transform.fanoutPlan.maxPagesPerFileReference"),
          String(DEFAULT_MAX_PAGES_PER_FILE_REFERENCE),
          t("transform.fanoutPlan.maxPagesPerFileReferenceHint")
        )}
        {optionalPositiveNumberField(
          value,
          "min_tokens",
          onChange,
          t("transform.fanoutPlan.minTokens"),
          String(DEFAULT_MIN_TOKENS),
          t("transform.fanoutPlan.minTokensHint")
        )}
        {optionalPositiveNumberField(
          value,
          "diagram_poll_timeout_sec",
          onChange,
          t("transform.fanoutPlan.pollTimeoutSec"),
          String(DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC),
          t("transform.fanoutPlan.pollTimeoutSecHint")
        )}
        {optionalPositiveNumberField(
          value,
          "max_detect_jobs_per_invocation",
          onChange,
          t("transform.fanoutPlan.maxDetectJobsPerInvocation"),
          "1",
          t("transform.fanoutPlan.maxDetectJobsPerInvocationHint")
        )}
        <label
          className="gov-label gov-label--block transform-flow-inspector__field--checkbox"
          style={{ marginTop: "0.75rem" }}
        >
          <span>{t("transform.fanoutPlan.partialMatch")}</span>
          <input
            type="checkbox"
            checked={value.partial_match !== false}
            onChange={(e) => patch({ partial_match: e.target.checked })}
          />
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fanoutPlan.diagramDetectConfig")}
          <textarea
            className="gov-input transform-flow-inspector__json"
            rows={5}
            style={{ marginTop: "0.35rem" }}
            value={diagramDetectConfigText}
            placeholder={t("transform.fanoutPlan.diagramDetectConfigPlaceholder")}
            spellCheck={false}
            onChange={(e) => {
              const v = e.target.value.trim();
              if (!v) {
                const next = { ...value };
                delete next.diagram_detect_config;
                onChange(next);
                return;
              }
              try {
                onChange({ ...value, diagram_detect_config: JSON.parse(v) as JsonObject });
              } catch {
                onChange({ ...value, diagram_detect_config: e.target.value });
              }
            }}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.fanoutPlan.diagramDetectConfigHint")}
          </span>
        </label>
      </fieldset>

      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.75rem" }}>
        {t("transform.fanoutPlan.pagesSummary", {
          perCall: String(
            readOptionalPositiveInt(value.max_pages_per_detect_request) ??
              DEFAULT_MAX_PAGES_PER_DETECT_REQUEST
          ),
          perRef: String(
            readOptionalPositiveInt(value.max_pages_per_file_reference) ??
              DEFAULT_MAX_PAGES_PER_FILE_REFERENCE
          ),
        })}
      </p>
    </div>
  );
}
