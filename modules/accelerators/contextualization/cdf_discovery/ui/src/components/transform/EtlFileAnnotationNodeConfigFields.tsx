import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC,
  DEFAULT_MAX_PAGES_PER_DETECT_REQUEST,
  DEFAULT_MAX_PAGES_PER_FILE_REFERENCE,
  DEFAULT_MAX_PATTERN_SAMPLES,
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

export function EtlFileAnnotationNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const entityTarget = String(value.entity_target ?? "asset") as EntityTargetPreset;

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
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.fanoutPlan.maxPagesPerDetectRequest")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            type="number"
            min={1}
            value={String(value.max_pages_per_detect_request ?? DEFAULT_MAX_PAGES_PER_DETECT_REQUEST)}
            onChange={(e) => patch({ max_pages_per_detect_request: parseInt(e.target.value, 10) })}
          />
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fanoutPlan.minTokens")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            type="number"
            min={1}
            value={String(value.min_tokens ?? DEFAULT_MIN_TOKENS)}
            onChange={(e) => patch({ min_tokens: parseInt(e.target.value, 10) })}
          />
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fanoutPlan.pollTimeoutSec")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            type="number"
            min={60}
            value={String(value.diagram_poll_timeout_sec ?? DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC)}
            onChange={(e) => patch({ diagram_poll_timeout_sec: parseInt(e.target.value, 10) })}
          />
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
