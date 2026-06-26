import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  DEFAULT_MAX_PAGES_PER_DETECT_REQUEST,
  DEFAULT_MAX_PAGES_PER_FILE_REFERENCE,
  DEFAULT_MAX_PATTERN_SAMPLES,
  readOptionalPositiveInt,
} from "../../utils/fanoutNodeConfigModel";
import {
  applyEntityTargetPreset,
  type EntityTargetPreset,
} from "../../utils/fileAnnotationNodeConfigModel";
import { fileAnnotationConnectorLabelDefaults } from "../../utils/dualInputConnectorLabels";
import { DualInputConnectorLabelFields } from "./DualInputConnectorLabelFields";
import { FanoutDetectConfigSection, OptionalPositiveIntField } from "./FanoutConfigSections";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function EtlFileAnnotationNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const entityTarget = String(value.entity_target ?? "asset") as EntityTargetPreset;
  const patternModeEnabled = value.pattern_mode !== false;
  const patternNormalization = String(value.pattern_normalization ?? "file_annotation").trim();
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
        {!patternModeEnabled ? (
          <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
            {t("transform.fanoutPlan.searchField")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.search_field ?? "")}
              placeholder={t("transform.fanoutPlan.searchFieldPlaceholder")}
              onChange={(e) => patch({ search_field: e.target.value })}
            />
            <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
              {t("transform.fanoutPlan.searchFieldHintAnnotate")}
            </span>
          </label>
        ) : null}
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fanoutPlan.patternMode")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={patternModeEnabled ? "pattern" : "annotate"}
            onChange={(e) => patch({ pattern_mode: e.target.value === "pattern" })}
          >
            <option value="pattern">{t("transform.fanoutPlan.patternModeOptionPattern")}</option>
            <option value="annotate">{t("transform.fanoutPlan.patternModeOptionAnnotate")}</option>
          </select>
        </label>
        <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.35rem" }}>
          {patternModeEnabled
            ? t("transform.fanoutPlan.patternModePatternHint")
            : t("transform.fanoutPlan.patternModeAnnotateHint")}
        </p>
        {!patternModeEnabled ? null : (
          <>
            <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
              {t("transform.fanoutPlan.patternResourceType")}
              <input
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={String(value.pattern_resource_type ?? "")}
                placeholder={t("transform.fanoutPlan.patternResourceTypePlaceholder")}
                onChange={(e) => patch({ pattern_resource_type: e.target.value })}
              />
              <span
                className="transform-node-editor-modal__hint"
                style={{ display: "block", marginTop: "0.25rem" }}
              >
                {t("transform.fanoutPlan.patternResourceTypeHintPattern")}
              </span>
            </label>
            <OptionalPositiveIntField
              value={value}
              onChange={onChange}
              configKey="max_pattern_samples"
              label={t("transform.fanoutPlan.maxPatternSamples")}
              placeholder={String(DEFAULT_MAX_PATTERN_SAMPLES)}
              hint={t("transform.fanoutPlan.maxPatternSamplesHint")}
            />
            <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
              {t("transform.fanoutPlan.patternNormalization")}
              <select
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={patternNormalization}
                onChange={(e) => patch({ pattern_normalization: e.target.value })}
              >
                <option value="file_annotation">{t("transform.fanoutPlan.patternFileAnnotation")}</option>
                <option value="heuristic_literal">{t("transform.fanoutPlan.patternHeuristicLiteral")}</option>
              </select>
            </label>
          </>
        )}
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
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fileAnnotation.fileExternalIds")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.file_external_ids ?? "")}
            placeholder={t("transform.fileAnnotation.fileExternalIdsPlaceholder")}
            onChange={(e) => patch({ file_external_ids: e.target.value })}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.fileAnnotation.fileExternalIdsHint")}
          </span>
        </label>
      </fieldset>

      <FanoutDetectConfigSection value={value} onChange={onChange} />

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
