import type { Node } from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC,
  DEFAULT_FANOUT_BATCH_SIZE,
  DEFAULT_MAX_PAGES_PER_DETECT_REQUEST,
  DEFAULT_MAX_PAGES_PER_FILE_REFERENCE,
  DEFAULT_MAX_PATTERN_SAMPLES,
  DEFAULT_MIN_TOKENS,
  readOptionalPositiveInt,
} from "../../utils/fanoutNodeConfigModel";
import { fanoutPlanConnectorLabelDefaults } from "../../utils/dualInputConnectorLabels";
import { DualInputConnectorLabelFields } from "./DualInputConnectorLabelFields";
import { FanoutDetectConfigSection, OptionalPositiveIntField } from "./FanoutConfigSections";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  flowNodes?: readonly Node[];
};

export function EtlWorkflowFanoutPlanNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const fanoutMode = String(value.fanout_mode ?? "both").trim().toLowerCase();
  const patternModeEnabled = value.pattern_mode !== false;
  const patternMode = String(value.pattern_normalization ?? "file_annotation").trim();
  const fanoutProfile = String(value.fanout_profile ?? "file_annotation");

  return (
    <div className="transform-node-editor-fields transform-fanout-plan-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.fanoutPlan.canvasHint")}</p>
      <p className="transform-node-editor-modal__hint">{t("transform.fanoutPlan.wiringHint")}</p>
      <p className="transform-node-editor-modal__hint">{t("transform.fanoutPlan.functionLimitHint")}</p>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.fanoutPlan.fanoutProfile")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.fanout_profile ?? "file_annotation")}
          onChange={(e) => patch({ fanout_profile: e.target.value })}
        >
          <option value="file_annotation">{t("transform.fanoutPlan.profileFileAnnotation")}</option>
        </select>
      </label>

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

      <DualInputConnectorLabelFields
        value={value}
        onChange={onChange}
        defaults={fanoutPlanConnectorLabelDefaults(fanoutProfile)}
      />

      <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
        <legend>{t("transform.fanoutPlan.sectionPlanning")}</legend>

        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.fileAnnotation.fileIds")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.file_ids ?? "")}
            placeholder={t("transform.fileAnnotation.fileIdsPlaceholder")}
            onChange={(e) => patch({ file_ids: e.target.value })}
            spellCheck={false}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.fileAnnotation.fileIdsHint")}
          </span>
        </label>

        <OptionalPositiveIntField
          value={value}
          onChange={onChange}
          configKey="batch_size"
          label={t("transform.fanoutPlan.batchSize")}
          hint={t("transform.fanoutPlan.batchSizeHint")}
          placeholder={String(DEFAULT_FANOUT_BATCH_SIZE)}
        />
        <OptionalPositiveIntField
          value={value}
          onChange={onChange}
          configKey="max_files_per_run"
          label={t("transform.fanoutPlan.maxFilesPerRun")}
          hint={t("transform.fanoutPlan.maxFilesPerRunHint")}
          placeholder=""
        />

        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fanoutPlan.childFunction")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.child_function_external_id ?? "")}
            placeholder={t("transform.fanout.functionPlaceholder")}
            onChange={(e) => patch({ child_function_external_id: e.target.value })}
            spellCheck={false}
          />
        </label>
      </fieldset>

      <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
        <legend>{t("transform.fanoutPlan.sectionEntities")}</legend>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.fanoutPlan.fanoutMode")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={fanoutMode}
            onChange={(e) => {
              const next = e.target.value;
              if (next === "pattern") {
                patch({ fanout_mode: "pattern", pattern_mode: true });
              } else if (next === "annotation") {
                patch({ fanout_mode: "annotation", pattern_mode: false });
              } else {
                patch({ fanout_mode: "both" });
              }
            }}
          >
            <option value="both">{t("transform.fanoutPlan.fanoutModeOptionBoth")}</option>
            <option value="pattern">{t("transform.fanoutPlan.fanoutModeOptionPattern")}</option>
            <option value="annotation">{t("transform.fanoutPlan.fanoutModeOptionAnnotation")}</option>
          </select>
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.fanoutPlan.fanoutModeHint")}
          </span>
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
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
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fanoutPlan.entitiesProperty")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.patterns_entity_property ?? "")}
            placeholder={t("transform.fanoutPlan.entitiesPropertyPlaceholder")}
            onChange={(e) => patch({ patterns_entity_property: e.target.value })}
            spellCheck={false}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {fanoutMode === "both"
              ? t("transform.fanoutPlan.fanoutModeHint")
              : patternModeEnabled
              ? t("transform.fanoutPlan.entitiesPropertyHintPattern")
              : t("transform.fanoutPlan.entitiesPropertyHintAnnotate")}
          </span>
        </label>
        {!patternModeEnabled || fanoutMode === "both" ? (
          <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
            {t("transform.fanoutPlan.searchField")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.search_field ?? "")}
              placeholder={t("transform.fanoutPlan.searchFieldPlaceholder")}
              onChange={(e) => patch({ search_field: e.target.value })}
              spellCheck={false}
            />
            <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
              {t("transform.fanoutPlan.searchFieldHintAnnotate")}
            </span>
          </label>
        ) : null}
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transform.fanoutPlan.patternResourceType")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.pattern_resource_type ?? "")}
            placeholder={t("transform.fanoutPlan.patternResourceTypePlaceholder")}
            onChange={(e) => patch({ pattern_resource_type: e.target.value })}
            spellCheck={false}
          />
          <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {patternModeEnabled
              ? t("transform.fanoutPlan.patternResourceTypeHintPattern")
              : t("transform.fanoutPlan.patternResourceTypeHintAnnotate")}
          </span>
        </label>

        {patternModeEnabled ? (
          <>
            <OptionalPositiveIntField
              value={value}
              onChange={onChange}
              configKey="max_pattern_samples"
              label={t("transform.fanoutPlan.maxPatternSamples")}
              hint={t("transform.fanoutPlan.maxPatternSamplesHint")}
              placeholder={String(DEFAULT_MAX_PATTERN_SAMPLES)}
            />

            <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
              {t("transform.fanoutPlan.patternNormalization")}
              <select
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={patternMode}
                onChange={(e) => patch({ pattern_normalization: e.target.value })}
              >
                <option value="file_annotation">{t("transform.fanoutPlan.patternFileAnnotation")}</option>
                <option value="heuristic_literal">{t("transform.fanoutPlan.patternHeuristicLiteral")}</option>
              </select>
            </label>
          </>
        ) : null}
      </fieldset>

      <FanoutDetectConfigSection
        value={value}
        onChange={onChange}
        showSectionHint
        includeMaxAttempts
      />

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
