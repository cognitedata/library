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

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  flowNodes?: readonly Node[];
};

function numField(
  value: JsonObject,
  key: string,
  onChange: (next: JsonObject) => void,
  label: string,
  hint: string,
  placeholder: string
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
      <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
        {hint}
      </span>
    </label>
  );
}

export function EtlWorkflowFanoutPlanNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

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

        {numField(
          value,
          "batch_size",
          onChange,
          t("transform.fanoutPlan.batchSize"),
          t("transform.fanoutPlan.batchSizeHint"),
          String(DEFAULT_FANOUT_BATCH_SIZE)
        )}

        {numField(
          value,
          "max_files_per_run",
          onChange,
          t("transform.fanoutPlan.maxFilesPerRun"),
          t("transform.fanoutPlan.maxFilesPerRunHint"),
          ""
        )}

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
        <legend>{t("transform.fanoutPlan.sectionPatterns")}</legend>

        {numField(
          value,
          "max_pattern_samples",
          onChange,
          t("transform.fanoutPlan.maxPatternSamples"),
          t("transform.fanoutPlan.maxPatternSamplesHint"),
          String(DEFAULT_MAX_PATTERN_SAMPLES)
        )}

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
      </fieldset>

      <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
        <legend>{t("transform.fanoutPlan.sectionDetect")}</legend>
        <p className="transform-node-editor-modal__hint">{t("transform.fanoutPlan.sectionDetectHint")}</p>

        {numField(
          value,
          "max_pages_per_detect_request",
          onChange,
          t("transform.fanoutPlan.maxPagesPerDetectRequest"),
          t("transform.fanoutPlan.maxPagesPerDetectRequestHint"),
          String(DEFAULT_MAX_PAGES_PER_DETECT_REQUEST)
        )}

        {numField(
          value,
          "max_pages_per_file_reference",
          onChange,
          t("transform.fanoutPlan.maxPagesPerFileReference"),
          t("transform.fanoutPlan.maxPagesPerFileReferenceHint"),
          String(DEFAULT_MAX_PAGES_PER_FILE_REFERENCE)
        )}

        {numField(
          value,
          "min_tokens",
          onChange,
          t("transform.fanoutPlan.minTokens"),
          t("transform.fanoutPlan.minTokensHint"),
          String(DEFAULT_MIN_TOKENS)
        )}

        {numField(
          value,
          "diagram_poll_timeout_sec",
          onChange,
          t("transform.fanoutPlan.pollTimeoutSec"),
          t("transform.fanoutPlan.pollTimeoutSecHint"),
          String(DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC)
        )}

        {numField(
          value,
          "max_attempts",
          onChange,
          t("transform.fanoutPlan.maxAttempts"),
          t("transform.fanoutPlan.maxAttemptsHint"),
          "3"
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
