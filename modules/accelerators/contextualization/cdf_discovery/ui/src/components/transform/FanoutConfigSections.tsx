import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC,
  DEFAULT_MAX_PAGES_PER_DETECT_REQUEST,
  DEFAULT_MAX_PAGES_PER_FILE_REFERENCE,
  DEFAULT_MIN_TOKENS,
} from "../../utils/fanoutNodeConfigModel";

type OptionalPositiveIntFieldProps = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  configKey: string;
  label: string;
  placeholder: string;
  hint?: string;
};

export function OptionalPositiveIntField({
  value,
  onChange,
  configKey,
  label,
  placeholder,
  hint,
}: OptionalPositiveIntFieldProps) {
  const raw = value[configKey];
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
            delete next[configKey];
            onChange(next);
            return;
          }
          const n = parseInt(v, 10);
          if (Number.isFinite(n) && n > 0) onChange({ ...value, [configKey]: n });
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

type FanoutDetectConfigSectionProps = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  showSectionHint?: boolean;
  includeMaxAttempts?: boolean;
};

export function FanoutDetectConfigSection({
  value,
  onChange,
  showSectionHint = false,
  includeMaxAttempts = false,
}: FanoutDetectConfigSectionProps) {
  const { t } = useAppSettings();
  const rawDiagramDetectConfig = value.diagram_detect_config;
  const diagramDetectConfigText =
    rawDiagramDetectConfig == null
      ? ""
      : typeof rawDiagramDetectConfig === "string"
        ? rawDiagramDetectConfig
        : JSON.stringify(rawDiagramDetectConfig, null, 2);

  return (
    <fieldset className="transform-node-editor-fields__section" style={{ marginTop: "1rem" }}>
      <legend>{t("transform.fanoutPlan.sectionDetect")}</legend>
      {showSectionHint ? <p className="transform-node-editor-modal__hint">{t("transform.fanoutPlan.sectionDetectHint")}</p> : null}
      <OptionalPositiveIntField
        value={value}
        onChange={onChange}
        configKey="max_pages_per_detect_request"
        label={t("transform.fanoutPlan.maxPagesPerDetectRequest")}
        placeholder={String(DEFAULT_MAX_PAGES_PER_DETECT_REQUEST)}
        hint={t("transform.fanoutPlan.maxPagesPerDetectRequestHint")}
      />
      <OptionalPositiveIntField
        value={value}
        onChange={onChange}
        configKey="max_pages_per_file_reference"
        label={t("transform.fanoutPlan.maxPagesPerFileReference")}
        placeholder={String(DEFAULT_MAX_PAGES_PER_FILE_REFERENCE)}
        hint={t("transform.fanoutPlan.maxPagesPerFileReferenceHint")}
      />
      <OptionalPositiveIntField
        value={value}
        onChange={onChange}
        configKey="min_tokens"
        label={t("transform.fanoutPlan.minTokens")}
        placeholder={String(DEFAULT_MIN_TOKENS)}
        hint={t("transform.fanoutPlan.minTokensHint")}
      />
      <OptionalPositiveIntField
        value={value}
        onChange={onChange}
        configKey="diagram_poll_timeout_sec"
        label={t("transform.fanoutPlan.pollTimeoutSec")}
        placeholder={String(DEFAULT_DIAGRAM_POLL_TIMEOUT_SEC)}
        hint={t("transform.fanoutPlan.pollTimeoutSecHint")}
      />
      {includeMaxAttempts ? (
        <OptionalPositiveIntField
          value={value}
          onChange={onChange}
          configKey="max_attempts"
          label={t("transform.fanoutPlan.maxAttempts")}
          placeholder="3"
          hint={t("transform.fanoutPlan.maxAttemptsHint")}
        />
      ) : null}
      <OptionalPositiveIntField
        value={value}
        onChange={onChange}
        configKey="max_detect_jobs_per_invocation"
        label={t("transform.fanoutPlan.maxDetectJobsPerInvocation")}
        placeholder="1"
        hint={t("transform.fanoutPlan.maxDetectJobsPerInvocationHint")}
      />
      <label className="gov-label gov-label--block transform-flow-inspector__field--checkbox" style={{ marginTop: "0.75rem" }}>
        <span>{t("transform.fanoutPlan.partialMatch")}</span>
        <input
          type="checkbox"
          checked={value.partial_match !== false}
          onChange={(e) => onChange({ ...value, partial_match: e.target.checked })}
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
  );
}
