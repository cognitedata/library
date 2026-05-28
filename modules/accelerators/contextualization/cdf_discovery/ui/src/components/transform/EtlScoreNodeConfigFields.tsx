import { useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  parseScoringRuleRows,
  readScoreFields,
  readScoringRules,
  serializeScoringRuleRows,
} from "../../utils/scoreNodeConfigModel";
import { ScoringRulesEditor } from "./ScoringRulesEditor";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

function fieldsToText(fields: string[]): string {
  return fields.join(", ");
}

function textToFields(raw: string): string[] {
  return raw
    .split(/[,;]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export function EtlScoreNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const fields = readScoreFields(value as Record<string, unknown>);
  const [fieldsDraft, setFieldsDraft] = useState(() => fieldsToText(fields));

  useEffect(() => {
    setFieldsDraft(fieldsToText(fields));
  }, [fields.join("\0")]);

  const rules = useMemo(
    () => parseScoringRuleRows(readScoringRules(value as Record<string, unknown>)),
    [value]
  );

  const commitFields = (raw: string) => {
    const nextFields = textToFields(raw);
    patch({
      score_fields: nextFields.length ? nextFields : undefined,
      score_field: undefined,
    });
    setFieldsDraft(fieldsToText(nextFields));
  };

  return (
    <div className="transform-node-editor-fields transform-score-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.score.canvasHint")}</p>

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
        {t("transform.score.scoreFields")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={fieldsDraft}
          onChange={(e) => setFieldsDraft(e.target.value)}
          onBlur={() => commitFields(fieldsDraft)}
          onKeyDown={(e) => {
            if (e.key === "Enter") commitFields(fieldsDraft);
          }}
          placeholder={t("transform.score.scoreFieldsPlaceholder")}
          spellCheck={false}
        />
      </label>
      <p className="transform-node-editor-modal__hint">{t("transform.score.scoreFieldsHint")}</p>

      <div
        className="transform-flow-inspector__field transform-flow-inspector__field--field-pair"
        style={{ marginTop: "0.75rem", flexWrap: "wrap" }}
      >
        <label className="gov-label">
          {t("transform.score.initialScore")}
          <input
            className="gov-input"
            type="number"
            step="any"
            min={0}
            max={1}
            value={String(value.initial_score ?? 1.0)}
            onChange={(e) => patch({ initial_score: Number(e.target.value) })}
          />
        </label>
        <label className="gov-label">
          {t("transform.score.minScore")}
          <input
            className="gov-input"
            type="number"
            step="any"
            min={0}
            max={1}
            value={String(value.min_score ?? 0.0)}
            onChange={(e) => patch({ min_score: Number(e.target.value) })}
          />
        </label>
      </div>

      <div className="transform-flow-inspector__field" style={{ marginTop: "0.75rem", flexWrap: "wrap" }}>
        <label className="gov-label" style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={value.min_threshold_filter_enabled === true}
            onChange={(e) =>
              patch({
                min_threshold_filter_enabled: e.target.checked,
                ...(e.target.checked && value.min_threshold == null
                  ? { min_threshold: value.min_score ?? 0.0 }
                  : {}),
              })
            }
          />
          {t("transform.score.minThresholdFilterEnabled")}
        </label>
        {value.min_threshold_filter_enabled === true ? (
          <label className="gov-label">
            {t("transform.score.minThreshold")}
            <input
              className="gov-input"
              type="number"
              step="any"
              min={0}
              max={1}
              value={String(value.min_threshold ?? value.min_score ?? 0.0)}
              onChange={(e) => patch({ min_threshold: Number(e.target.value) })}
            />
          </label>
        ) : null}
      </div>
      <p className="transform-node-editor-modal__hint">{t("transform.score.minThresholdFilterHint")}</p>

      <ScoringRulesEditor
        rules={rules}
        onChange={(nextRules) => {
          const serialized = serializeScoringRuleRows(nextRules);
          const next: JsonObject = {
            ...value,
            scoring_rules: serialized,
          };
          delete next.score_rules;
          onChange(next);
        }}
      />
    </div>
  );
}
