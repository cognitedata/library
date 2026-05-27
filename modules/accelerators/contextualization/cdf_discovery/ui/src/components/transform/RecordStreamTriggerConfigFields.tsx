import { useMemo } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  mergeRecordStreamTriggerIntoStart,
  readRecordStreamTriggerFromStart,
} from "../../utils/recordStreamTriggerConfigModel";
import { RecordsFilterEditor } from "../query/RecordsFilterEditor";
import { RecordsSourcesEditor } from "../query/RecordsSourcesEditor";
import { StreamPickerField } from "../query/StreamPickerField";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function RecordStreamTriggerConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const fields = useMemo(() => readRecordStreamTriggerFromStart(value), [value]);

  const patchFields = (patch: Partial<typeof fields>) => {
    onChange(mergeRecordStreamTriggerIntoStart(value, { ...fields, ...patch }));
  };

  const triggerRuleText =
    value.trigger_rule == null
      ? ""
      : typeof value.trigger_rule === "string"
        ? value.trigger_rule
        : JSON.stringify(value.trigger_rule, null, 2);

  return (
    <div className="transform-record-stream-trigger">
      <p className="transform-node-editor-modal__hint">{t("transform.trigger.recordStreamHint")}</p>
      <StreamPickerField
        streamExternalId={fields.streamExternalId}
        onStreamChange={(id) => patchFields({ streamExternalId: id })}
      />
      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.trigger.recordStreamBatchSize")}
        <input
          className="gov-input"
          type="number"
          min={1}
          max={1000}
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={fields.batchSize}
          onChange={(e) => {
            const n = parseInt(e.target.value, 10);
            if (Number.isFinite(n)) patchFields({ batchSize: Math.max(1, Math.min(1000, n)) });
          }}
        />
      </label>
      <label className="gov-label gov-label--block">
        {t("transform.trigger.recordStreamBatchTimeout")}
        <input
          className="gov-input"
          type="number"
          min={10}
          max={86400}
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={fields.batchTimeout}
          onChange={(e) => {
            const n = parseInt(e.target.value, 10);
            if (Number.isFinite(n)) patchFields({ batchTimeout: Math.max(10, Math.min(86400, n)) });
          }}
        />
      </label>
      <details style={{ marginTop: "0.75rem" }}>
        <summary>{t("transform.trigger.recordStreamSourcesSection")}</summary>
        <RecordsSourcesEditor
          sources={fields.sources}
          onChange={(sources) => patchFields({ sources })}
        />
      </details>
      <details style={{ marginTop: "0.75rem" }}>
        <summary>{t("transform.trigger.recordStreamFilterSection")}</summary>
        <RecordsFilterEditor filter={fields.filter} onChange={(filter) => patchFields({ filter })} />
      </details>
      <details style={{ marginTop: "0.75rem" }}>
        <summary>{t("transform.trigger.recordStreamAdvanced")}</summary>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.config.triggerRuleJson")}
          <textarea
            className="gov-input gov-input--mono"
            style={{ marginTop: "0.35rem", minHeight: "8rem" }}
            value={triggerRuleText}
            onChange={(e) => {
              const raw = e.target.value.trim();
              if (!raw) {
                const next = { ...value };
                delete next.trigger_rule;
                onChange(next);
                return;
              }
              try {
                const parsed = JSON.parse(raw);
                if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
                  onChange(mergeRecordStreamTriggerIntoStart(value, readRecordStreamTriggerFromStart({ ...value, trigger_rule: parsed })));
                }
              } catch {
                onChange({ ...value, trigger_rule: raw });
              }
            }}
            spellCheck={false}
          />
        </label>
      </details>
    </div>
  );
}
