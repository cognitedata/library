import { useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  readRecordsWriteMode,
  validateRecordsSaveConfig,
} from "../../utils/recordsSaveConfigModel";
import { QueryEditorTabs, useQueryEditorTabState, type QueryEditorTabDef } from "../query/QueryEditorTabs";
import { StreamPickerField } from "../query/StreamPickerField";
import { FieldPoliciesEditor } from "./FieldPoliciesEditor";

const TAB_TARGET = "target";
const TAB_OPTIONS = "options";
const TAB_POLICIES = "policies";

const RECORDS_SAVE_TABS: QueryEditorTabDef[] = [
  { id: TAB_TARGET, labelKey: "transform.save.tabTarget" },
  { id: TAB_OPTIONS, labelKey: "transform.save.tabOptions" },
  { id: TAB_POLICIES, labelKey: "transform.save.tabPolicies" },
];

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

export function RecordsSaveConfigFields({ value, onChange, fieldKey }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const [activeTab, setActiveTab] = useQueryEditorTabState(fieldKey, TAB_TARGET);
  const [streamDetail, setStreamDetail] = useState<JsonObject | null>(null);
  const validation = useMemo(() => validateRecordsSaveConfig(value), [value]);
  const writeMode = readRecordsWriteMode(value);
  const fanIn = String(value.save_fan_in_mode ?? "none").trim() || "none";
  const immutable = streamDetail?.mutable === false;
  const streamId = String(value.stream_external_id ?? "").trim();

  return (
    <div className="transform-node-editor-fields transform-save-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.save.recordsCanvasHint")}</p>
      {validation.issues.length > 0 ? (
        <div className="transform-query-validation" role="alert">
          {validation.issues.map((key) => (
            <p key={key}>{t(key)}</p>
          ))}
        </div>
      ) : null}
      <QueryEditorTabs
        tabs={RECORDS_SAVE_TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`records-save-${fieldKey}`}
      >
        {activeTab === TAB_TARGET ? (
          <>
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
            <StreamPickerField
              streamExternalId={streamId}
              onStreamChange={(id) => patch({ stream_external_id: id })}
              onStreamDetail={setStreamDetail}
            />
            <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
              {t("transform.save.recordsWriteMode")}
              <select
                className="gov-input"
                style={{ marginTop: "0.35rem", maxWidth: "100%" }}
                value={writeMode}
                onChange={(e) => patch({ write_mode: e.target.value })}
              >
                <option value="ingest">{t("transform.save.recordsWriteModeIngest")}</option>
                <option value="upsert">{t("transform.save.recordsWriteModeUpsert")}</option>
                <option value="delete">{t("transform.save.recordsWriteModeDelete")}</option>
              </select>
            </label>
            {immutable && writeMode !== "ingest" ? (
              <p className="transform-query-error" role="alert">
                {t("transform.save.recordsImmutableWarning")}
              </p>
            ) : null}
            <p className="transform-node-editor-modal__hint">{t("transform.save.recordsMappingHint")}</p>
          </>
        ) : null}

        {activeTab === TAB_OPTIONS ? (
          <>
            <label className="gov-label gov-label--block">
              {t("transform.save.batchSize")}
              <input
                className="gov-input"
                type="number"
                min={1}
                style={{ marginTop: "0.35rem" }}
                value={value.batch_size == null ? "" : String(value.batch_size)}
                onChange={(e) => {
                  const v = e.target.value.trim();
                  if (!v) {
                    const next = { ...value };
                    delete next.batch_size;
                    onChange(next);
                    return;
                  }
                  const n = parseInt(v, 10);
                  if (Number.isFinite(n) && n > 0) patch({ batch_size: n });
                }}
              />
            </label>
            <h4 className="gov-modal__title" style={{ fontSize: "0.95rem", marginTop: "1.25rem" }}>
              {t("transform.save.fanInSection")}
            </h4>
            <label className="gov-label gov-label--block">
              {t("flow.saveFanInMode")}
              <select
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={fanIn}
                onChange={(e) => patch({ save_fan_in_mode: e.target.value })}
              >
                <option value="none">{t("flow.saveFanInNone")}</option>
                <option value="merge_per_instance">{t("flow.saveFanInMerge")}</option>
              </select>
            </label>
          </>
        ) : null}

        {activeTab === TAB_POLICIES ? (
          <FieldPoliciesEditor
            t={t}
            policies={value.save_field_policies}
            onChange={(policies) => patch({ save_field_policies: policies })}
            omitWhenEmpty
          />
        ) : null}
      </QueryEditorTabs>
    </div>
  );
}
