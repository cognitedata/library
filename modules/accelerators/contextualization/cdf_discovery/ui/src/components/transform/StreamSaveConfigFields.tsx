import { useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  hasStreamDefinitionOverride,
  readStreamSources,
  validateStreamSaveConfig,
} from "../../utils/streamSaveConfigModel";
import { QueryEditorTabs, type QueryEditorTabDef } from "../query/QueryEditorTabs";
import { RecordsSourcesEditor } from "../query/RecordsSourcesEditor";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

const TAB_CONFIG = "config";
const TAB_DEFINITION = "definition";
const TAB_VALIDATE = "validate";

const TABS: QueryEditorTabDef[] = [
  { id: TAB_CONFIG, labelKey: "transform.query.tabConfig" },
  { id: TAB_DEFINITION, labelKey: "transform.save.streamTabDefinition" },
  { id: TAB_VALIDATE, labelKey: "transform.save.streamTabValidate" },
];

export function StreamSaveConfigFields({ value, onChange, fieldKey }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const [activeTab, setActiveTab] = useState(TAB_CONFIG);
  const [advancedJson, setAdvancedJson] = useState(() => {
    const raw = value.stream_definition;
    if (raw == null) return "";
    return typeof raw === "string" ? raw : JSON.stringify(raw, null, 2);
  });
  const validation = useMemo(() => validateStreamSaveConfig(value), [value]);
  const advancedWins = hasStreamDefinitionOverride(value);

  useEffect(() => {
    setActiveTab(TAB_CONFIG);
  }, [fieldKey]);

  const applyAdvanced = () => {
    const raw = advancedJson.trim();
    if (!raw) {
      const next = { ...value };
      delete next.stream_definition;
      onChange(next);
      return;
    }
    try {
      patch({ stream_definition: JSON.parse(raw) });
    } catch {
      patch({ stream_definition: raw });
    }
  };

  return (
    <div className="transform-query-fields-wrap transform-save-stream-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.save.streamCanvasHint")}</p>
      <QueryEditorTabs
        tabs={TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`stream-save-${fieldKey}`}
      >
      {activeTab === TAB_CONFIG ? (
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
          <p className="transform-query-hint">{t("transform.save.streamOperationCreate")}</p>
          <label className="gov-label gov-label--block">
            {t("transform.save.streamExternalId")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.stream_external_id ?? "")}
              onChange={(e) => patch({ stream_external_id: e.target.value, operation: "create" })}
              spellCheck={false}
            />
          </label>
          <label className="gov-label gov-label--block">
            {t("transform.save.streamSpace")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.stream_space ?? value.space ?? "")}
              onChange={(e) => patch({ stream_space: e.target.value, space: e.target.value })}
              spellCheck={false}
            />
          </label>
          <label className="gov-label gov-label--block">
            {t("transform.save.streamDisplayName")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.name ?? "")}
              onChange={(e) => patch({ name: e.target.value })}
              spellCheck={false}
            />
          </label>
        </>
      ) : null}
      {activeTab === TAB_DEFINITION ? (
        <>
          <label className="gov-label gov-label--block">
            {t("transform.save.streamTemplate")}
            <select
              className="gov-input"
              style={{ marginTop: "0.35rem", maxWidth: "100%" }}
              value={String(value.template ?? "")}
              onChange={(e) => patch({ template: e.target.value || undefined })}
            >
              <option value="">{t("transform.save.streamTemplateCustom")}</option>
              <option value="ImmutableTestStream">ImmutableTestStream</option>
              <option value="MutableStream">MutableStream</option>
            </select>
          </label>
          <label className="gov-label" style={{ display: "flex", alignItems: "center", gap: 8, marginTop: "0.5rem" }}>
            <input
              type="checkbox"
              checked={value.mutable === true}
              onChange={(e) => patch({ mutable: e.target.checked })}
            />
            {t("transform.save.streamMutable")}
          </label>
          <RecordsSourcesEditor
            sources={readStreamSources(value)}
            onChange={(next) => patch({ sources: next.length ? next : undefined })}
          />
          <details style={{ marginTop: "1rem" }}>
            <summary>{t("transform.save.streamAdvancedJson")}</summary>
            {advancedWins ? (
              <p className="transform-query-hint">{t("transform.save.streamAdvancedOverrides")}</p>
            ) : null}
            <textarea
              className="gov-input gov-input--mono"
              style={{ marginTop: "0.35rem", minHeight: "10rem", width: "100%" }}
              value={advancedJson}
              onChange={(e) => setAdvancedJson(e.target.value)}
              onBlur={applyAdvanced}
              spellCheck={false}
            />
          </details>
        </>
      ) : null}
      {activeTab === TAB_VALIDATE ? (
        <div>
          {validation.issues.length === 0 ? (
            <p className="transform-query-hint">{t("transform.save.streamValidateOk")}</p>
          ) : (
            <div className="transform-query-validation" role="alert">
              {validation.issues.map((key) => (
                <p key={key}>{t(key)}</p>
              ))}
            </div>
          )}
        </div>
      ) : null}
      </QueryEditorTabs>
      <p className="transform-node-editor-modal__hint" style={{ marginTop: "1rem" }}>
        {t("transform.save.streamNoPredecessors")}
      </p>
    </div>
  );
}
