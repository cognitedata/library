import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import { QueryEditorTabs, useQueryEditorTabState, type QueryEditorTabDef } from "../query/QueryEditorTabs";
import { ViewQueryConfigFields } from "../query/ViewQueryConfigFields";
import { FieldPoliciesEditor } from "./FieldPoliciesEditor";

const DEFAULT_SAVE_BATCH_SIZE = 500;

const TAB_TARGET = "target";
const TAB_OPTIONS = "options";
const TAB_POLICIES = "policies";

const SAVE_TABS: QueryEditorTabDef[] = [
  { id: TAB_TARGET, labelKey: "transform.save.tabTarget" },
  { id: TAB_OPTIONS, labelKey: "transform.save.tabOptions" },
  { id: TAB_POLICIES, labelKey: "transform.save.tabPolicies" },
];

type Props = {
  kind: TransformCanvasNodeKind;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
  schemaSpace?: string;
};

function saveBatchSizeField(
  value: JsonObject,
  onChange: (next: JsonObject) => void,
  label: string,
  hint: string
) {
  const raw = value.batch_size;
  const display = raw === undefined || raw === null ? "" : String(raw);
  return (
    <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
      {label}
      <input
        className="gov-input"
        style={{ marginTop: "0.35rem" }}
        type="number"
        min={1}
        placeholder={String(DEFAULT_SAVE_BATCH_SIZE)}
        value={display}
        onChange={(e) => {
          const v = e.target.value.trim();
          if (!v) {
            const next = { ...value };
            delete next.batch_size;
            onChange(next);
            return;
          }
          const n = parseInt(v, 10);
          if (Number.isFinite(n) && n > 0) onChange({ ...value, batch_size: n });
        }}
      />
      <span className="transform-node-editor-modal__hint" style={{ display: "block", marginTop: "0.25rem" }}>
        {hint}
      </span>
    </label>
  );
}

export function EtlSaveNodeConfigFields({ kind, value, onChange, fieldKey, schemaSpace }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const fanIn = String(value.save_fan_in_mode ?? "none").trim() || "none";
  const [activeTab, setActiveTab] = useQueryEditorTabState(fieldKey, TAB_TARGET);

  return (
    <div className="transform-node-editor-fields transform-save-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.save.canvasHint")}</p>
      <QueryEditorTabs
        tabs={SAVE_TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`save-node-${fieldKey}`}
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
                autoComplete="off"
              />
            </label>

            {kind === "save_view" ? (
              <div style={{ marginTop: "1rem" }}>
                <ViewQueryConfigFields
                  value={value}
                  onChange={onChange}
                  fieldKey={fieldKey}
                  schema_space={schemaSpace}
                  variant="viewTarget"
                />
              </div>
            ) : null}

            {kind === "save_raw" ? (
              <>
                <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
                  {t("transform.config.rawDb")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={String(value.source_raw_db ?? value.raw_db ?? "")}
                    onChange={(e) => patch({ source_raw_db: e.target.value })}
                    spellCheck={false}
                  />
                </label>
                <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
                  {t("transform.config.rawTable")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={String(value.source_raw_table_key ?? value.raw_table_key ?? "")}
                    onChange={(e) => patch({ source_raw_table_key: e.target.value })}
                    spellCheck={false}
                  />
                </label>
              </>
            ) : null}

            {kind === "save_classic" ? (
              <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
                {t("transform.config.resourceType")}
                <input
                  className="gov-input"
                  style={{ marginTop: "0.35rem" }}
                  value={String(value.resource_type ?? "assets")}
                  onChange={(e) => patch({ resource_type: e.target.value })}
                  spellCheck={false}
                />
              </label>
            ) : null}
          </>
        ) : null}

        {activeTab === TAB_OPTIONS ? (
          <>
            {saveBatchSizeField(value, onChange, t("transform.save.batchSize"), t("transform.save.batchSizeHint"))}

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
            <p className="transform-node-editor-modal__hint">{t("flow.saveFanInHint")}</p>
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
