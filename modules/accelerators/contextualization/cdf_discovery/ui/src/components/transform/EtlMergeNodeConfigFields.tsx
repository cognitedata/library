import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { FieldPoliciesEditor } from "./FieldPoliciesEditor";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function EtlMergeNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const policiesRaw = value.field_policies ?? value.save_field_policies;
  const enabled = value.enabled !== false;

  return (
    <div className="transform-node-editor-fields transform-merge-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.merge.canvasHint")}</p>

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

      <label className="gov-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={enabled}
          onChange={(e) => patch({ enabled: e.target.checked })}
        />
        {t("transform.merge.enabledLabel")}
      </label>
      <p className="transform-node-editor-modal__hint">{t("transform.merge.enabledHint")}</p>

      <div style={{ marginTop: "0.75rem" }}>
        <FieldPoliciesEditor
          t={t}
          policies={policiesRaw}
          onChange={(policies) => {
            const next: JsonObject = { ...value, field_policies: policies ?? [] };
            if ("save_field_policies" in next) {
              const { save_field_policies: _removed, ...rest } = next as JsonObject & {
                save_field_policies?: unknown;
              };
              onChange(rest);
              return;
            }
            onChange(next);
          }}
          omitWhenEmpty={false}
          emptyHintKey="transform.merge.fieldPoliciesHint"
          sectionTitleKey="transform.merge.fieldPoliciesSection"
        />
      </div>
    </div>
  );
}
