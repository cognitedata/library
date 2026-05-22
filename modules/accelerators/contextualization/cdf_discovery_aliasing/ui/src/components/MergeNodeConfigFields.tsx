import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import { FieldPoliciesEditor } from "./FieldPoliciesEditor";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function MergeNodeConfigFields({ t, value, onChange }: Props) {
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const policiesRaw = value.field_policies ?? value.save_field_policies;

  return (
    <div className="discovery-loc-fields">
      <label className="discovery-label discovery-label--block">
        {t("validations.description")}
        <input
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
        />
      </label>

      <div style={{ marginTop: "0.75rem" }}>
        <FieldPoliciesEditor
          t={t}
          policies={policiesRaw}
          onChange={(policies) => patch({ field_policies: policies ?? [] })}
          omitWhenEmpty={false}
          emptyHintKey="flow.merge.fieldPoliciesHint"
        />
      </div>
    </div>
  );
}
