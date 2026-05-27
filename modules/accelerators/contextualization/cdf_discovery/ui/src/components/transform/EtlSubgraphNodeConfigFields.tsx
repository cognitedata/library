import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function EtlSubgraphNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  return (
    <div className="transform-node-editor-fields transform-subgraph-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.subgraph.canvasHint")}</p>
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
    </div>
  );
}
