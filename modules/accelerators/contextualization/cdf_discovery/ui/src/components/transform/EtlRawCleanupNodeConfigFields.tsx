import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  kind?: "raw_cleanup" | "end";
};

export function EtlRawCleanupNodeConfigFields({ value, onChange, kind = "raw_cleanup" }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const hintKey = kind === "end" ? "transform.end.canvasHint" : "transform.rawCleanup.canvasHint";

  return (
    <div className="transform-node-editor-fields transform-raw-cleanup-fields">
      <p className="transform-node-editor-modal__hint">{t(hintKey)}</p>
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
