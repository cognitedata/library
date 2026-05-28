import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function EtlNodePreviewConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  return (
    <div className="transform-node-editor-fields transform-node-preview-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.nodePreview.canvasHint")}</p>
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
      <label className="gov-label gov-label--block">
        {t("transform.nodePreview.recordKind")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.record_kind ?? "entity")}
          onChange={(e) => patch({ record_kind: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="gov-label gov-label--block">
        {t("transform.nodePreview.rowCap")}
        <input
          className="gov-input"
          type="number"
          min={1}
          style={{ marginTop: "0.35rem" }}
          value={value.row_cap != null ? String(value.row_cap) : "10000"}
          onChange={(e) => patch({ row_cap: Number(e.target.value) || 10000 })}
        />
        <span className="transform-query-hint">{t("transform.nodePreview.rowCapHint")}</span>
      </label>
      <label className="gov-label gov-label--block">
        {t("transform.nodePreview.stableRawTable")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.preview_raw_table_key ?? "")}
          onChange={(e) => patch({ preview_raw_table_key: e.target.value.trim() })}
          spellCheck={false}
          autoComplete="off"
        />
        <span className="transform-query-hint">{t("transform.nodePreview.stableRawTableHint")}</span>
      </label>
    </div>
  );
}
