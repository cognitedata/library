import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { CdfTransformationPicker } from "./CdfTransformationPicker";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

/** Link-only editor for ``transformation_ref`` nodes (runs an existing CDF Transformation). */
export function TransformationRefConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const externalId = String(value.transformation_external_id ?? "");

  return (
    <div className="transform-query-fields">
      <p className="transform-query-hint">{t("transform.transformationRef.hint")}</p>
      <label className="transform-query-label transform-query-label--block">
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
      <label className="transform-query-label transform-query-label--block">
        {t("transform.config.transformationExternalId")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={externalId}
          spellCheck={false}
          autoComplete="off"
          onChange={(e) => patch({ transformation_external_id: e.target.value })}
        />
      </label>
      <CdfTransformationPicker
        externalIdValue={externalId}
        onExternalIdChange={(ext) => patch({ transformation_external_id: ext })}
      />
    </div>
  );
}
