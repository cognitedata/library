import { useAppSettings } from "../../../context/AppSettingsContext";
import type { JsonObject } from "../../../types/jsonConfig";
import {
  DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE,
  LOOKUP_KEY_NORMALIZATION_OPTIONS,
  isLookupKeyNormalization,
  type LookupKeyNormalization,
} from "../../../utils/buildIndexHandlerTemplates";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function AnnotationVertexIndexHandlerFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const norm = String(value.lookup_key_normalization ?? "strip_casefold").trim();
  const normalization = isLookupKeyNormalization(norm) ? norm : "strip_casefold";

  return (
    <div className="transform-build-index-handler-fields">
      <label className="gov-label gov-label--block">
        {t("buildIndex.lookupKeyNormalization")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem", maxWidth: "16rem" }}
          value={normalization}
          onChange={(e) => patch({ lookup_key_normalization: e.target.value })}
        >
          {LOOKUP_KEY_NORMALIZATION_OPTIONS.map((opt: LookupKeyNormalization) => (
            <option key={opt} value={opt}>
              {t(`buildIndex.lookupKeyNormalization.${opt}`)}
            </option>
          ))}
        </select>
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("buildIndex.tokenInitialConfidence")}
        <input
          className="gov-input"
          type="number"
          step="any"
          min={0}
          style={{ marginTop: "0.35rem", maxWidth: "8rem" }}
          value={String(value.token_initial_confidence ?? 1.0)}
          onChange={(e) => patch({ token_initial_confidence: Number(e.target.value) })}
        />
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("buildIndex.rowKeyTemplate")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.row_key_template ?? DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE)}
          onChange={(e) => patch({ row_key_template: e.target.value })}
          spellCheck={false}
        />
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("buildIndex.querySource")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.query_source ?? "build_index")}
          onChange={(e) => patch({ query_source: e.target.value })}
          spellCheck={false}
        />
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("buildIndex.defaultViewVersion")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem", maxWidth: "8rem" }}
          value={String(value.default_view_version ?? "v1")}
          onChange={(e) => patch({ default_view_version: e.target.value })}
          spellCheck={false}
        />
      </label>

      <label className="gov-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={value.include_region_vertices !== false}
          onChange={(e) => patch({ include_region_vertices: e.target.checked })}
        />
        {t("buildIndex.annotation.includeRegionVertices")}
      </label>

      <label className="gov-label" style={{ marginTop: "0.5rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={value.include_bounding_box !== false}
          onChange={(e) => patch({ include_bounding_box: e.target.checked })}
        />
        {t("buildIndex.annotation.includeBoundingBox")}
      </label>
    </div>
  );
}
