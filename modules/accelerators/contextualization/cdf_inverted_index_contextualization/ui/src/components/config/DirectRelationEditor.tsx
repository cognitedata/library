import { useAppSettings } from "../../context/AppSettingsContext";
import { DIRECT_RELATION_LINK_KEYS, type DirectRelationTopLevel } from "../../types/invertedIndexConfig";
import { StringListInput } from "./StringListInput";

type Props = {
  value: DirectRelationTopLevel;
  onChange: (next: DirectRelationTopLevel) => void;
};

const LINK_LABEL_KEYS = {
  file_to_asset: "config.linking.linkFileToAsset",
  equipment_to_asset: "config.linking.linkEquipmentToAsset",
  equipment_to_file: "config.linking.linkEquipmentToFile",
  timeseries_to_asset: "config.linking.linkTimeseriesToAsset",
  timeseries_to_equipment: "config.linking.linkTimeseriesToEquipment",
} as const;

export function DirectRelationEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();

  return (
    <div className="idx-config-section">
      <h3 className="idx-config-section__title">{t("config.linking.title")}</h3>
      <p className="idx-pane__hint">{t("config.linking.hint")}</p>

      <div className="idx-config-grid">
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.enabled}
            onChange={(e) => onChange({ ...value, enabled: e.target.checked })}
          />
          {t("config.linking.enabled")}
        </label>
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.writeOnSuggestedAnnotations}
            onChange={(e) =>
              onChange({ ...value, writeOnSuggestedAnnotations: e.target.checked })
            }
          />
          {t("config.linking.writeOnSuggested")}
        </label>
        <label className="idx-label">
          {t("config.linking.minConfidence")}
          <input
            className="idx-input"
            type="number"
            min={0}
            max={1}
            step={0.05}
            value={value.minConfidence}
            onChange={(e) => onChange({ ...value, minConfidence: Number(e.target.value) })}
          />
        </label>
        <label className="idx-label">
          {t("config.linking.maxListSize")}
          <input
            className="idx-input"
            type="number"
            min={1}
            step={1}
            value={value.maxListSize}
            onChange={(e) => onChange({ ...value, maxListSize: Number(e.target.value) })}
          />
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.linking.allowedStatuses")}
          <StringListInput
            value={value.allowedAnnotationStatuses}
            onChange={(allowedAnnotationStatuses) =>
              onChange({ ...value, allowedAnnotationStatuses })
            }
            placeholder={t("config.linking.allowedStatusesPlaceholder")}
          />
        </label>
        <label className="idx-label idx-config-grid__full">
          {t("config.linking.sourceTypes")}
          <StringListInput
            value={value.sourceTypes}
            onChange={(sourceTypes) => onChange({ ...value, sourceTypes })}
            placeholder={t("config.linking.sourceTypesPlaceholder")}
            mono
          />
          <span className="idx-config-hint">{t("config.linking.sourceTypesHint")}</span>
        </label>
      </div>

      <h4 className="idx-config-subsection__title">{t("config.linking.linkToggles")}</h4>
      <p className="idx-pane__hint">{t("config.linking.linkTogglesHint")}</p>
      <div className="idx-config-link-grid">
        {DIRECT_RELATION_LINK_KEYS.map((key) => (
          <label key={key} className="idx-checkbox-label">
            <input
              type="checkbox"
              checked={value.linkEnabled[key] ?? true}
              onChange={(e) =>
                onChange({
                  ...value,
                  linkEnabled: { ...value.linkEnabled, [key]: e.target.checked },
                })
              }
            />
            {t(LINK_LABEL_KEYS[key])}
          </label>
        ))}
      </div>
    </div>
  );
}
