import { useAppSettings } from "../../context/AppSettingsContext";
import type { VirtualTagCreationConfig } from "../../types/invertedIndexConfig";
import { FormPanel } from "../shared/FormPanel";

type Props = {
  value: VirtualTagCreationConfig;
  onChange: (next: VirtualTagCreationConfig) => void;
};

export function VirtualTagCreationConfigEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();

  return (
    <FormPanel title={t("config.virtualTags.title")} hint={t("config.virtualTags.hint")}>
      <div className="idx-config-grid">
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.enabled}
            onChange={(e) => onChange({ ...value, enabled: e.target.checked })}
          />
          {t("config.virtualTags.enabled")}
        </label>
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.incrementalEnabled}
            onChange={(e) =>
              onChange({ ...value, incrementalEnabled: e.target.checked })
            }
          />
          {t("config.virtualTags.incrementalEnabled")}
        </label>
        <label className="idx-label">
          {t("config.virtualTags.termSelectionMode")}
          <select
            className="idx-input"
            value={value.termSelectionMode}
            onChange={(e) =>
              onChange({
                ...value,
                termSelectionMode: e.target.value as VirtualTagCreationConfig["termSelectionMode"],
              })
            }
          >
            <option value="all">{t("config.virtualTags.termSelectionAll")}</option>
            <option value="missing_tags_only">
              {t("config.virtualTags.termSelectionMissing")}
            </option>
          </select>
        </label>
        <label className="idx-label">
          {t("config.virtualTags.instanceSpace")}
          <input
            className="idx-input idx-input--mono"
            value={value.instanceSpace}
            onChange={(e) => onChange({ ...value, instanceSpace: e.target.value })}
          />
        </label>
        <label className="idx-checkbox-label idx-config-grid__full">
          <input
            type="checkbox"
            checked={value.missingTagCriteria.requirePatternDetection}
            onChange={(e) =>
              onChange({
                ...value,
                missingTagCriteria: {
                  ...value.missingTagCriteria,
                  requirePatternDetection: e.target.checked,
                },
              })
            }
          />
          {t("config.virtualTags.requirePatternDetection")}
        </label>
        <label className="idx-checkbox-label idx-config-grid__full">
          <input
            type="checkbox"
            checked={value.missingTagCriteria.checkExistingCogniteAsset}
            onChange={(e) =>
              onChange({
                ...value,
                missingTagCriteria: {
                  ...value.missingTagCriteria,
                  checkExistingCogniteAsset: e.target.checked,
                },
              })
            }
          />
          {t("config.virtualTags.checkExistingAsset")}
        </label>
        <label className="idx-checkbox-label idx-config-grid__full">
          <input
            type="checkbox"
            checked={value.missingTagCriteria.excludeWithCogniteAssetMetadata}
            onChange={(e) =>
              onChange({
                ...value,
                missingTagCriteria: {
                  ...value.missingTagCriteria,
                  excludeWithCogniteAssetMetadata: e.target.checked,
                },
              })
            }
          />
          {t("config.virtualTags.excludeCogniteAssetMetadata")}
        </label>
      </div>
    </FormPanel>
  );
}
