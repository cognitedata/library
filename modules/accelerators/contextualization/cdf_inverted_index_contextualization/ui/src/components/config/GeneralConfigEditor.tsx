import { useAppSettings } from "../../context/AppSettingsContext";
import type { GeneralConfig } from "../../types/invertedIndexConfig";

type Props = {
  value: GeneralConfig;
  onChange: (next: GeneralConfig) => void;
};

export function GeneralConfigEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();

  return (
    <div className="idx-config-section">
      <h3 className="idx-config-section__title">{t("config.general.title")}</h3>
      <p className="idx-pane__hint">{t("config.general.hint")}</p>
      <div className="idx-config-grid">
        <label className="idx-label">
          {t("config.general.name")}
          <input
            className="idx-input"
            value={value.name}
            onChange={(e) => onChange({ ...value, name: e.target.value })}
          />
        </label>
        <label className="idx-label">
          {t("config.general.organization")}
          <input
            className="idx-input"
            value={value.organization}
            onChange={(e) => onChange({ ...value, organization: e.target.value })}
          />
        </label>
        <label className="idx-label">
          {t("config.general.storageBackend")}
          <select
            className="idx-select"
            value={value.indexStorageBackend}
            onChange={(e) =>
              onChange({
                ...value,
                indexStorageBackend: e.target.value === "dm" ? "dm" : "raw",
              })
            }
          >
            <option value="raw">{t("config.general.backendRaw")}</option>
            <option value="dm">{t("config.general.backendDm")}</option>
          </select>
        </label>
        <label className="idx-label">
          {t("config.general.rawDatabase")}
          <input
            className="idx-input idx-input--mono"
            value={value.indexRawDatabase}
            onChange={(e) => onChange({ ...value, indexRawDatabase: e.target.value })}
            disabled={value.indexStorageBackend !== "raw"}
          />
        </label>
      </div>
    </div>
  );
}
