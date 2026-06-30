import { useAppSettings } from "../../context/AppSettingsContext";
import type { GeneralConfig } from "../../types/invertedIndexConfig";
import { FormPanel } from "../shared/FormPanel";

type Props = {
  value: GeneralConfig;
  onChange: (next: GeneralConfig) => void;
};

export function GeneralConfigEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const isRaw = value.indexStorageBackend === "raw";

  return (
    <>
      <FormPanel title={t("config.general.title")} hint={t("config.general.hint")}>
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
              disabled={!isRaw}
            />
            <span className="idx-config-hint">{t("config.general.rawDatabaseHint")}</span>
          </label>
        </div>
      </FormPanel>

      {isRaw ? (
        <FormPanel
          variant="compact"
          title={t("config.termPartition.title")}
          hint={t("config.termPartition.hint")}
        >
          <div className="idx-config-grid">
            <label className="idx-checkbox-label">
              <input
                type="checkbox"
                checked={value.termPartition.enabled}
                onChange={(e) =>
                  onChange({
                    ...value,
                    termPartition: { ...value.termPartition, enabled: e.target.checked },
                  })
                }
              />
              {t("config.termPartition.enabled")}
            </label>
            <label className="idx-label">
              {t("config.termPartition.activateAboveRows")}
              <input
                className="idx-input"
                type="number"
                min={1}
                step={1000}
                value={value.termPartition.activateAboveRows}
                disabled={!value.termPartition.enabled}
                onChange={(e) => {
                  const n = Number(e.target.value);
                  onChange({
                    ...value,
                    termPartition: {
                      ...value.termPartition,
                      activateAboveRows: Number.isFinite(n) && n > 0 ? n : 400_000,
                    },
                  });
                }}
              />
              <span className="idx-config-hint">{t("config.termPartition.activateAboveRowsHint")}</span>
            </label>
            <label className="idx-label">
              {t("config.termPartition.bucketMode")}
              <select
                className="idx-select"
                value={value.termPartition.bucketMode}
                disabled={!value.termPartition.enabled}
                onChange={(e) =>
                  onChange({
                    ...value,
                    termPartition: {
                      ...value.termPartition,
                      bucketMode:
                        e.target.value === "ascii_first_char"
                          ? "ascii_first_char"
                          : "script_aware",
                    },
                  })
                }
              >
                <option value="script_aware">{t("config.termPartition.bucketScriptAware")}</option>
                <option value="ascii_first_char">{t("config.termPartition.bucketAscii")}</option>
              </select>
              <span className="idx-config-hint">{t("config.termPartition.bucketModeHint")}</span>
            </label>
          </div>
        </FormPanel>
      ) : null}
    </>
  );
}
