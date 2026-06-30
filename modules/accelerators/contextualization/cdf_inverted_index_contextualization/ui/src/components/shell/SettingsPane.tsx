import { useMemo, useState } from "react";
import { LOCALES, en, type Locale, type MessageKey } from "../../i18n";
import { useAppSettings } from "../../context/AppSettingsContext";
import { SectionIntro } from "../shared/SectionIntro";

const MESSAGE_KEYS = Object.keys(en) as MessageKey[];

export function SettingsPane() {
  const { t, locale, getTranslationValue, setTranslationValue } = useAppSettings();
  const [targetLocale, setTargetLocale] = useState<Locale>(locale);
  const [filter, setFilter] = useState("");

  const normalizedFilter = filter.trim().toLowerCase();
  const rows = useMemo(() => {
    if (!normalizedFilter) return MESSAGE_KEYS;
    return MESSAGE_KEYS.filter((key) => {
      const baseValue = en[key];
      const currentValue = getTranslationValue(targetLocale, key);
      return (
        key.toLowerCase().includes(normalizedFilter) ||
        baseValue.toLowerCase().includes(normalizedFilter) ||
        currentValue.toLowerCase().includes(normalizedFilter)
      );
    });
  }, [normalizedFilter, targetLocale, getTranslationValue]);

  return (
    <section className="idx-pane idx-editor-page idx-settings-pane" aria-label={t("settings.title")}>
      <header className="idx-pane-header">
        <h2 className="idx-pane__title">{t("settings.translationEditor.title")}</h2>
        <SectionIntro>{t("settings.translationEditor.hint")}</SectionIntro>
      </header>
      <div className="idx-settings-pane__controls">
        <label className="idx-settings-pane__field">
          <span>{t("settings.translationEditor.locale")}</span>
          <select
            value={targetLocale}
            onChange={(e) => setTargetLocale(e.target.value as Locale)}
            aria-label={t("settings.translationEditor.locale")}
          >
            {LOCALES.map((entry) => (
              <option key={entry.code} value={entry.code}>
                {entry.label}
              </option>
            ))}
          </select>
        </label>
        <label className="idx-settings-pane__field">
          <span>{t("settings.translationEditor.filter")}</span>
          <input
            className="idx-input"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder={t("settings.translationEditor.filterPlaceholder")}
            aria-label={t("settings.translationEditor.filter")}
          />
        </label>
      </div>
      <div className="idx-table-wrap">
        <table className="idx-table">
          <thead>
            <tr>
              <th>{t("settings.translationEditor.tableKey")}</th>
              <th>{t("settings.translationEditor.tableEnglish")}</th>
              <th>{t("settings.translationEditor.tableTranslation")}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((key) => (
              <tr key={key}>
                <td className="idx-settings-table__key">{key}</td>
                <td>{en[key]}</td>
                <td>
                  <input
                    className="idx-input idx-settings-table__input"
                    value={getTranslationValue(targetLocale, key)}
                    onChange={(e) => setTranslationValue(targetLocale, key, e.target.value)}
                    aria-label={t("settings.translationEditor.translationInput", { key })}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
