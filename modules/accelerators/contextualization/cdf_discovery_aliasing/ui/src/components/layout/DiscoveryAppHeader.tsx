import { CogniteLogo } from "../CogniteLogo";
import { useAppSettings } from "../../context/AppSettingsContext";
import { LOCALES } from "../../i18n";

/** Discovery module header (brand, theme, locale). */
export function DiscoveryAppHeader() {
  const { t, theme, setTheme, locale, setLocale } = useAppSettings();
  return (
    <header className="discovery-header">
      <div className="discovery-header__shell">
        <div className="discovery-header__brand">
          <CogniteLogo />
          <div className="discovery-header__brand-text">
            <h1 className="discovery-header__title">{t("app.title")}</h1>
            <p className="discovery-header__subtitle">{t("app.subtitle")}</p>
          </div>
        </div>
        <div className="discovery-header__toolbar">
          <div className="discovery-header__toolbar-group">
            <label className="discovery-header__control" title={t("controls.theme.tooltip")}>
              <span className="discovery-header__control-label">{t("controls.theme")}</span>
              <span className="discovery-theme-toggle" role="group">
                <button type="button" data-active={theme === "light"} onClick={() => setTheme("light")}>
                  {t("controls.themeLight")}
                </button>
                <button type="button" data-active={theme === "dark"} onClick={() => setTheme("dark")}>
                  {t("controls.themeDark")}
                </button>
              </span>
            </label>
            <label className="discovery-header__control" title={t("controls.language.tooltip")}>
              <span className="discovery-header__control-label">{t("controls.language")}</span>
              <select value={locale} onChange={(e) => setLocale(e.target.value as typeof locale)}>
                {LOCALES.map(({ code, label }) => (
                  <option key={code} value={code}>
                    {label}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>
      </div>
    </header>
  );
}
