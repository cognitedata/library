import {
  createContext,
  useCallback,
  useContext,
  useLayoutEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { createTranslator, translations, type Locale, type MessageKey, type Theme } from "../i18n";

const THEME_KEY = "cdf-inverted-index-theme";
const LOCALE_KEY = "cdf-inverted-index-locale";
const TRANSLATION_OVERRIDES_KEY = "cdf-inverted-index-translation-overrides";

type TranslationOverrides = Partial<Record<Locale, Partial<Record<MessageKey, string>>>>;

function readStoredTheme(): Theme | null {
  try {
    const v = localStorage.getItem(THEME_KEY);
    if (v === "light" || v === "dark") return v;
  } catch {
    /* ignore */
  }
  return null;
}

function readStoredLocale(): Locale | null {
  try {
    const v = localStorage.getItem(LOCALE_KEY);
    if (
      v === "en" ||
      v === "ar" ||
      v === "es" ||
      v === "nb" ||
      v === "ja" ||
      v === "pt" ||
      v === "fr" ||
      v === "de" ||
      v === "zh" ||
      v === "hi" ||
      v === "bn"
    )
      return v;
  } catch {
    /* ignore */
  }
  return null;
}

function prefersDark(): boolean {
  return typeof window !== "undefined" && window.matchMedia?.("(prefers-color-scheme: dark)").matches;
}

function readTranslationOverrides(): TranslationOverrides {
  try {
    const raw = localStorage.getItem(TRANSLATION_OVERRIDES_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return {};
    return parsed as TranslationOverrides;
  } catch {
    return {};
  }
}

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type AppSettingsValue = {
  theme: Theme;
  setTheme: (t: Theme) => void;
  locale: Locale;
  setLocale: (l: Locale) => void;
  t: TFn;
  getTranslationValue: (targetLocale: Locale, key: MessageKey) => string;
  setTranslationValue: (targetLocale: Locale, key: MessageKey, value: string) => void;
};

const AppSettingsContext = createContext<AppSettingsValue | null>(null);

export function AppSettingsProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(
    () => readStoredTheme() ?? (prefersDark() ? "dark" : "light")
  );
  const [locale, setLocaleState] = useState<Locale>(() => readStoredLocale() ?? "en");
  const [translationOverrides, setTranslationOverrides] = useState<TranslationOverrides>(
    () => readTranslationOverrides()
  );

  const setTheme = useCallback((next: Theme) => {
    setThemeState(next);
    try {
      localStorage.setItem(THEME_KEY, next);
    } catch {
      /* ignore */
    }
  }, []);

  const setLocale = useCallback((next: Locale) => {
    setLocaleState(next);
    try {
      localStorage.setItem(LOCALE_KEY, next);
    } catch {
      /* ignore */
    }
  }, []);

  const setTranslationValue = useCallback((targetLocale: Locale, key: MessageKey, value: string) => {
    setTranslationOverrides((prev) => {
      const currentLocaleOverrides = { ...(prev[targetLocale] ?? {}) };
      if (value) {
        currentLocaleOverrides[key] = value;
      } else {
        delete currentLocaleOverrides[key];
      }
      const next: TranslationOverrides = { ...prev };
      if (Object.keys(currentLocaleOverrides).length) {
        next[targetLocale] = currentLocaleOverrides;
      } else {
        delete next[targetLocale];
      }
      try {
        localStorage.setItem(TRANSLATION_OVERRIDES_KEY, JSON.stringify(next));
      } catch {
        /* ignore */
      }
      return next;
    });
  }, []);

  const getTranslationValue = useCallback(
    (targetLocale: Locale, key: MessageKey): string => {
      const override = translationOverrides[targetLocale]?.[key];
      if (typeof override === "string") return override;
      return translations[targetLocale][key];
    },
    [translationOverrides]
  );

  useLayoutEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  useLayoutEffect(() => {
    document.documentElement.lang =
      locale === "nb" ? "nb" : locale === "zh" ? "zh-CN" : locale === "ar" ? "ar" : locale;
    document.documentElement.dir = locale === "ar" ? "rtl" : "ltr";
  }, [locale]);

  const t = useMemo(() => {
    const base = createTranslator(locale);
    return (key: MessageKey, vars?: Record<string, string | number>) => {
      const override = translationOverrides[locale]?.[key];
      if (typeof override !== "string") {
        return base(key, vars);
      }
      if (!vars) return override;
      return override.replace(/\{(\w+)\}/g, (_, k: string) =>
        Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : `{${k}}`
      );
    };
  }, [locale, translationOverrides]);

  const value = useMemo(
    () => ({
      theme,
      setTheme,
      locale,
      setLocale,
      t,
      getTranslationValue,
      setTranslationValue,
    }),
    [theme, setTheme, locale, setLocale, t, getTranslationValue, setTranslationValue]
  );

  return <AppSettingsContext.Provider value={value}>{children}</AppSettingsContext.Provider>;
}

export function useAppSettings(): AppSettingsValue {
  const ctx = useContext(AppSettingsContext);
  if (!ctx) throw new Error("useAppSettings must be used within AppSettingsProvider");
  return ctx;
}
