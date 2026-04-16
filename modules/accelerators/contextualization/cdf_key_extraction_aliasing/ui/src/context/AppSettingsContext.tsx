import {
  createContext,
  useCallback,
  useContext,
  useLayoutEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { createTranslator, type Locale, type MessageKey, type Theme } from "../i18n";

const THEME_KEY = "cdf-kea-extraction-theme";
const LOCALE_KEY = "cdf-kea-extraction-locale";

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
    if (v === "en" || v === "es" || v === "nb" || v === "ja") return v;
  } catch {
    /* ignore */
  }
  return null;
}

function prefersDark(): boolean {
  return typeof window !== "undefined" && window.matchMedia?.("(prefers-color-scheme: dark)").matches;
}

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type AppSettingsValue = {
  theme: Theme;
  setTheme: (t: Theme) => void;
  locale: Locale;
  setLocale: (l: Locale) => void;
  t: TFn;
};

const AppSettingsContext = createContext<AppSettingsValue | null>(null);

export function AppSettingsProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(
    () => readStoredTheme() ?? (prefersDark() ? "dark" : "light")
  );
  const [locale, setLocaleState] = useState<Locale>(() => readStoredLocale() ?? "en");

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

  useLayoutEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  useLayoutEffect(() => {
    document.documentElement.lang = locale === "nb" ? "nb" : locale;
  }, [locale]);

  const t = useMemo(() => createTranslator(locale), [locale]);

  const value = useMemo(
    () => ({
      theme,
      setTheme,
      locale,
      setLocale,
      t,
    }),
    [theme, setTheme, locale, setLocale, t]
  );

  return <AppSettingsContext.Provider value={value}>{children}</AppSettingsContext.Provider>;
}

export function useAppSettings(): AppSettingsValue {
  const ctx = useContext(AppSettingsContext);
  if (!ctx) throw new Error("useAppSettings must be used within AppSettingsProvider");
  return ctx;
}
