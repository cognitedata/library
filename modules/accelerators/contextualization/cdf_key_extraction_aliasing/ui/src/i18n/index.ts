import type { Locale } from "./types";
import type { MessageKey, Messages } from "./types";
import { en } from "./en";
import { es } from "./es";
import { de } from "./de";
import { fr } from "./fr";
import { hi } from "./hi";
import { ja } from "./ja";
import { nb } from "./nb";
import { pt } from "./pt";
import { zh } from "./zh";

export type { Locale, MessageKey, Messages, Theme } from "./types";
export { de, en, es, fr, hi, ja, nb, pt, zh };

export const LOCALES: { code: Locale; label: string }[] = [
  { code: "en", label: "English" },
  { code: "de", label: "Deutsch" },
  { code: "es", label: "Español" },
  { code: "fr", label: "Français" },
  { code: "nb", label: "Norsk" },
  { code: "pt", label: "Português" },
  { code: "hi", label: "हिन्दी" },
  { code: "ja", label: "日本語" },
  { code: "zh", label: "中文" },
];

export const translations: Record<Locale, Messages> = {
  en,
  es,
  pt,
  fr,
  de,
  nb,
  ja,
  zh,
  hi,
};

export function interpolate(template: string, vars: Record<string, string | number>): string {
  return template.replace(/\{(\w+)\}/g, (_, k: string) =>
    Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : `{${k}}`
  );
}

export function createTranslator(locale: Locale) {
  const table = translations[locale];
  return function t(key: MessageKey, vars?: Record<string, string | number>): string {
    let s = table[key];
    if (vars) s = interpolate(s, vars);
    return s;
  };
}
