import type { Locale } from "./types";
import type { MessageKey, Messages } from "./types";
import { en } from "./en";
import { es } from "./es";
import { ja } from "./ja";
import { nb } from "./nb";
import { pt } from "./pt";

export type { Locale, MessageKey, Messages, Theme } from "./types";
export { en, es, ja, nb, pt };

export const LOCALES: { code: Locale; label: string }[] = [
  { code: "en", label: "English" },
  { code: "es", label: "Español" },
  { code: "pt", label: "Português" },
  { code: "nb", label: "Norsk" },
  { code: "ja", label: "日本語" },
];

export const translations: Record<Locale, Messages> = {
  en,
  es,
  pt,
  nb,
  ja,
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
