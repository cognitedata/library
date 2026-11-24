import en from './en.json';
import ja from './ja.json';

export type LanguageCode = 'en' | 'ja';

const MESSAGES: Record<LanguageCode, Record<string, string>> = {
  en,
  ja,
};

export function translate(key: string, language: LanguageCode): string {
  const localized = MESSAGES[language][key];
  if (localized !== undefined) {
    return localized;
  }
  const fallback = MESSAGES.en[key];
  if (fallback !== undefined) {
    return fallback;
  }
  return key;
}
