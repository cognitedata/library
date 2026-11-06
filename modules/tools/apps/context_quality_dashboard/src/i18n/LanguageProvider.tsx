import { useCallback, useMemo, useState, type ReactNode } from 'react';

import { LanguageContext } from './languageReactContext';
import { translate, type LanguageCode } from './messages';

const STORAGE_KEY = 'context-quality-dashboard.language';

function readStoredLanguage(): LanguageCode {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'ja') {
    return 'ja';
  }
  return 'en';
}

type LanguageProviderProps = {
  children: ReactNode;
};

export function LanguageProvider({ children }: LanguageProviderProps) {
  const [language, setLanguageState] = useState<LanguageCode>(() => readStoredLanguage());

  const setLanguage = useCallback((next: LanguageCode) => {
    setLanguageState(next);
    localStorage.setItem(STORAGE_KEY, next);
  }, []);

  const t = useCallback((key: string) => translate(key, language), [language]);

  const value = useMemo(
    () => ({
      language,
      setLanguage,
      t,
    }),
    [language, setLanguage, t]
  );

  return <LanguageContext.Provider value={value}>{children}</LanguageContext.Provider>;
}
