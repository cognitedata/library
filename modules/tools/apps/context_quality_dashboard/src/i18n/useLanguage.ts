import { useContext } from 'react';

import { LanguageContext } from './languageReactContext';
import type { LanguageContextValue } from './languageTypes';

export function useLanguage(): LanguageContextValue {
  const context = useContext(LanguageContext);
  if (context === null) {
    throw new Error('useLanguage must be used within LanguageProvider');
  }
  return context;
}
