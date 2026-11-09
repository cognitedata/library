import { createContext } from 'react';

import type { LanguageContextValue } from './languageTypes';

export const LanguageContext = createContext<LanguageContextValue | null>(null);
