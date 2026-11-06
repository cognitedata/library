import { useEffect, useState } from 'react';

export type ThemeMode = 'light' | 'dark';

const STORAGE_KEY = 'context-quality-dashboard.theme';

function readStoredTheme(): ThemeMode {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'dark' || stored === 'light') {
    return stored;
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export function applyThemeMode(mode: ThemeMode): void {
  document.documentElement.classList.toggle('dark', mode === 'dark');
}

export function useThemeMode() {
  const [mode, setMode] = useState<ThemeMode>(() => readStoredTheme());

  useEffect(() => {
    applyThemeMode(mode);
    localStorage.setItem(STORAGE_KEY, mode);
  }, [mode]);

  return {
    mode,
    isDark: mode === 'dark',
    setTheme: setMode,
  };
}
