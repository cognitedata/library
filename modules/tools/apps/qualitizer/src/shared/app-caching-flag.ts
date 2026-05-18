const STORAGE_KEY = "qualitizer.cachingEnabled";

export function loadAppCachingEnabled(): boolean {
  if (typeof window === "undefined") return true;
  try {
    const v = window.localStorage.getItem(STORAGE_KEY);
    if (v === null) return true;
    return v === "true";
  } catch {
    return true;
  }
}

let appCachingEnabled = loadAppCachingEnabled();

/** Synchronous read for cache modules outside React. Stays in sync via AppCachingProvider. */
export function isAppCachingEnabled(): boolean {
  return appCachingEnabled;
}

export function setAppCachingEnabledFlag(enabled: boolean): void {
  appCachingEnabled = enabled;
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(STORAGE_KEY, String(enabled));
  } catch {
    // ignore
  }
}
