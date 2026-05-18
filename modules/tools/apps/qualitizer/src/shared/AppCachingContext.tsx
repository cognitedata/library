import { createContext, useCallback, useContext, useEffect, useState } from "react";
import {
  loadAppCachingEnabled,
  setAppCachingEnabledFlag,
} from "@/shared/app-caching-flag";

type AppCachingContextValue = {
  isCachingEnabled: boolean;
  setCachingEnabled: (enabled: boolean) => void;
};

const AppCachingContext = createContext<AppCachingContextValue | null>(null);

export function AppCachingProvider({ children }: { children: React.ReactNode }) {
  const [isCachingEnabled, setCachingEnabledState] = useState(loadAppCachingEnabled);

  const setCachingEnabled = useCallback((enabled: boolean) => {
    setCachingEnabledState(enabled);
    setAppCachingEnabledFlag(enabled);
  }, []);

  useEffect(() => {
    const w = window as unknown as Record<string, unknown>;
    w.enableAppCaching = () => {
      setCachingEnabled(true);
      // eslint-disable-next-line no-console
      console.log("%cCaching enabled", "color: #22c55e; font-weight: bold");
    };
    w.disableAppCaching = () => {
      setCachingEnabled(false);
      // eslint-disable-next-line no-console
      console.log("%cCaching disabled (LRU bypassed)", "color: #f97316; font-weight: bold");
    };
    w.toggleAppCaching = () => {
      setCachingEnabledState((prev) => {
        const next = !prev;
        setAppCachingEnabledFlag(next);
        // eslint-disable-next-line no-console
        console.log(
          next
            ? "%cCaching enabled"
            : "%cCaching disabled (LRU bypassed)",
          `color: ${next ? "#22c55e" : "#f97316"}; font-weight: bold`
        );
        return next;
      });
    };

    return () => {
      delete w.enableAppCaching;
      delete w.disableAppCaching;
      delete w.toggleAppCaching;
    };
  }, [setCachingEnabled]);

  return (
    <AppCachingContext.Provider value={{ isCachingEnabled, setCachingEnabled }}>
      {children}
    </AppCachingContext.Provider>
  );
}

export function useAppCaching() {
  const ctx = useContext(AppCachingContext);
  if (!ctx) {
    throw new Error("useAppCaching must be used within AppCachingProvider");
  }
  return ctx;
}
