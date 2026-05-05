import { createContext, useCallback, useContext, useEffect, useState } from "react";

const STORAGE_KEY = "qualitizer.privateMode";

function loadPrivateMode(): boolean {
  try {
    return window.localStorage.getItem(STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

type PrivateModeContextValue = {
  isPrivateMode: boolean;
  setPrivateMode: (enabled: boolean) => void;
};

const PrivateModeContext = createContext<PrivateModeContextValue | null>(null);

export function PrivateModeProvider({ children }: { children: React.ReactNode }) {
  const [isPrivateMode, setPrivateModeState] = useState(loadPrivateMode);

  const setPrivateMode = useCallback((enabled: boolean) => {
    setPrivateModeState(enabled);
    try {
      window.localStorage.setItem(STORAGE_KEY, String(enabled));
    } catch {
      // ignore
    }
  }, []);

  // Expose console commands for quick toggling
  useEffect(() => {
    const w = window as unknown as Record<string, unknown>;
    w.enablePrivateMode = () => {
      setPrivateMode(true);
      // eslint-disable-next-line no-console
      console.log("%c🔒 Private mode enabled", "color: #f97316; font-weight: bold");
    };
    w.disablePrivateMode = () => {
      setPrivateMode(false);
      // eslint-disable-next-line no-console
      console.log("%c🔓 Private mode disabled", "color: #22c55e; font-weight: bold");
    };
    w.togglePrivateMode = () => {
      setPrivateModeState((prev) => {
        const next = !prev;
        try {
          window.localStorage.setItem(STORAGE_KEY, String(next));
        } catch {
          // ignore
        }
        // eslint-disable-next-line no-console
        console.log(
          next
            ? "%c🔒 Private mode enabled"
            : "%c🔓 Private mode disabled",
          `color: ${next ? "#f97316" : "#22c55e"}; font-weight: bold`
        );
        return next;
      });
    };

    return () => {
      delete w.enablePrivateMode;
      delete w.disablePrivateMode;
      delete w.togglePrivateMode;
    };
  }, [setPrivateMode]);

  return (
    <PrivateModeContext.Provider value={{ isPrivateMode, setPrivateMode }}>
      {children}
    </PrivateModeContext.Provider>
  );
}

export function usePrivateMode() {
  const ctx = useContext(PrivateModeContext);
  if (!ctx) {
    throw new Error("usePrivateMode must be used within PrivateModeProvider");
  }
  return ctx;
}

/**
 * Masks a string for display in private mode.
 * Preserves the first character and length, replaces the rest with bullets.
 * Returns the original string if private mode is off.
 */
export function mask(value: string | null | undefined, isPrivate: boolean): string {
  if (!value) return value ?? "";
  if (!isPrivate) return value;
  if (value.length <= 1) return "•";
  return value[0] + "•".repeat(Math.min(value.length - 1, 12));
}
