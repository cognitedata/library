import { createContext, useCallback, useContext, useState } from "react";

const DEFAULT_EDGE_LIMIT = 10000;
const DEFAULT_ASSET_LIMIT = 10000;

const STORAGE_KEY_EDGE = "qualitizer.edgeLimit";
const STORAGE_KEY_ASSET = "qualitizer.assetLimit";

function loadEdgeLimit(): number {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY_EDGE);
    if (stored != null) {
      const n = Number(stored);
      if (Number.isFinite(n) && n > 0) return n;
    }
  } catch {
    // ignore
  }
  return DEFAULT_EDGE_LIMIT;
}

function loadAssetLimit(): number {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY_ASSET);
    if (stored != null) {
      const n = Number(stored);
      if (Number.isFinite(n) && n > 0) return n;
    }
  } catch {
    // ignore
  }
  return DEFAULT_ASSET_LIMIT;
}

type LimitsContextValue = {
  edgeLimit: number;
  setEdgeLimit: (n: number) => void;
  assetLimit: number;
  setAssetLimit: (n: number) => void;
};

const LimitsContext = createContext<LimitsContextValue | null>(null);

export function LimitsProvider({ children }: { children: React.ReactNode }) {
  const [edgeLimit, setEdgeLimitState] = useState(loadEdgeLimit);
  const [assetLimit, setAssetLimitState] = useState(loadAssetLimit);

  const setEdgeLimit = useCallback((n: number) => {
    const sanitized = Number.isFinite(n) && n > 0 ? n : DEFAULT_EDGE_LIMIT;
    setEdgeLimitState(sanitized);
    try {
      window.localStorage.setItem(STORAGE_KEY_EDGE, String(sanitized));
    } catch {
      // ignore
    }
  }, []);

  const setAssetLimit = useCallback((n: number) => {
    const sanitized = Number.isFinite(n) && n > 0 ? n : DEFAULT_ASSET_LIMIT;
    setAssetLimitState(sanitized);
    try {
      window.localStorage.setItem(STORAGE_KEY_ASSET, String(sanitized));
    } catch {
      // ignore
    }
  }, []);

  const value: LimitsContextValue = {
    edgeLimit,
    setEdgeLimit,
    assetLimit,
    setAssetLimit,
  };
  return <LimitsContext.Provider value={value}>{children}</LimitsContext.Provider>;
}

export function useLimits() {
  const ctx = useContext(LimitsContext);
  if (!ctx) {
    throw new Error("useLimits must be used within LimitsProvider");
  }
  return ctx;
}
