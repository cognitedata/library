import { createContext, useCallback, useContext, useState } from "react";

const DEFAULT_EDGE_LIMIT = 10000;
const DEFAULT_ASSET_LIMIT = 10000;
const DEFAULT_SUNBURST_MAX_DEPTH = 6;
const DEFAULT_MATRIX_SIZE = 10;

const STORAGE_KEY_EDGE = "qualitizer.edgeLimit";
const STORAGE_KEY_ASSET = "qualitizer.assetLimit";
const STORAGE_KEY_SUNBURST_DEPTH = "qualitizer.sunburstMaxDepth";
const STORAGE_KEY_MATRIX_SIZE = "qualitizer.matrixSize";

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

function loadMatrixSize(): number {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY_MATRIX_SIZE);
    if (stored != null) {
      const n = Number(stored);
      if (Number.isInteger(n) && n >= 3 && n <= 25) return n;
    }
  } catch {
    // ignore
  }
  return DEFAULT_MATRIX_SIZE;
}

function loadSunburstMaxDepth(): number {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY_SUNBURST_DEPTH);
    if (stored != null) {
      const n = Number(stored);
      if (Number.isInteger(n) && n >= 1 && n <= 20) return n;
    }
  } catch {
    // ignore
  }
  return DEFAULT_SUNBURST_MAX_DEPTH;
}

type LimitsContextValue = {
  edgeLimit: number;
  setEdgeLimit: (n: number) => void;
  assetLimit: number;
  setAssetLimit: (n: number) => void;
  sunburstMaxDepth: number;
  setSunburstMaxDepth: (n: number) => void;
  matrixSize: number;
  setMatrixSize: (n: number) => void;
};

const LimitsContext = createContext<LimitsContextValue | null>(null);

export function LimitsProvider({ children }: { children: React.ReactNode }) {
  const [edgeLimit, setEdgeLimitState] = useState(loadEdgeLimit);
  const [assetLimit, setAssetLimitState] = useState(loadAssetLimit);
  const [sunburstMaxDepth, setSunburstMaxDepthState] = useState(loadSunburstMaxDepth);
  const [matrixSize, setMatrixSizeState] = useState(loadMatrixSize);

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

  const setSunburstMaxDepth = useCallback((n: number) => {
    const sanitized = Number.isInteger(n) && n >= 1 && n <= 20 ? n : DEFAULT_SUNBURST_MAX_DEPTH;
    setSunburstMaxDepthState(sanitized);
    try {
      window.localStorage.setItem(STORAGE_KEY_SUNBURST_DEPTH, String(sanitized));
    } catch {
      // ignore
    }
  }, []);

  const setMatrixSize = useCallback((n: number) => {
    const sanitized = Number.isInteger(n) && n >= 3 && n <= 25 ? n : DEFAULT_MATRIX_SIZE;
    setMatrixSizeState(sanitized);
    try {
      window.localStorage.setItem(STORAGE_KEY_MATRIX_SIZE, String(sanitized));
    } catch {
      // ignore
    }
  }, []);

  const value: LimitsContextValue = {
    edgeLimit,
    setEdgeLimit,
    assetLimit,
    setAssetLimit,
    sunburstMaxDepth,
    setSunburstMaxDepth,
    matrixSize,
    setMatrixSize,
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
