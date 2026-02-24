import { createContext, useContext, useState } from "react";

const DEFAULT_EDGE_LIMIT = 10000;
const DEFAULT_ASSET_LIMIT = 10000;

type LimitsContextValue = {
  edgeLimit: number;
  setEdgeLimit: (n: number) => void;
  assetLimit: number;
  setAssetLimit: (n: number) => void;
};

const LimitsContext = createContext<LimitsContextValue | null>(null);

export function LimitsProvider({ children }: { children: React.ReactNode }) {
  const [edgeLimit, setEdgeLimit] = useState(DEFAULT_EDGE_LIMIT);
  const [assetLimit, setAssetLimit] = useState(DEFAULT_ASSET_LIMIT);
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
