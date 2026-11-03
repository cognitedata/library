/**
 * Single integration boundary for Flows auth.
 * Hooks and providers consume `useCdfClient()` instead of importing `useCogniteSdk` directly.
 */

import { useCogniteSdk } from "@cognite/app-sdk/react";
import type { CogniteClient } from "@cognite/sdk";
import { createContext, type ReactNode, useContext, useMemo } from "react";

export interface CdfClientContextValue {
  sdk: CogniteClient | null;
  isAuthLoading: boolean;
}

const CdfClientContext = createContext<CdfClientContextValue | null>(null);

export function CdfClientProvider({ children }: { children: ReactNode }) {
  const sdk = useCogniteSdk();

  const value = useMemo<CdfClientContextValue>(
    () => ({
      sdk: sdk ?? null,
      isAuthLoading: !sdk,
    }),
    [sdk],
  );

  return (
    <CdfClientContext.Provider value={value}>
      {children}
    </CdfClientContext.Provider>
  );
}

export function useCdfClient(): CdfClientContextValue {
  const ctx = useContext(CdfClientContext);
  if (!ctx) {
    throw new Error("useCdfClient must be used within CdfClientProvider");
  }
  return ctx;
}
