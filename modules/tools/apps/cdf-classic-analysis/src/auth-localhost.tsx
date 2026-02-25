/**
 * Auth for localhost: CDF with .env. Loaded only when hostname === "localhost".
 */
import React from "react";
import { CDFAuthenticationProvider, useCDF } from "@cognite/dune-fe-auth";
import { LegacySDKContext } from "./auth-context";

function Bridge({ children }: { children: React.ReactNode }) {
  const { sdk } = useCDF();
  return (
    <LegacySDKContext.Provider value={sdk}>
      {children}
    </LegacySDKContext.Provider>
  );
}

export function LocalhostAuth({ children }: { children: React.ReactNode }) {
  return (
    <CDFAuthenticationProvider>
      <Bridge>{children}</Bridge>
    </CDFAuthenticationProvider>
  );
}
