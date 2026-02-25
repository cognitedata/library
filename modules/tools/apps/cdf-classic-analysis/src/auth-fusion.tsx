/**
 * Auth for Fusion (Dune iframe). Loaded only when not on localhost.
 */
import React from "react";
import { DuneAuthProvider, useDune } from "@cognite/dune";
import { LegacySDKContext } from "./auth-context";

function Bridge({ children }: { children: React.ReactNode }) {
  const { sdk } = useDune();
  return (
    <LegacySDKContext.Provider value={sdk}>
      {children}
    </LegacySDKContext.Provider>
  );
}

const loading = (
  <div style={{ padding: "1.5rem", color: "#94a3b8" }}>
    Loading CDF authentication…
  </div>
);

export function FusionAuth({ children }: { children: React.ReactNode }) {
  return (
    <DuneAuthProvider
      loadingComponent={loading}
      errorComponent={(error: string) => (
        <div style={{ padding: "1.5rem", color: "#f87171" }}>
          Authentication error: {error}
        </div>
      )}
    >
      <Bridge>{children}</Bridge>
    </DuneAuthProvider>
  );
}
