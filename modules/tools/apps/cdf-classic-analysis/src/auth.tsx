/**
 * Picks localhost vs Fusion auth so only one provider is ever loaded.
 * Use useLegacySDK() from auth-context in the app.
 */
import React, { lazy, Suspense } from "react";

const LocalhostAuth = lazy(() =>
  import("./auth-localhost").then((m) => ({ default: m.LocalhostAuth }))
);
const FusionAuth = lazy(() =>
  import("./auth-fusion").then((m) => ({ default: m.FusionAuth }))
);

const loading = (
  <div style={{ padding: "1.5rem", color: "#94a3b8" }}>Loading auth…</div>
);

export function AuthBridge({ children }: { children: React.ReactNode }) {
  const inFusion =
    typeof window !== "undefined" && window.self !== window.top;
  const isLocalhost =
    typeof window !== "undefined" && window.location.hostname === "localhost";
  // Use Fusion (Dune) auth when embedded in iframe (e.g. Fusion page); else CDF when on localhost
  const Auth = inFusion ? FusionAuth : isLocalhost ? LocalhostAuth : FusionAuth;

  return (
    <Suspense fallback={loading}>
      <Auth>{children}</Auth>
    </Suspense>
  );
}

export { useLegacySDK } from "./auth-context";
