import { DuneAuthProvider, useDune } from "@cognite/dune";

import { StandaloneAuthProvider } from "../standalone-auth";
import { AppSdkContext, useAppSdk } from "../sdk-context";

export { useAppSdk };

function DuneBridge({ children }: { children: React.ReactNode }) {
  const { sdk, isLoading } = useDune();
  return (
    <AppSdkContext.Provider value={{ sdk, isLoading }}>{children}</AppSdkContext.Provider>
  );
}

export function AppAuthProvider({ children }: { children: React.ReactNode }) {
  const isStandalone = import.meta.env.VITE_STANDALONE === "true";

  if (isStandalone) {
    return <StandaloneAuthProvider>{children}</StandaloneAuthProvider>;
  }

  return (
    <DuneAuthProvider>
      <DuneBridge>{children}</DuneBridge>
    </DuneAuthProvider>
  );
}
