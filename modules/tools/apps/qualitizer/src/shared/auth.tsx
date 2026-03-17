import { DuneAuthProvider, useDune } from "@cognite/dune";

import { SdkManagerProvider } from "../shared/SdkManager";
import { StandaloneAuthProvider } from "../standalone-auth";
import { AppSdkContext, useAppSdk } from "../sdk-context";

export { useAppSdk };

function DuneBridge({ children }: { children: React.ReactNode }) {
  const { sdk, isLoading } = useDune();
  if (!sdk) {
    return <div className="min-h-screen w-full flex items-center justify-center text-slate-500">Loading...</div>;
  }
  return (
    <AppSdkContext.Provider value={{ sdk, isLoading }}>
      <SdkManagerProvider baseSdk={sdk} isLoading={isLoading}>
        {children}
      </SdkManagerProvider>
    </AppSdkContext.Provider>
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
