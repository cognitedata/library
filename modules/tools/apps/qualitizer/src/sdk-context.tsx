import { createContext, useContext } from "react";
import type { CogniteClient } from "@cognite/sdk";

type AppSdkContextValue = {
  sdk: CogniteClient;
  isLoading: boolean;
};

export const AppSdkContext = createContext<AppSdkContextValue | null>(null);

export function useAppSdk() {
  const context = useContext(AppSdkContext);
  if (!context) {
    throw new Error("useAppSdk must be used within AppAuthProvider");
  }
  return context;
}
