/**
 * Shared SDK context and hook. No provider imports here so we can load
 * localhost vs Fusion auth in separate modules.
 */
import type { CogniteClient } from "@cognite/sdk";
import { createContext, useContext } from "react";

export const LegacySDKContext = createContext<CogniteClient | null>(null);

export function useLegacySDK(): { sdk: CogniteClient | null } {
  const sdk = useContext(LegacySDKContext);
  return { sdk };
}
