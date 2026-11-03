/**
 * Loads agents from CDF and syncs selected agent + display name.
 * Used by SettingsPage for agent configuration.
 */

import type { CogniteClient } from "@cognite/sdk";
import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { getAppConfig } from "@/config/appConfig";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { useOperations } from "@/contexts/OperationsSummarizerContext";
import { useCanEditAgentConfig } from "@/hooks/useCdfData";
import type { Agent } from "@/types/operations";

type UseAgentLoaderDeps = {
  useCdfClient: typeof useCdfClient;
};

const defaultAgentLoaderDeps: UseAgentLoaderDeps = {
  useCdfClient,
};

export const UseAgentLoaderContext = createContext<UseAgentLoaderDeps>(
  defaultAgentLoaderDeps,
);

export interface UseAgentLoaderResult {
  agentName: string | null;
  setAgentName: (name: string | null) => void;
  currentAgent: Agent | null;
  canEditAgentConfig: boolean;
  canEditAgentConfigLoading: boolean;
  canEditAgentConfigError: boolean;
  refetchCanEditAgentConfig: () => void;
  sdk: CogniteClient | null;
  state: ReturnType<typeof useOperations>["state"];
}

export function useAgentLoader(): UseAgentLoaderResult {
  const { useCdfClient: useClient } = useContext(UseAgentLoaderContext);
  const { sdk } = useClient();
  const { state, dispatch } = useOperations();
  const {
    data: canEditAgentConfig = false,
    isLoading: canEditAgentConfigLoading,
    isError: canEditAgentConfigError,
    refetch: refetchCanEditAgentConfig,
  } = useCanEditAgentConfig();

  const [agentName, setAgentName] = useState<string | null>(null);

  useEffect(() => {
    if (state.customInstructions.trim() !== "") return;
    const instructions = getAppConfig().prompts.agentInstructions.trim();
    if (instructions === "") return;
    dispatch({
      type: "SET_CUSTOM_INSTRUCTIONS",
      payload: instructions,
    });
  }, [state.customInstructions, dispatch]);

  // Sync local agent name when selected agent changes
  // Agent loading is now handled by OperationsProvider on app initialization
  useEffect(() => {
    if (state.selectedAgent && state.agents.length > 0) {
      const current = state.agents.find(
        (a) => a.externalId === state.selectedAgent,
      );
      if (current) {
        setAgentName(current.name);
      }
    } else if (!state.selectedAgent) {
      setAgentName(null);
    }
  }, [state.selectedAgent, state.agents]);

  const currentAgent = useMemo(() => {
    if (!state.selectedAgent || !state.agents) return null;
    return (
      state.agents.find((a) => a.externalId === state.selectedAgent) ?? null
    );
  }, [state.selectedAgent, state.agents]);

  return {
    agentName,
    setAgentName,
    currentAgent,
    canEditAgentConfig,
    canEditAgentConfigLoading,
    canEditAgentConfigError,
    refetchCanEditAgentConfig,
    sdk,
    state,
  };
}
