/**
 * CDF Agent Hooks.
 * Lists agents and creates the default agent from the config node when it is missing.
 */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createContext, useContext } from "react";
import { getAppConfig } from "@/config/appConfig";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { CDFHelper } from "@/services/cdfHelper";
import type { AppConfig } from "@/types/appConfig";
import { CDF_QUERY_KEYS } from "./cdfQueryKeys";

const DEFAULT_AGENT_DESCRIPTION = "AI agent for summarizing operations data.";

type UseAgentsCdfDeps = {
  useCdfClient: typeof useCdfClient;
};

const defaultAgentsCdfDeps: UseAgentsCdfDeps = {
  useCdfClient,
};

export const UseAgentsCdfContext =
  createContext<UseAgentsCdfDeps>(defaultAgentsCdfDeps);

type EnsureAgentDeps = {
  readConfig: () => AppConfig;
};

const defaultEnsureDeps: EnsureAgentDeps = {
  readConfig: () => getAppConfig(),
};

export const UseEnsureDefaultAgentContext =
  createContext<EnsureAgentDeps>(defaultEnsureDeps);

/**
 * Hook to fetch all available agents
 */
export function useAgents() {
  const { useCdfClient: useClient } = useContext(UseAgentsCdfContext);
  const { sdk, isAuthLoading } = useClient();

  return useQuery({
    queryKey: CDF_QUERY_KEYS.agents,
    queryFn: async () => {
      if (!sdk) throw new Error("SDK not available");
      const helper = new CDFHelper(sdk);
      const { agents } = await helper.getAgents();
      return agents;
    },
    enabled: !!sdk && !isAuthLoading,
    staleTime: 5 * 60 * 1000,
  });
}

/**
 * Hook to determine if the current user can create the default agent.
 * Uses GET /api/v1/token/inspect and checks for agentsAcl.actions including "WRITE".
 */
export function useCanEditAgentConfig() {
  const { useCdfClient: useClient } = useContext(UseAgentsCdfContext);
  const { sdk, isAuthLoading } = useClient();

  return useQuery({
    queryKey: CDF_QUERY_KEYS.canEditAgentConfig,
    queryFn: async () => {
      if (!sdk) return false;
      const helper = new CDFHelper(sdk);
      return helper.canEditAgentConfig();
    },
    enabled: !!sdk && !isAuthLoading,
    staleTime: 5 * 60 * 1000,
  });
}

/**
 * Creates the default agent when it is missing. Never updates an existing agent.
 * Name, external id, model, and instructions come from the config node.
 */
export function useEnsureDefaultAgent() {
  const { useCdfClient: useClient } = useContext(UseAgentsCdfContext);
  const { sdk } = useClient();
  const { readConfig } = useContext(UseEnsureDefaultAgentContext);
  const queryClient = useQueryClient();
  useAgents();

  return useMutation({
    mutationFn: async () => {
      if (!sdk) throw new Error("SDK not available");

      const config = readConfig();
      const name = config.agent.name;
      const instructions = config.prompts.agentInstructions.trim();
      if (instructions === "") {
        throw new Error(
          "Config node agent instructions are empty. Set prompts.agentInstructions before creating the default agent.",
        );
      }

      const helper = new CDFHelper(sdk);
      const { agents: currentAgents } = await helper.getAgents();
      const existingAgent = currentAgents.find((agent) => agent.name === name);
      if (existingAgent) {
        return existingAgent.externalId;
      }

      const canWrite = await helper.canEditAgentConfig();
      if (!canWrite) {
        throw new Error(
          "agents:write is required to create the default agent.",
        );
      }

      const models = await helper.getSupportedModelNames();
      const preferred = config.agent.defaultLanguageModel;
      const model = models.includes(preferred) ? preferred : models[0];
      if (!model) {
        throw new Error(
          "No language model is available for the default agent.",
        );
      }
      if (model !== preferred) {
        console.info(
          `Configured model ${preferred} is not supported; using ${model}.`,
        );
      }

      const externalId = `${sdk.project}${config.agent.externalIdSuffix}`;
      await helper.createAgent({
        externalId,
        name,
        description: DEFAULT_AGENT_DESCRIPTION,
        instructions,
        model,
        tools: [],
      });

      return externalId;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: CDF_QUERY_KEYS.agents });
    },
  });
}
