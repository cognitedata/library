/**
 * Settings page. Shows the selected agent and the prompts stored on the config node.
 */

import { getAppConfig } from "@/config/appConfig";
import { useAgentLoader } from "@/features/dashboard-agent";
import { AgentRequiredCard } from "@/pages/settings/AgentRequiredCard";
import { AgentStatusCard } from "@/pages/settings/AgentStatusCard";
import { PromptsCard } from "@/pages/settings/PromptsCard";
import { SettingsHeader } from "@/pages/settings/SettingsHeader";

export function SettingsPage() {
  const { state, currentAgent, canEditAgentConfig, canEditAgentConfigLoading } =
    useAgentLoader();
  const prompts = getAppConfig().prompts;

  return (
    <div className="min-h-full bg-background">
      <div className="max-w-7xl mx-auto px-3 py-3 space-y-3">
        <SettingsHeader hasAgent={Boolean(state.selectedAgent)} />

        {state.selectedAgent ? null : (
          <AgentRequiredCard
            canWrite={canEditAgentConfig}
            isLoading={canEditAgentConfigLoading}
          />
        )}

        <AgentStatusCard agent={currentAgent} />

        <PromptsCard
          instructions={prompts.agentInstructions}
          prompts={prompts.dataTypes}
        />
      </div>
    </div>
  );
}
