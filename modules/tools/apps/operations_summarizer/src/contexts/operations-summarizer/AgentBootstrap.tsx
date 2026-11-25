import { type Dispatch, useEffect, useRef } from "react";
import { getAppConfig } from "@/config/appConfig";
import { useCdfClient } from "@/contexts/CdfClientContext";
import type { OperationsAction } from "@/contexts/operations-summarizer/types";
import { useEnsureDefaultAgent } from "@/hooks/useAgentsCdf";
import { CDFHelper } from "@/services/cdfHelper";
import type { Agent } from "@/types/operations";

export function AgentBootstrap({
	dispatch,
	selectedAgent,
	setAgents,
	setSelectedAgent,
}: {
	dispatch: Dispatch<OperationsAction>;
	selectedAgent: string | null;
	setAgents: (agents: Agent[]) => void;
	setSelectedAgent: (id: string | null) => void;
}) {
	const { sdk, isAuthLoading: isLoading } = useCdfClient();
	const { mutateAsync: createDefaultAgent } = useEnsureDefaultAgent();
	const hasInitializedRef = useRef(false);
	// Track selectedAgent in a ref so the effect doesn't re-run when it changes,
	// while still reading the latest value when the async result arrives.
	const selectedAgentRef = useRef(selectedAgent);
	selectedAgentRef.current = selectedAgent;
	// The mutation object changes every render; keep the latest callable in a ref
	// so it does not retrigger the effect.
	const createDefaultAgentRef = useRef(createDefaultAgent);
	createDefaultAgentRef.current = createDefaultAgent;

	useEffect(() => {
		if (!sdk || isLoading || hasInitializedRef.current) return;

		// Claim the initialization slot before the async call so that a
		// rapid re-render (e.g. sdk reference flip) cannot start a second fetch.
		hasInitializedRef.current = true;
		let cancelled = false;

		const initializeAgents = async () => {
			try {
				const helper = new CDFHelper(sdk);
				const config = getAppConfig();
				const agentExternalId = config.agent.externalId;
				const findHelperAgent = (list: Agent[]) =>
					list.find((a) => a.externalId === agentExternalId);

				let { agents } = await helper.getAgents();
				let helperAgent = findHelperAgent(agents);

				if (!helperAgent && (await helper.canEditAgentConfig())) {
					await createDefaultAgentRef.current();
					({ agents } = await helper.getAgents());
					helperAgent = findHelperAgent(agents);
				}

				if (cancelled) return;

				setAgents(agents);

				if (!helperAgent) {
					dispatch({
						type: "SET_AGENT_INIT_ERROR",
						payload: `Agent '${agentExternalId}' not found. Generate is disabled until an admin with agents:write creates it.`,
					});
					return;
				}

				dispatch({ type: "SET_AGENT_INIT_ERROR", payload: null });
				const fromAgent = helperAgent.instructions?.trim() ?? "";
				dispatch({
					type: "SET_CUSTOM_INSTRUCTIONS",
					payload:
						fromAgent !== "" ? fromAgent : config.prompts.agentInstructions,
				});

				if (!selectedAgentRef.current) {
					setSelectedAgent(helperAgent.externalId);
				}
			} catch (error) {
				if (!cancelled) {
					// Release the claim so a retry is possible (e.g. after network recovery).
					hasInitializedRef.current = false;
					const message =
						error instanceof Error ? error.message : "Failed to load agents";
					console.error("Error initializing agents:", error);
					dispatch({ type: "SET_AGENT_INIT_ERROR", payload: message });
				}
			}
		};

		void initializeAgents();

		return () => {
			cancelled = true;
			// Reset so that StrictMode's second invocation (or a dep-change re-run)
			// can start a fresh fetch rather than being blocked by the stale claim.
			hasInitializedRef.current = false;
		};
	}, [sdk, isLoading, setAgents, setSelectedAgent, dispatch]);

	return null;
}
