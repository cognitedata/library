import { render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getAppConfig } from "@/config/appConfig";
import { AgentBootstrap } from "@/contexts/operations-summarizer/AgentBootstrap";
import { CDFHelper } from "@/services/cdfHelper";
import { partialCogniteClient } from "@/test-utils/testDoubles";
import type { Agent } from "@/types/operations";

const { mutateAsync } = vi.hoisted(() => ({ mutateAsync: vi.fn() }));

vi.mock("@/contexts/CdfClientContext", () => ({
	useCdfClient: () => ({
		sdk: partialCogniteClient(),
		isAuthLoading: false,
	}),
}));

vi.mock("@/hooks/useAgentsCdf", () => ({
	useEnsureDefaultAgent: () => ({ mutateAsync }),
}));

function helperAgent(overrides: Partial<Agent> = {}): Agent {
	return {
		externalId: getAppConfig().agent.externalId,
		name: "Any display name",
		instructions: "agent instructions",
		...overrides,
	};
}

function mockAgentLists(...lists: Agent[][]) {
	const getAgents = vi.spyOn(CDFHelper.prototype, "getAgents");
	for (const agents of lists) {
		getAgents.mockResolvedValueOnce({ agents, invalidAgents: [] });
	}
	return getAgents;
}

function renderBootstrap() {
	const dispatch = vi.fn();
	const setAgents = vi.fn();
	const setSelectedAgent = vi.fn();
	render(
		<AgentBootstrap
			dispatch={dispatch}
			selectedAgent={null}
			setAgents={setAgents}
			setSelectedAgent={setSelectedAgent}
		/>
	);
	return { dispatch, setAgents, setSelectedAgent };
}

describe("AgentBootstrap", () => {
	beforeEach(() => {
		mutateAsync.mockReset();
		vi.spyOn(console, "error").mockImplementation(() => {});
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("selects the agent whose external id matches the config node", async () => {
		mockAgentLists([
			{ externalId: "other_agent", name: getAppConfig().agent.name },
			helperAgent(),
		]);
		const { setSelectedAgent } = renderBootstrap();

		await waitFor(() =>
			expect(setSelectedAgent).toHaveBeenCalledWith(
				getAppConfig().agent.externalId
			)
		);
		expect(mutateAsync).not.toHaveBeenCalled();
	});

	it("creates the agent when it is missing and the user has agents:write", async () => {
		const other: Agent = {
			externalId: "other_agent",
			name: getAppConfig().agent.name,
		};
		mockAgentLists([other], [other, helperAgent()]);
		vi.spyOn(CDFHelper.prototype, "canEditAgentConfig").mockResolvedValue(true);
		mutateAsync.mockResolvedValue(getAppConfig().agent.externalId);
		const { setSelectedAgent } = renderBootstrap();

		await waitFor(() =>
			expect(setSelectedAgent).toHaveBeenCalledWith(
				getAppConfig().agent.externalId
			)
		);
		expect(mutateAsync).toHaveBeenCalledTimes(1);
	});

	it("reports the missing agent without creating it when the user cannot write agents", async () => {
		mockAgentLists([]);
		vi.spyOn(CDFHelper.prototype, "canEditAgentConfig").mockResolvedValue(
			false
		);
		const { dispatch, setSelectedAgent } = renderBootstrap();

		await waitFor(() =>
			expect(dispatch).toHaveBeenCalledWith({
				type: "SET_AGENT_INIT_ERROR",
				payload: expect.stringContaining(getAppConfig().agent.externalId),
			})
		);
		expect(mutateAsync).not.toHaveBeenCalled();
		expect(setSelectedAgent).not.toHaveBeenCalled();
	});
});
