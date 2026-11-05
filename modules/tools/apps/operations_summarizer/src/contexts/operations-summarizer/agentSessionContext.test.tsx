import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import {
  AgentSessionContext,
  useAgentContext,
} from "@/contexts/operations-summarizer/agentSessionContext";
import {
  type AgentSessionContextValue,
  pickAgentSession,
} from "@/contexts/operations-summarizer/agentSlice";
import { createInitialOperationsState } from "@/contexts/operations-summarizer/types";

describe("useAgentContext", () => {
  it("throws when rendered outside OperationsProvider", () => {
    expect(() => renderHook(() => useAgentContext())).toThrow(
      "useAgentContext must be used within OperationsProvider",
    );
  });

  it("returns the agent session provided by AgentSessionContext", () => {
    const setSelectedAgent = vi.fn();
    const value: AgentSessionContextValue = {
      ...pickAgentSession(createInitialOperationsState()),
      dispatch: vi.fn(),
      setSelectedAgent,
      setAgents: vi.fn(),
    };
    const wrapper = ({ children }: { children: ReactNode }) => (
      <AgentSessionContext.Provider value={value}>
        {children}
      </AgentSessionContext.Provider>
    );

    const { result } = renderHook(() => useAgentContext(), { wrapper });

    expect(result.current.selectedAgent).toBeNull();
    expect(result.current.agents).toEqual([]);
    act(() => {
      result.current.setSelectedAgent("agent-1");
    });
    expect(setSelectedAgent).toHaveBeenCalledWith("agent-1");
  });
});
