import { createContext, useContext } from "react";
import type { AgentSessionContextValue } from "@/contexts/operations-summarizer/agentSlice";

export const AgentSessionContext =
  createContext<AgentSessionContextValue | null>(null);

/** Agent list, selection, and editor flags. */
export function useAgentContext(): AgentSessionContextValue {
  const ctx = useContext(AgentSessionContext);
  if (!ctx) {
    throw new Error("useAgentContext must be used within OperationsProvider");
  }
  return ctx;
}
