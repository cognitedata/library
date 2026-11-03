import type { Dispatch } from "react";
import type { Agent } from "@/types/operations";
import type { OperationsAction, OperationsState } from "./types";

export type AgentSessionState = Pick<
  OperationsState,
  | "selectedAgent"
  | "agents"
  | "customInstructions"
  | "showAgentEditor"
  | "confirmDelete"
  | "agentInitError"
>;

export function pickAgentSession(state: OperationsState): AgentSessionState {
  return {
    selectedAgent: state.selectedAgent,
    agents: state.agents,
    customInstructions: state.customInstructions,
    showAgentEditor: state.showAgentEditor,
    confirmDelete: state.confirmDelete,
    agentInitError: state.agentInitError,
  };
}

export function agentSessionReducer(
  state: AgentSessionState,
  action: OperationsAction,
): AgentSessionState {
  switch (action.type) {
    case "SET_SELECTED_AGENT":
      return { ...state, selectedAgent: action.payload };
    case "SET_AGENTS":
      return { ...state, agents: action.payload };
    case "SET_CUSTOM_INSTRUCTIONS":
      return { ...state, customInstructions: action.payload };
    case "SET_SHOW_AGENT_EDITOR":
      return { ...state, showAgentEditor: action.payload };
    case "SET_CONFIRM_DELETE":
      return { ...state, confirmDelete: action.payload };
    case "SET_AGENT_INIT_ERROR":
      return { ...state, agentInitError: action.payload };
    default:
      return state;
  }
}

export type AgentSessionContextValue = AgentSessionState & {
  dispatch: Dispatch<OperationsAction>;
  setSelectedAgent: (agentId: string | null) => void;
  setAgents: (agents: Agent[]) => void;
};
