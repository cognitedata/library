/**
 * Operations session provider: one reducer, focused slice contexts for subscribers.
 */

import {
  createContext,
  type Dispatch,
  type ReactNode,
  useCallback,
  useContext,
  useMemo,
  useReducer,
} from "react";
import { AgentBootstrap } from "@/contexts/operations-summarizer/AgentBootstrap";
import { AgentSessionContext } from "@/contexts/operations-summarizer/agentSessionContext";
import type { AgentSessionContextValue } from "@/contexts/operations-summarizer/agentSlice";
import { operationsReducer } from "@/contexts/operations-summarizer/rootReducer";
import { SummaryGenerationContext } from "@/contexts/operations-summarizer/summaryGenerationContext";
import type { SummaryGenerationContextValue } from "@/contexts/operations-summarizer/summaryGenerationSlice";
import { TestingSessionContext } from "@/contexts/operations-summarizer/testingSessionContext";
import type { TestingSessionContextValue } from "@/contexts/operations-summarizer/testingSessionSlice";
import {
  createInitialOperationsState,
  type OperationsAction,
  type OperationsState,
} from "@/contexts/operations-summarizer/types";
import type { Agent, SummaryContents } from "@/types/operations";

interface OperationsContextValue {
  state: OperationsState;
  dispatch: Dispatch<OperationsAction>;
  setSelectedAgent: (agentId: string | null) => void;
  setAgents: (agents: Agent[]) => void;
  setSummaryPeriod: (start: Date, end: Date) => void;
  toggleSummaryContent: (key: keyof SummaryContents) => void;
  resetTestState: () => void;
  resetSummaryState: () => void;
}

const OperationsContext = createContext<OperationsContextValue | null>(null);

export function OperationsProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(
    operationsReducer,
    undefined,
    createInitialOperationsState,
  );

  const setSelectedAgent = useCallback((agentId: string | null) => {
    dispatch({ type: "SET_SELECTED_AGENT", payload: agentId });
  }, []);

  const setAgents = useCallback((agents: Agent[]) => {
    dispatch({ type: "SET_AGENTS", payload: agents });
  }, []);

  const setSummaryPeriod = useCallback((start: Date, end: Date) => {
    dispatch({ type: "SET_SUMMARY_PERIOD_START", payload: start });
    dispatch({ type: "SET_SUMMARY_PERIOD_END", payload: end });
  }, []);

  const toggleSummaryContent = useCallback((key: keyof SummaryContents) => {
    dispatch({ type: "TOGGLE_SUMMARY_CONTENT", payload: key });
  }, []);

  const resetTestState = useCallback(() => {
    dispatch({ type: "RESET_TEST_STATE" });
  }, []);

  const resetSummaryState = useCallback(() => {
    dispatch({ type: "RESET_SUMMARY_STATE" });
  }, []);

  const agentContextValue = useMemo<AgentSessionContextValue>(
    () => ({
      selectedAgent: state.selectedAgent,
      agents: state.agents,
      customInstructions: state.customInstructions,
      showAgentEditor: state.showAgentEditor,
      confirmDelete: state.confirmDelete,
      agentInitError: state.agentInitError,
      dispatch,
      setSelectedAgent,
      setAgents,
    }),
    [
      state.selectedAgent,
      state.agents,
      state.customInstructions,
      state.showAgentEditor,
      state.confirmDelete,
      state.agentInitError,
      setSelectedAgent,
      setAgents,
    ],
  );

  const summaryContextValue = useMemo<SummaryGenerationContextValue>(
    () => ({
      summaryPeriodStart: state.summaryPeriodStart,
      summaryPeriodEnd: state.summaryPeriodEnd,
      summaryContents: state.summaryContents,
      summaryPrompts: state.summaryPrompts,
      isGenerating: state.isGenerating,
      generationProgress: state.generationProgress,
      generationError: state.generationError,
      showSummary: state.showSummary,
      generatedSummary: state.generatedSummary,
      dispatch,
      setSummaryPeriod,
      toggleSummaryContent,
      resetSummaryState,
    }),
    [
      state.summaryPeriodStart,
      state.summaryPeriodEnd,
      state.summaryContents,
      state.summaryPrompts,
      state.isGenerating,
      state.generationProgress,
      state.generationError,
      state.showSummary,
      state.generatedSummary,
      setSummaryPeriod,
      toggleSummaryContent,
      resetSummaryState,
    ],
  );

  const testingContextValue = useMemo<TestingSessionContextValue>(
    () => ({
      testResults: state.testResults,
      testResultData: state.testResultData,
      isTesting: state.isTesting,
      testGenerationProgress: state.testGenerationProgress,
      testError: state.testError,
      testSaved: state.testSaved,
      yamlExported: state.yamlExported,
      yamlContent: state.yamlContent,
      testSelectedSummaryId: state.testSelectedSummaryId,
      testStartDate: state.testStartDate,
      testEndDate: state.testEndDate,
      testSelectedDataTypes: state.testSelectedDataTypes,
      dispatch,
      resetTestState,
    }),
    [
      state.testResults,
      state.testResultData,
      state.isTesting,
      state.testGenerationProgress,
      state.testError,
      state.testSaved,
      state.yamlExported,
      state.yamlContent,
      state.testSelectedSummaryId,
      state.testStartDate,
      state.testEndDate,
      state.testSelectedDataTypes,
      resetTestState,
    ],
  );

  const operationsValue = useMemo<OperationsContextValue>(
    () => ({
      state,
      dispatch,
      setSelectedAgent,
      setAgents,
      setSummaryPeriod,
      toggleSummaryContent,
      resetTestState,
      resetSummaryState,
    }),
    [
      state,
      setSelectedAgent,
      setAgents,
      setSummaryPeriod,
      toggleSummaryContent,
      resetTestState,
      resetSummaryState,
    ],
  );

  return (
    <OperationsContext.Provider value={operationsValue}>
      <AgentSessionContext.Provider value={agentContextValue}>
        <SummaryGenerationContext.Provider value={summaryContextValue}>
          <TestingSessionContext.Provider value={testingContextValue}>
            <AgentBootstrap
              dispatch={dispatch}
              selectedAgent={state.selectedAgent}
              setAgents={setAgents}
              setSelectedAgent={setSelectedAgent}
            />
            {children}
          </TestingSessionContext.Provider>
        </SummaryGenerationContext.Provider>
      </AgentSessionContext.Provider>
    </OperationsContext.Provider>
  );
}

/** Full operations facade (backward compatible). */
export function useOperations() {
  const context = useContext(OperationsContext);
  if (!context) {
    throw new Error("useOperations must be used within an OperationsProvider");
  }
  return context;
}
