import { agentSessionReducer, pickAgentSession } from "./agentSlice";
import {
  pickSummaryGenerationSession,
  summaryGenerationSessionReducer,
} from "./summaryGenerationSlice";
import {
  pickTestingSession,
  testingSessionReducer,
} from "./testingSessionSlice";
import type { OperationsAction, OperationsState } from "./types";

export function operationsReducer(
  state: OperationsState,
  action: OperationsAction,
): OperationsState {
  const agent = pickAgentSession(state);
  const nextAgent = agentSessionReducer(agent, action);
  const summary = pickSummaryGenerationSession(state);
  const nextSummary = summaryGenerationSessionReducer(summary, action);
  const testing = pickTestingSession(state);
  const nextTesting = testingSessionReducer(testing, action);

  if (
    nextAgent === agent &&
    nextSummary === summary &&
    nextTesting === testing
  ) {
    return state;
  }

  return {
    ...state,
    ...nextAgent,
    ...nextSummary,
    ...nextTesting,
  };
}
