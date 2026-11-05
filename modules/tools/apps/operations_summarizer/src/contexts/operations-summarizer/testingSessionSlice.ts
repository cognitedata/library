import type { Dispatch } from "react";
import type { OperationsAction, OperationsState } from "./types";

export type TestingSessionState = Pick<
  OperationsState,
  | "testResults"
  | "testResultData"
  | "isTesting"
  | "testGenerationProgress"
  | "testError"
  | "testSaved"
  | "yamlExported"
  | "yamlContent"
  | "testSelectedSummaryId"
  | "testStartDate"
  | "testEndDate"
  | "testSelectedDataTypes"
>;

export function pickTestingSession(
  state: OperationsState,
): TestingSessionState {
  return {
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
  };
}

export function testingSessionReducer(
  state: TestingSessionState,
  action: OperationsAction,
): TestingSessionState {
  switch (action.type) {
    case "SET_TEST_RESULTS":
      return { ...state, testResults: action.payload };
    case "SET_TEST_RESULT_DATA":
      return { ...state, testResultData: action.payload };
    case "SET_IS_TESTING":
      return { ...state, isTesting: action.payload };
    case "SET_TEST_GENERATION_PROGRESS":
      return { ...state, testGenerationProgress: action.payload };
    case "SET_TEST_ERROR":
      return { ...state, testError: action.payload };
    case "SET_TEST_SAVED":
      return { ...state, testSaved: action.payload };
    case "SET_YAML_EXPORTED":
      return { ...state, yamlExported: action.payload };
    case "SET_YAML_CONTENT":
      return { ...state, yamlContent: action.payload };
    case "SET_TEST_SELECTED_SUMMARY_ID":
      return { ...state, testSelectedSummaryId: action.payload };
    case "SET_TEST_START_DATE":
      return { ...state, testStartDate: action.payload };
    case "SET_TEST_END_DATE":
      return { ...state, testEndDate: action.payload };
    case "SET_TEST_SELECTED_DATA_TYPES":
      return { ...state, testSelectedDataTypes: action.payload };
    case "RESET_TEST_STATE":
      return {
        ...state,
        testResults: null,
        testResultData: null,
        isTesting: false,
        testGenerationProgress: { status: "idle", message: "", progress: 0 },
        testError: null,
        testSaved: false,
        yamlExported: false,
        yamlContent: "",
      };
    default:
      return state;
  }
}

export type TestingSessionContextValue = TestingSessionState & {
  dispatch: Dispatch<OperationsAction>;
  resetTestState: () => void;
};
