/**
 * Test configuration and result state from Operations context.
 * Single place for all test-related context state and setters.
 */

import { useTestingSessionContext } from "@/contexts/OperationsSummarizerContext";
import type {
  DataTypeName,
  TestGenerationProgress,
  TestResultData,
} from "@/types/operations";

interface UseTestingStateResult {
  // Configuration (persisted)
  selectedSummaryId: string;
  setSelectedSummaryId: (id: string) => void;
  testStartDate: string;
  setTestStartDate: (date: string) => void;
  testEndDate: string;
  setTestEndDate: (date: string) => void;
  selectedDataTypes: DataTypeName[];
  setSelectedDataTypes: (types: DataTypeName[]) => void;

  // Test run state (persisted)
  isRunning: boolean;
  setIsRunning: (value: boolean) => void;
  generationProgress: TestGenerationProgress;
  setGenerationProgress: (progress: TestGenerationProgress) => void;
  testError: string | null;
  setTestError: (error: string | null) => void;

  // YAML export (persisted)
  yamlContent: string;
  setYamlContent: (content: string) => void;
  yamlExported: boolean;
  setYamlExported: (value: boolean) => void;

  // Result (persisted)
  testResultData: TestResultData | null;
  setTestResultData: (data: TestResultData | null) => void;

  dispatch: ReturnType<typeof useTestingSessionContext>["dispatch"];
}

export function useTestingState(): UseTestingStateResult {
  const {
    dispatch,
    testSelectedSummaryId,
    testStartDate,
    testEndDate,
    testSelectedDataTypes,
    isTesting,
    testGenerationProgress,
    testError,
    yamlContent,
    yamlExported,
    testResultData,
  } = useTestingSessionContext();

  return {
    selectedSummaryId: testSelectedSummaryId,
    setSelectedSummaryId: (id) =>
      dispatch({ type: "SET_TEST_SELECTED_SUMMARY_ID", payload: id }),
    testStartDate,
    setTestStartDate: (date) =>
      dispatch({ type: "SET_TEST_START_DATE", payload: date }),
    testEndDate,
    setTestEndDate: (date) =>
      dispatch({ type: "SET_TEST_END_DATE", payload: date }),
    selectedDataTypes: testSelectedDataTypes as DataTypeName[],
    setSelectedDataTypes: (types) =>
      dispatch({ type: "SET_TEST_SELECTED_DATA_TYPES", payload: types }),

    isRunning: isTesting,
    setIsRunning: (value) =>
      dispatch({ type: "SET_IS_TESTING", payload: value }),
    generationProgress: testGenerationProgress,
    setGenerationProgress: (progress) =>
      dispatch({ type: "SET_TEST_GENERATION_PROGRESS", payload: progress }),
    testError,
    setTestError: (error) =>
      dispatch({ type: "SET_TEST_ERROR", payload: error }),

    yamlContent,
    setYamlContent: (content) =>
      dispatch({ type: "SET_YAML_CONTENT", payload: content }),
    yamlExported,
    setYamlExported: (value) =>
      dispatch({ type: "SET_YAML_EXPORTED", payload: value }),

    testResultData,
    setTestResultData: (data) =>
      dispatch({ type: "SET_TEST_RESULT_DATA", payload: data }),

    dispatch,
  };
}
