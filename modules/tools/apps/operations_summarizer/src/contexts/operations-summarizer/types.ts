/**
 * Composed app session state for Operations Summarizer (split across slice reducers).
 */

import type {
  Agent,
  GeneratedSummary,
  SummaryContents,
  TestGenerationProgress,
  TestResultData,
  TestResults,
} from "@/types/operations";

/** Default toggles until `@/config/dataTypes` exists on this stack. */
function getDefaultSummaryContents(): SummaryContents {
  return {
    assets: true,
    files: true,
    work_orders: true,
    time_series: true,
    equipment: true,
  };
}

// ============================================================
// Generation progress (summary flow)
// ============================================================

export interface GenerationProgress {
  status:
    | "idle"
    | "fetching"
    | "processing"
    | "generating"
    | "saving"
    | "complete"
    | "error";
  message: string;
  progress: number;
  currentStep?: string;
}

// ============================================================
// Composed state
// ============================================================

export interface OperationsState {
  selectedAgent: string | null;
  agents: Agent[];
  customInstructions: string;
  showAgentEditor: boolean;
  confirmDelete: boolean;
  agentInitError: string | null;

  summaryPeriodStart: Date;
  summaryPeriodEnd: Date;
  summaryContents: SummaryContents;
  summaryPrompts: Record<string, string>;
  isGenerating: boolean;
  generationProgress: GenerationProgress;
  generationError: string | null;
  showSummary: boolean;
  generatedSummary: GeneratedSummary | null;

  testResults: TestResults | null;
  testResultData: TestResultData | null;
  isTesting: boolean;
  testGenerationProgress: TestGenerationProgress;
  testError: string | null;
  testSaved: boolean;
  yamlExported: boolean;
  yamlContent: string;
  testSelectedSummaryId: string;
  /** Calendar ISO `YYYY-MM-DD` (same convention as {@link summaryPeriodStart}). */
  testStartDate: string;
  /** Calendar ISO `YYYY-MM-DD`. */
  testEndDate: string;
  testSelectedDataTypes: string[];
}

export type OperationsAction =
  | { type: "SET_SELECTED_AGENT"; payload: string | null }
  | { type: "SET_AGENTS"; payload: Agent[] }
  | { type: "SET_CUSTOM_INSTRUCTIONS"; payload: string }
  | { type: "SET_SHOW_AGENT_EDITOR"; payload: boolean }
  | { type: "SET_CONFIRM_DELETE"; payload: boolean }
  | { type: "SET_SUMMARY_PERIOD_START"; payload: Date }
  | { type: "SET_SUMMARY_PERIOD_END"; payload: Date }
  | { type: "SET_AGENT_INIT_ERROR"; payload: string | null }
  | { type: "SET_SUMMARY_CONTENTS"; payload: SummaryContents }
  | { type: "TOGGLE_SUMMARY_CONTENT"; payload: keyof SummaryContents }
  | { type: "SET_SUMMARY_PROMPTS"; payload: Record<string, string> }
  | { type: "SET_IS_GENERATING"; payload: boolean }
  | { type: "SET_GENERATION_PROGRESS"; payload: GenerationProgress }
  | { type: "SET_GENERATION_ERROR"; payload: string | null }
  | { type: "SET_SHOW_SUMMARY"; payload: boolean }
  | { type: "SET_GENERATED_SUMMARY"; payload: GeneratedSummary | null }
  | { type: "SET_TEST_RESULTS"; payload: TestResults | null }
  | { type: "SET_TEST_RESULT_DATA"; payload: TestResultData | null }
  | { type: "SET_IS_TESTING"; payload: boolean }
  | { type: "SET_TEST_GENERATION_PROGRESS"; payload: TestGenerationProgress }
  | { type: "SET_TEST_ERROR"; payload: string | null }
  | { type: "SET_TEST_SAVED"; payload: boolean }
  | { type: "SET_YAML_EXPORTED"; payload: boolean }
  | { type: "SET_YAML_CONTENT"; payload: string }
  | { type: "SET_TEST_SELECTED_SUMMARY_ID"; payload: string }
  | { type: "SET_TEST_START_DATE"; payload: string }
  | { type: "SET_TEST_END_DATE"; payload: string }
  | { type: "SET_TEST_SELECTED_DATA_TYPES"; payload: string[] }
  | { type: "RESET_TEST_STATE" }
  | { type: "RESET_SUMMARY_STATE" };

/**
 * Fresh initial session state (e.g. when the operations provider mounts).
 * Uses the current time for the default summary period so it does not freeze
 * to the calendar day captured at module load.
 */
export function createInitialOperationsState(): OperationsState {
  const now = new Date();
  return {
    selectedAgent: null,
    agents: [],
    customInstructions: "",
    showAgentEditor: false,
    confirmDelete: false,
    agentInitError: null,

    summaryPeriodStart: now,
    summaryPeriodEnd: now,
    summaryContents: getDefaultSummaryContents(),
    summaryPrompts: {},
    isGenerating: false,
    generationProgress: {
      status: "idle",
      message: "Ready to generate",
      progress: 0,
    },
    generationError: null,
    showSummary: false,
    generatedSummary: null,

    testResults: null,
    testResultData: null,
    isTesting: false,
    testGenerationProgress: {
      status: "idle",
      message: "",
      progress: 0,
    },
    testError: null,
    testSaved: false,
    yamlExported: false,
    yamlContent: "",
    testSelectedSummaryId: "",
    testStartDate: "",
    testEndDate: "",
    testSelectedDataTypes: [],
  };
}
