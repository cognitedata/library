import type { Dispatch } from "react";
import type { SummaryContents } from "@/types/operations";
import type { OperationsAction, OperationsState } from "./types";

export type SummaryGenerationSessionState = Pick<
  OperationsState,
  | "summaryPeriodStart"
  | "summaryPeriodEnd"
  | "summaryContents"
  | "summaryPrompts"
  | "isGenerating"
  | "generationProgress"
  | "generationError"
  | "showSummary"
  | "generatedSummary"
>;

export function pickSummaryGenerationSession(
  state: OperationsState,
): SummaryGenerationSessionState {
  return {
    summaryPeriodStart: state.summaryPeriodStart,
    summaryPeriodEnd: state.summaryPeriodEnd,
    summaryContents: state.summaryContents,
    summaryPrompts: state.summaryPrompts,
    isGenerating: state.isGenerating,
    generationProgress: state.generationProgress,
    generationError: state.generationError,
    showSummary: state.showSummary,
    generatedSummary: state.generatedSummary,
  };
}

export function summaryGenerationSessionReducer(
  state: SummaryGenerationSessionState,
  action: OperationsAction,
): SummaryGenerationSessionState {
  switch (action.type) {
    case "SET_SUMMARY_PERIOD_START":
      return { ...state, summaryPeriodStart: action.payload };
    case "SET_SUMMARY_PERIOD_END":
      return { ...state, summaryPeriodEnd: action.payload };
    case "SET_SUMMARY_CONTENTS":
      return { ...state, summaryContents: action.payload };
    case "TOGGLE_SUMMARY_CONTENT":
      return {
        ...state,
        summaryContents: {
          ...state.summaryContents,
          [action.payload]: !state.summaryContents[action.payload],
        },
      };
    case "SET_SUMMARY_PROMPTS":
      return { ...state, summaryPrompts: action.payload };
    case "SET_IS_GENERATING":
      return { ...state, isGenerating: action.payload };
    case "SET_GENERATION_PROGRESS":
      return { ...state, generationProgress: action.payload };
    case "SET_GENERATION_ERROR":
      return { ...state, generationError: action.payload };
    case "SET_SHOW_SUMMARY":
      return { ...state, showSummary: action.payload };
    case "SET_GENERATED_SUMMARY":
      return { ...state, generatedSummary: action.payload };
    case "RESET_SUMMARY_STATE":
      return {
        ...state,
        isGenerating: false,
        generationProgress: {
          status: "idle",
          message: "Ready to generate",
          progress: 0,
        },
        generationError: null,
        showSummary: false,
        generatedSummary: null,
      };
    default:
      return state;
  }
}

export type SummaryGenerationContextValue = SummaryGenerationSessionState & {
  dispatch: Dispatch<OperationsAction>;
  setSummaryPeriod: (start: Date, end: Date) => void;
  toggleSummaryContent: (key: keyof SummaryContents) => void;
  resetSummaryState: () => void;
};
