import { createContext, useContext } from "react";
import type { SummaryGenerationContextValue } from "@/contexts/operations-summarizer/summaryGenerationSlice";

export const SummaryGenerationContext =
  createContext<SummaryGenerationContextValue | null>(null);

/** Summary period, contents, prompts, and generation UI state. */
export function useSummaryGenerationContext(): SummaryGenerationContextValue {
  const ctx = useContext(SummaryGenerationContext);
  if (!ctx) {
    throw new Error(
      "useSummaryGenerationContext must be used within OperationsProvider",
    );
  }
  return ctx;
}
