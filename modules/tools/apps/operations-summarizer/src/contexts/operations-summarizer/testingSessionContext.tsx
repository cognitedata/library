import { createContext, useContext } from "react";
import type { TestingSessionContextValue } from "@/contexts/operations-summarizer/testingSessionSlice";

export const TestingSessionContext =
  createContext<TestingSessionContextValue | null>(null);

/** Comparison test configuration, progress, YAML, and results. */
export function useTestingSessionContext(): TestingSessionContextValue {
  const ctx = useContext(TestingSessionContext);
  if (!ctx) {
    throw new Error(
      "useTestingSessionContext must be used within OperationsProvider",
    );
  }
  return ctx;
}
