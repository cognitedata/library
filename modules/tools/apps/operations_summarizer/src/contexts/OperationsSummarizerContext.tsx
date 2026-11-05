/**
 * Operations Summarizer session contexts — barrel re-exports.
 * Slice hooks and provider live in `operations-summarizer/` for independent maintenance.
 */

export { useAgentContext } from "@/contexts/operations-summarizer/agentSessionContext";
export {
  OperationsProvider,
  useOperations,
} from "@/contexts/operations-summarizer/operationsProvider";
export { useSummaryGenerationContext } from "@/contexts/operations-summarizer/summaryGenerationContext";
export { useTestingSessionContext } from "@/contexts/operations-summarizer/testingSessionContext";
export type {
  OperationsAction,
  OperationsState,
} from "@/contexts/operations-summarizer/types";
