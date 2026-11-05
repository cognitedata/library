/**
 * App-Level Contexts
 *
 * These contexts are used across the entire application.
 * Feature-specific contexts should be in their respective feature folders.
 *
 * @example
 * ```tsx
 * import { NavigationProvider, useNavigation } from '@/contexts';
 * import { OperationsProvider, useOperations } from '@/contexts';
 * ```
 */

export {
  type CdfClientContextValue,
  CdfClientProvider,
  useCdfClient,
} from "./CdfClientContext";
export { NavigationProvider, useNavigation } from "./NavigationContext";
export {
  type OperationsAction,
  OperationsProvider,
  type OperationsState,
  useAgentContext,
  useOperations,
  useSummaryGenerationContext,
  useTestingSessionContext,
} from "./OperationsSummarizerContext";
export {
  DEFAULT_SUMMARIES_PAGE_MODEL_DEPS,
  type SummariesPageModelDeps,
  SummariesPageModelDepsProvider,
  type SummariesRobustDownloadValue,
  type SummaryGenerationVmValue,
  useSummariesPageModelDeps,
} from "./SummariesPageModelDepsContext";
export {
  type ComparisonTestVmValue,
  DEFAULT_TESTS_PAGE_MODEL_DEPS,
  type TestsPageModelDeps,
  TestsPageModelDepsProvider,
  useTestsPageModelDeps,
} from "./TestsPageModelDepsContext";
