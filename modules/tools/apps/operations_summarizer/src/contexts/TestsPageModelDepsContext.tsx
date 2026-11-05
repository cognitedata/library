/**
 * Injectable dependencies for {@link useTestsPageModel}.
 */

import { createContext, type ReactNode, useContext } from "react";
import { useComparisonTest } from "@/hooks/useComparisonTest";
import {
  type TestsPageCdfBundleValue,
  useDefaultTestsPageCdfBundle,
} from "@/hooks/useDefaultTestsPageCdfBundle";
import { saveTestReportToCdf } from "@/utils/testingSave";

export type ComparisonTestVmValue = ReturnType<typeof useComparisonTest>;

export interface TestsPageModelDeps {
  useTestsPageCdfBundle: () => TestsPageCdfBundleValue;
  useComparisonTest: () => ComparisonTestVmValue;
  saveTestReportToCdf: typeof saveTestReportToCdf;
}

export const DEFAULT_TESTS_PAGE_MODEL_DEPS: TestsPageModelDeps = {
  useTestsPageCdfBundle: useDefaultTestsPageCdfBundle,
  useComparisonTest,
  saveTestReportToCdf,
};

const TestsPageModelDepsContext = createContext<TestsPageModelDeps | null>(
  null,
);

export function TestsPageModelDepsProvider({
  children,
  deps,
}: {
  children: ReactNode;
  deps?: TestsPageModelDeps;
}) {
  const value = deps ?? DEFAULT_TESTS_PAGE_MODEL_DEPS;
  return (
    <TestsPageModelDepsContext.Provider value={value}>
      {children}
    </TestsPageModelDepsContext.Provider>
  );
}

export function useTestsPageModelDeps(): TestsPageModelDeps {
  const ctx = useContext(TestsPageModelDepsContext);
  if (!ctx) {
    throw new Error(
      "useTestsPageModelDeps must be used within TestsPageModelDepsProvider",
    );
  }
  return ctx;
}
