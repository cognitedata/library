/**
 * Injectable dependencies for {@link useTestsPageModel}.
 */

import type { ReactNode } from "react";
import { TestsPageModelDepsContext } from "@/contexts/testsPageModelDeps";
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

const DEFAULT_TESTS_PAGE_MODEL_DEPS: TestsPageModelDeps = {
	useTestsPageCdfBundle: useDefaultTestsPageCdfBundle,
	useComparisonTest,
	saveTestReportToCdf,
};

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
