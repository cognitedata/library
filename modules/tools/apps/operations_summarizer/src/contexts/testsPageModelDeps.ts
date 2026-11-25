/**
 * Context object and consumer hook for {@link TestsPageModelDeps}.
 * Kept out of the provider's .tsx file so that file stays eligible for React Fast Refresh.
 */

import { createContext, useContext } from "react";
import type { TestsPageModelDeps } from "./TestsPageModelDepsContext";

export const TestsPageModelDepsContext =
	createContext<TestsPageModelDeps | null>(null);

export function useTestsPageModelDeps(): TestsPageModelDeps {
	const ctx = useContext(TestsPageModelDepsContext);
	if (!ctx) {
		throw new Error(
			"useTestsPageModelDeps must be used within TestsPageModelDepsProvider"
		);
	}
	return ctx;
}
