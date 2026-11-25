/**
 * Context object and consumer hook for {@link SummariesPageModelDeps}.
 * Kept out of the provider's .tsx file so that file stays eligible for React Fast Refresh;
 * otherwise any hot update remounts the page and closes open modals.
 */

import { createContext, useContext } from "react";
import type { SummariesPageModelDeps } from "./SummariesPageModelDepsContext";

export const SummariesPageModelDepsContext =
	createContext<SummariesPageModelDeps | null>(null);

export function useSummariesPageModelDeps(): SummariesPageModelDeps {
	const ctx = useContext(SummariesPageModelDepsContext);
	if (!ctx) {
		throw new Error(
			"useSummariesPageModelDeps must be used within SummariesPageModelDepsProvider"
		);
	}
	return ctx;
}
