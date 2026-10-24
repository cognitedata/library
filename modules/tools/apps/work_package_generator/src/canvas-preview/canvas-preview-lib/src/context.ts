import { QueryClient } from "@tanstack/react-query";
import { createContext } from "react";

export interface CanvasPreviewCaches {
	queryClient: QueryClient;
}

/**
 * Create a fresh set of caches for use with CanvasPreviewProvider.
 *
 * Call this if you need isolated caches (e.g. for testing or micro-frontends).
 * Otherwise, CanvasPreviewProvider calls this automatically.
 */
export function createCanvasPreviewCaches(): CanvasPreviewCaches {
	return {
		queryClient: new QueryClient({
			defaultOptions: {
				queries: {
					retry: false,
					staleTime: Infinity,
				},
			},
		}),
	};
}

export const CanvasPreviewContext = createContext<CanvasPreviewCaches | null>(
	null
);
