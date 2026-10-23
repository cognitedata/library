import { QueryClientProvider } from "@tanstack/react-query";
import type React from "react";
import { useState } from "react";

import {
	type CanvasPreviewCaches,
	CanvasPreviewContext,
	createCanvasPreviewCaches,
} from "./context";

export interface CanvasPreviewProviderProps {
	children: React.ReactNode;
	/**
	 * Optional pre-created caches. Use createCanvasPreviewCaches() to build these
	 * when you need isolated instances (e.g. testing, micro-frontends).
	 * If omitted, the provider creates its own caches automatically.
	 */
	caches?: CanvasPreviewCaches;
}

/**
 * Wrapper for PreviewCanvas.
 *
 * Owns the QueryClient so it is isolated per provider instance rather than
 * shared as a module singleton.
 *
 * Basic usage:
 * ```tsx
 * <CanvasPreviewProvider>
 *   <MyApp />
 * </CanvasPreviewProvider>
 * ```
 *
 * Custom caches (for testing / micro-frontends):
 * ```tsx
 * const caches = createCanvasPreviewCaches();
 * <CanvasPreviewProvider caches={caches}>
 *   <MyApp />
 * </CanvasPreviewProvider>
 * ```
 */
export function CanvasPreviewProvider({
	children,
	caches,
}: CanvasPreviewProviderProps) {
	const [ctx] = useState(() => caches ?? createCanvasPreviewCaches());

	return (
		<CanvasPreviewContext.Provider value={ctx}>
			<QueryClientProvider client={ctx.queryClient}>
				{children}
			</QueryClientProvider>
		</CanvasPreviewContext.Provider>
	);
}
