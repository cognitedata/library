/**
 * Resolver Provider
 *
 * Provides data resolution functions to plugins without them needing
 * to import from canvas-app directly.
 *
 * This allows canvas-plugins to remain independent of canvas-app while
 * still being able to resolve FDM/CDF data sources.
 */

import type React from "react";

import type { DataResolvers } from "./resolverContextDef";
import { ResolverContext } from "./resolverContextDef";

export interface ResolverProviderProps {
	children: React.ReactNode;
	resolvers: DataResolvers;
}

/**
 * Provider component for data resolvers
 * Canvas-app should wrap plugins with this provider
 */
export const ResolverProvider: React.FC<ResolverProviderProps> = ({
	children,
	resolvers,
}) => {
	return (
		<ResolverContext.Provider value={resolvers}>
			{children}
		</ResolverContext.Provider>
	);
};
