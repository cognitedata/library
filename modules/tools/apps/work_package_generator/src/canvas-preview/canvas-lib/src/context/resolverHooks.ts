import { useContext } from "react";

import type {
	FileSource,
	TableSource,
	TimeseriesSource,
} from "../types/dataSources";

import {
	type ResolvedAnnotationsData,
	type ResolvedFileData,
	type ResolvedTableData,
	type ResolvedTimeseriesData,
	ResolverContext,
} from "./resolverContextDef";

export interface UseResolvedDataOptions {
	enabled?: boolean;
}

export interface UseDocumentAnnotationsOptions {
	enabled?: boolean;
}

/**
 * Hook to access data resolvers
 */
export function useResolvers() {
	const context = useContext(ResolverContext);
	return context ?? {};
}

/**
 * Hook to resolve a file source
 * Returns a default "no resolver" state if resolver is not provided
 */
export function useResolvedFile(
	source: FileSource | undefined
): ResolvedFileData {
	const { useResolvedFile: resolver } = useResolvers();

	// If no resolver provided, try to extract direct URL
	if (!resolver) {
		if (source?.type === "direct") {
			return { url: source.url, isLoading: false, error: null };
		}
		return { url: null, isLoading: false, error: null };
	}

	return resolver(source);
}

/**
 * Hook to resolve a timeseries source
 */
export function useResolvedTimeseries(
	source: TimeseriesSource | undefined
): ResolvedTimeseriesData {
	const { useResolvedTimeseries: resolver } = useResolvers();

	if (!resolver) {
		if (source?.type === "direct") {
			return { series: source.series, isLoading: false, error: null };
		}
		return { series: [], isLoading: false, error: null };
	}

	return resolver(source);
}

/**
 * Hook to resolve a table source
 */
export function useResolvedTableData(
	source: TableSource | undefined,
	_options?: UseResolvedDataOptions
): ResolvedTableData {
	const { useResolvedTableData: resolver } = useResolvers();

	if (!resolver) {
		if (source?.type === "direct") {
			return {
				columns: source.columns,
				rows: source.rows,
				isLoading: false,
				error: null,
			};
		}
		return { columns: [], rows: [], isLoading: false, error: null };
	}

	return resolver(source);
}

/**
 * Hook to get document annotations
 */
export function useDocumentAnnotations(
	source: FileSource | undefined,
	page: number,
	options?: UseDocumentAnnotationsOptions
): ResolvedAnnotationsData {
	const { useDocumentAnnotations: resolver } = useResolvers();

	if (!resolver) {
		return { annotations: [], isLoading: false, error: null };
	}

	return resolver(source, page, options);
}

/**
 * Hook to get SDK instance (returns unknown, null if not available)
 */
export function useSDK(): unknown {
	const { useSDK: resolver } = useResolvers();
	return resolver?.() ?? null;
}

/**
 * Hook to get Cognite client (returns null if not available)
 */
export function useCogniteClientOptional(): unknown {
	const { useCogniteClient: resolver } = useResolvers();
	return resolver?.() ?? null;
}

/**
 * Hook to get Cognite client (throws if not available)
 */
export function useCogniteClient(): unknown {
	const client = useCogniteClientOptional();
	if (!client) {
		throw new Error(
			"useCogniteClient must be used within a ResolverProvider with a CogniteClient"
		);
	}
	return client;
}
