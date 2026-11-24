/**
 * Resolver Context Definition
 *
 * Defines the React context and all related types for data resolution.
 * Kept in a separate file so ResolverContext.tsx can export only components.
 */

import { createContext } from "react";

import type {
	DocumentAnnotation,
	FileSource,
	TableColumn,
	TableSource,
	TimeseriesDataPoint,
	TimeseriesSource,
} from "../types/dataSources";

// ============================================================================
// Resolution Result Types
// ============================================================================

export interface ResolvedFileData {
	url: string | null;
	isLoading: boolean;
	error: Error | null;
}

export interface ResolvedTimeseriesData {
	series: TimeseriesDataPoint[];
	isLoading: boolean;
	error: Error | null;
}

export interface ResolvedTableData {
	columns: TableColumn[];
	rows: Record<string, string | number>[];
	isLoading: boolean;
	error: Error | null;
}

export interface ResolvedAnnotationsData {
	annotations: DocumentAnnotation[];
	isLoading: boolean;
	error: Error | null;
}

// ============================================================================
// Resolver Functions Interface
// ============================================================================

export interface DataResolvers {
	/**
	 * Resolve a file source to a URL
	 */
	useResolvedFile?: (source: FileSource | undefined) => ResolvedFileData;

	/**
	 * Resolve a timeseries source to data points
	 */
	useResolvedTimeseries?: (
		source: TimeseriesSource | undefined
	) => ResolvedTimeseriesData;

	/**
	 * Resolve a table source to columns and rows
	 */
	useResolvedTableData?: (source: TableSource | undefined) => ResolvedTableData;

	/**
	 * Get document annotations for a file
	 */
	useDocumentAnnotations?: (
		source: FileSource | undefined,
		page: number,
		options?: { enabled?: boolean }
	) => ResolvedAnnotationsData;

	/**
	 * Get the SDK instance (optional)
	 */
	useSDK?: () => unknown;

	/**
	 * Get the Cognite client (optional)
	 */
	useCogniteClient?: () => unknown;
}

// ============================================================================
// Context
// ============================================================================

export const ResolverContext = createContext<DataResolvers | null>(null);
