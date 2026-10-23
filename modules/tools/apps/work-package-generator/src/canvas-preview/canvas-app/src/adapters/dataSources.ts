/**
 * Flexible Data Source Types
 *
 * Each plugin can receive data from multiple sources:
 * - 'direct': Data is provided inline (e.g., for mock data or pre-fetched data)
 * - 'fdm': Data is referenced via FDM instance (space + externalId + view)
 *
 * This allows the same core rendering to be used regardless of data source.
 */

import type { TableColumn } from "@dune-canvas/canvas-plugins/table/TableNode";
import type { TimeseriesDataPoint } from "@dune-canvas/canvas-plugins/timeseries/TimeseriesNode";

// ============================================================================
// Base FDM Reference
// ============================================================================

/**
 * Reference to an FDM instance
 */
export interface FdmReference {
	/** Instance space */
	space: string;
	/** Instance external ID */
	externalId: string;
	/** View space (optional - for typed access) */
	viewSpace?: string;
	/** View external ID (optional) */
	viewExternalId?: string;
	/** View version (optional) */
	viewVersion?: string;
}

// ============================================================================
// Timeseries Data Sources
// ============================================================================

/**
 * Direct timeseries data - series is provided inline
 */
export interface TimeseriesDirectSource {
	type: "direct";
	/** The actual data points */
	series: TimeseriesDataPoint[];
}

/**
 * FDM-referenced timeseries - data fetched via FDM instance
 * The instance should have properties that map to a CDF timeseries
 */
export interface TimeseriesFdmSource {
	type: "fdm";
	/** FDM instance reference */
	instance: FdmReference;
	/** Property path to the timeseries ID (if the FDM instance wraps a CDF timeseries) */
	timeseriesIdProperty?: string;
	/** Start date for data fetching */
	startDate?: string;
	/** End date for data fetching */
	endDate?: string;
}

export type TimeseriesSource = TimeseriesDirectSource | TimeseriesFdmSource;

/**
 * Flexible timeseries node data
 */
export interface FlexibleTimeseriesData {
	/** Data source configuration */
	source: TimeseriesSource;
	/** Display label */
	label?: string;
	/** Line color */
	color?: string;
	/** Y-axis minimum */
	minY?: number;
	/** Y-axis maximum */
	maxY?: number;
}

// ============================================================================
// File/Document Data Sources
// ============================================================================

/**
 * Direct URL source - file URL is provided inline
 */
export interface FileDirectSource {
	type: "direct";
	/** File URL */
	url: string;
	/** MIME type */
	mimeType?: string;
}

/**
 * CDF File ID source
 */
export interface FileCdfSource {
	type: "cdf";
	/** CDF File ID */
	fileId: number;
}

/**
 * FDM-referenced file
 */
export interface FileFdmSource {
	type: "fdm";
	/** FDM instance reference */
	instance: FdmReference;
	/** Property path to the file ID (if instance wraps a CDF file) */
	fileIdProperty?: string;
}

export type FileSource = FileDirectSource | FileCdfSource | FileFdmSource;

/**
 * Flexible file/document node data
 */
export interface FlexibleFileData {
	/** File source configuration */
	source: FileSource;
	/** Display label */
	label?: string;
	/** Current page (for PDFs) */
	currentPage?: number;
}

// ============================================================================
// Table/Property Data Sources
// ============================================================================

/**
 * Direct table data - rows/columns provided inline
 */
export interface TableDirectSource {
	type: "direct";
	/** Column definitions */
	columns: TableColumn[];
	/** Row data */
	rows: Record<string, string | number>[];
}

/**
 * FDM instance source - fetch all properties from the actual FDM instance
 * Uses the view info to retrieve the real instance data (not just the container reference)
 */
export interface TableFdmSource {
	type: "fdm";
	/** FDM instance reference with view info */
	instance: FdmReference;
}

export type TableSource = TableDirectSource | TableFdmSource;

// ============================================================================
// Generic/Custom Data Sources
// ============================================================================

/**
 * Direct data source - data provided inline
 */
export interface GenericDirectSource {
	type: "direct";
	/** Arbitrary data payload */
	data: Record<string, unknown>;
}

/**
 * FDM instance source - fetch properties from instance
 */
export interface GenericFdmSource {
	type: "fdm";
	/** FDM instance reference */
	instance: FdmReference;
}

export type GenericSource = GenericDirectSource | GenericFdmSource;

// ============================================================================
// Type Guards
// ============================================================================

export function isDirectSource<T extends { type: string }>(
	source: T
): source is T & { type: "direct" } {
	return source.type === "direct";
}

export function isFdmSource<T extends { type: string }>(
	source: T
): source is T & { type: "fdm" } {
	return source.type === "fdm";
}

export function isCdfSource<T extends { type: string }>(
	source: T
): source is T & { type: "cdf" } {
	return source.type === "cdf";
}

// ============================================================================
// Union of all flexible data types
// ============================================================================
