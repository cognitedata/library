/**
 * Flexible Data Source Types
 *
 * Shared type definitions for data sources across canvas packages.
 * These types are used by both canvas-app (for resolution) and canvas-plugins (for rendering).
 *
 * Each plugin can receive data from multiple sources:
 * - 'direct': Data is provided inline (e.g., for mock data or pre-fetched data)
 * - 'fdm': Data is referenced via FDM instance (space + externalId + view)
 * - 'cdf': Data is referenced via CDF resource ID
 */

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
 * A single data point in a timeseries.
 * When data is fetched in aggregate mode, min/max/count carry
 * the server-computed values for that aggregation bucket.
 */
export interface TimeseriesDataPoint {
	x: number; // timestamp
	y: number; // value (raw) or average (aggregated)
	/** Bucket minimum — present when fetched with aggregation */
	min?: number;
	/** Bucket maximum — present when fetched with aggregation */
	max?: number;
	/** Raw datapoint count in the bucket — present when fetched with aggregation */
	count?: number;
}

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
 */
export interface TimeseriesFdmSource {
	type: "fdm";
	/** FDM instance reference */
	instance: FdmReference;
	/** Property path to the timeseries ID */
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
	/** Property path to the file ID */
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
// Table Data Sources
// ============================================================================

/**
 * Table column definition
 */
export interface TableColumn {
	key: string;
	label: string;
	width?: number;
}

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
 */
export interface TableFdmSource {
	type: "fdm";
	/** FDM instance reference with view info */
	instance: FdmReference;
}

export type TableSource = TableDirectSource | TableFdmSource;

// ============================================================================
// Generic Data Sources
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
// Document Annotations
// ============================================================================

/**
 * Resource types that can be linked in document annotations
 */
export type AnnotationResourceType =
	| "asset"
	| "file"
	| "timeSeries"
	| "sequence"
	| "event"
	| "diagram"
	| "unknown";

/**
 * A single document annotation with normalized coordinates (0-1 range)
 */
export interface DocumentAnnotation {
	id: string;
	resourceType: AnnotationResourceType;
	/** Normalized x coordinate (0-1) */
	x: number;
	/** Normalized y coordinate (0-1) */
	y: number;
	/** Normalized width (0-1) */
	width: number;
	/** Normalized height (0-1) */
	height: number;
	/** Page number (1-indexed) */
	page: number;
	/** Optional text label */
	text?: string;
	/** Optional label */
	label?: string;
	/** Resource ID if linked */
	linkedResourceId?: number;
	/** Resource external ID if linked */
	linkedResourceExternalId?: string;
}

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
