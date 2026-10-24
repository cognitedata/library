import type {
	TimeseriesDataPoint,
	TimeseriesSource,
} from "@dune-canvas/canvas-lib";

export type { TimeseriesDataPoint } from "@dune-canvas/canvas-lib";

/**
 * Direct data format with inline series
 */
export interface TimeseriesNodeDataDirect {
	series: TimeseriesDataPoint[];
	label?: string;
	color?: string;
	minY?: number;
	maxY?: number;
	startDate?: string; // ISO string -- used to filter series client-side
	endDate?: string;
}

/**
 * FDM source reference (data resolved via useResolvedTimeseries hook)
 */
export interface TimeseriesNodeDataSource {
	source: TimeseriesSource;
	label?: string;
	color?: string;
	minY?: number;
	maxY?: number;
	startDate?: string; // ISO string -- synced to source for refetch
	endDate?: string;
}

/**
 * Combined data type supporting both direct series and flexible source
 */
export type TimeseriesNodeData =
	| TimeseriesNodeDataDirect
	| TimeseriesNodeDataSource;

/**
 * Type guard to check if data has direct series
 */
export function hasDirectSeries(
	data: TimeseriesNodeData
): data is TimeseriesNodeDataDirect {
	return (
		"series" in data && Array.isArray((data as TimeseriesNodeDataDirect).series)
	);
}

/**
 * Type guard to check if data has source for resolution
 */
export function hasSource(
	data: TimeseriesNodeData
): data is TimeseriesNodeDataSource {
	return (
		"source" in data && (data as TimeseriesNodeDataSource).source !== undefined
	);
}
