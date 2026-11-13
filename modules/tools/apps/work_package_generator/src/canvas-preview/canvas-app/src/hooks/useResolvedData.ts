/**
 * Data Resolution Hooks
 *
 * These hooks resolve flexible data sources to actual data.
 * Plugins use these to fetch data based on the source type:
 * - 'direct': Return the inline data as-is
 * - 'fdm': Fetch data from the FDM instance
 * - 'cdf': Fetch data from CDF resource ID
 * - 'classic': Return the classic model identifier as-is
 */

import type { DatapointAggregate, DoubleDatapoint } from "@cognite/sdk";
import { getSupportedMimeTypeFromUrl } from "@dune-canvas/canvas-lib";
import type { TableColumn } from "@dune-canvas/canvas-plugins/table/TableNode";
import type { TimeseriesDataPoint } from "@dune-canvas/canvas-plugins/timeseries/TimeseriesNode";
import { useQuery } from "@tanstack/react-query";

import type {
	FileSource,
	TableSource,
	TimeseriesSource,
} from "../adapters/dataSources";
import { useCdfSchedule, useCogniteClientOptional } from "../sdk/SDKProvider";
import {
	DEFAULT_URL_CACHE_EXPIRE_MS,
	resolveFileDownloadConfig,
} from "../utils/fileResolution";

// ============================================================================
// Query Keys
// ============================================================================

export const resolvedDataKeys = {
	all: ["resolved-data"] as const,
	timeseries: (source: TimeseriesSource) =>
		[...resolvedDataKeys.all, "timeseries", source] as const,
	file: (source: FileSource) =>
		[...resolvedDataKeys.all, "file", source] as const,
	table: (source: TableSource) =>
		[...resolvedDataKeys.all, "table", source] as const,
};

// ============================================================================
// Timeseries Data Resolution
// ============================================================================

export interface ResolvedTimeseriesData {
	series: TimeseriesDataPoint[];
	isLoading: boolean;
	error: Error | null;
}

// ============================================================================
// Aggregation Helpers
// ============================================================================

/** Target number of data points per chart (matches plotting-components) */
const CHART_POINTS_PER_SERIES = 2000;

/** The SDK returns Date timestamps; chart points use epoch milliseconds. */
function toEpochMs(timestamp: Date | number): number {
	return timestamp instanceof Date ? timestamp.getTime() : timestamp;
}
/** Below this raw count, fetch raw datapoints instead of aggregates */
const RAW_DATAPOINTS_THRESHOLD = 500;

/**
 * Calculate an appropriate CDF granularity string for a given time range.
 * Ported from plotting-components/src/TimeseriesChart/domain/utils/calculateGranularity.ts
 */
function calculateGranularity(
	startMs: number,
	endMs: number,
	targetPoints = CHART_POINTS_PER_SERIES
): string {
	const diffSec = (endMs - startMs) / 1000;
	const granSec = Math.ceil(diffSec / targetPoints);
	if (granSec <= 60) return `${Math.max(1, granSec)}s`;
	const granMin = Math.ceil(granSec / 60);
	if (granMin <= 60) return `${granMin}m`;
	const granHr = Math.ceil(granMin / 60);
	if (granHr <= 24) return `${granHr}h`;
	const granDay = Math.ceil(granHr / 24);
	if (granDay <= 100) return `${granDay}d`;
	return "100d";
}

/**
 * Hook to resolve timeseries data from any source type
 *
 * @example
 * // In a timeseries plugin component:
 * const { series, isLoading, error } = useResolvedTimeseries(data.source);
 * // series is always TimeseriesDataPoint[], regardless of source type
 */
export function useResolvedTimeseries(
	source: TimeseriesSource | undefined,
	options?: { enabled?: boolean; timeRange?: { start: Date; end: Date } }
): ResolvedTimeseriesData {
	const client = useCogniteClientOptional();
	const schedule = useCdfSchedule();
	const enabled = options?.enabled ?? true;

	const query = useQuery({
		queryKey: source ? resolvedDataKeys.timeseries(source) : ["disabled"],
		queryFn: async (): Promise<TimeseriesDataPoint[]> => {
			if (!source || !client) return [];

			if (source.type === "direct") {
				// Direct data - return as-is
				return source.series;
			}

			if (source.type === "fdm") {
				// FDM source - use instanceId to identify the timeseries
				const { instance } = source;

				// Check if this is a CogniteTimeSeries view
				const isCogniteTimeSeries =
					instance.viewExternalId === "CogniteTimeSeries";

				if (isCogniteTimeSeries) {
					// Get time range from source properties, options, or use defaults (last 24 hours)
					const now = new Date();
					const defaultStart = new Date(now.getTime() - 24 * 60 * 60 * 1000); // 24 hours ago

					// Priority: options > source properties > defaults
					let start: Date;
					let end: Date;

					if (options?.timeRange?.start) {
						start = options.timeRange.start;
					} else if (source.startDate) {
						start = new Date(source.startDate);
					} else {
						start = defaultStart;
					}

					if (options?.timeRange?.end) {
						end = options.timeRange.end;
					} else if (source.endDate) {
						end = new Date(source.endDate);
					} else {
						end = now;
					}

					try {
						const instanceId = {
							space: instance.space,
							externalId: instance.externalId,
						};
						const startMs = start.getTime();
						const endMs = end.getTime();
						const granularity = calculateGranularity(startMs, endMs);

						// Step 1: Fetch aggregated datapoints (average, min, max, count)
						const aggResponse = await schedule(() =>
							client.datapoints.retrieve({
								items: [
									{
										instanceId,
										aggregates: ["average", "min", "max", "count"],
										granularity,
									},
								],
								start: startMs,
								end: endMs,
								limit: CHART_POINTS_PER_SERIES,
							})
						);

						// Sum the counts to get the true total datapoint count
						let totalCount = 0;
						if (aggResponse?.[0]?.datapoints) {
							const aggPoints = aggResponse[0]
								.datapoints as DatapointAggregate[];
							for (const dp of aggPoints) {
								totalCount += dp.count ?? 0;
							}

							// Step 2: If count is small enough, fetch raw datapoints instead
							// (raw data is more precise for sparse timeseries)
							if (totalCount < RAW_DATAPOINTS_THRESHOLD) {
								const rawResponse = await schedule(() =>
									client.datapoints.retrieve({
										items: [{ instanceId }],
										start: startMs,
										end: endMs,
										limit: RAW_DATAPOINTS_THRESHOLD,
									})
								);

								if (rawResponse?.[0]?.datapoints) {
									const rawPoints = rawResponse[0]
										.datapoints as DoubleDatapoint[];
									return rawPoints.map((dp) => ({
										x: toEpochMs(dp.timestamp),
										y: dp.value ?? 0,
									}));
								}
								return [];
							}

							// Step 3: Return aggregated data with server-computed stats per bucket
							return aggPoints.map((dp) => ({
								x: toEpochMs(dp.timestamp),
								y: dp.average ?? 0,
								min: dp.min,
								max: dp.max,
								count: dp.count,
							}));
						}

						return [];
					} catch (err) {
						console.error("Failed to fetch timeseries datapoints:", err);
						throw new Error(
							`Failed to fetch timeseries data: ${(err as Error).message}`
						);
					}
				}

				// Only CogniteTimeSeries instances have datapoints to fetch
				return [];
			}

			return [];
		},
		enabled: enabled && !!client && source !== undefined,
		staleTime: 1 * 60 * 1000, // 1 minute for timeseries data
	});

	return {
		series: query.data ?? [],
		isLoading: query.isLoading,
		error: query.error as Error | null,
	};
}

// ============================================================================

export interface ResolvedFileData {
	url: string | null;
	mimeType: string | null;
	isLoading: boolean;
	error: Error | null;
}

/**
 * Hook to resolve file URL from any source type
 *
 * Uses extended expiration (59 minutes) for CDF files and supports
 * Document Preview API fallback for Office documents (Word, Excel, PowerPoint).
 */
export function useResolvedFile(
	source: FileSource | undefined,
	options?: { enabled?: boolean }
): ResolvedFileData {
	const client = useCogniteClientOptional();
	const schedule = useCdfSchedule();
	const enabled = options?.enabled ?? true;

	const query = useQuery({
		queryKey: source ? resolvedDataKeys.file(source) : ["disabled"],
		queryFn: async (): Promise<{ url: string; mimeType: string | null }> => {
			if (!source) {
				return { url: "", mimeType: null };
			}

			// Direct URL - return as-is, infer MIME type from URL if not provided
			if (source.type === "direct") {
				const mimeType =
					source.mimeType ?? getSupportedMimeTypeFromUrl(source.url) ?? null;
				return { url: source.url, mimeType };
			}

			// Client required for non-direct sources
			if (!client) {
				return { url: "", mimeType: null };
			}

			// Helper to retrieve file and resolve download config
			const resolveFileById = async (
				id: { space: string; externalId: string } | number
			): Promise<{ url: string; mimeType: string }> => {
				const fileRef = typeof id === "number" ? { id } : { instanceId: id };
				const files = await schedule(() => client.files.retrieve([fileRef]));
				if (files.length === 0) {
					const context =
						typeof id === "number" ? id : `${id.space}:${id.externalId}`;
					throw new Error(`File not found: ${context}`);
				}
				const config = await resolveFileDownloadConfig(client, files[0], {
					schedule,
				});
				return { url: config.url, mimeType: config.mimeType };
			};

			if (source.type === "cdf") {
				return resolveFileById(source.fileId);
			}

			if (source.type === "fdm") {
				const { instance } = source;

				// CogniteFile views - resolve directly via Files API
				if (instance.viewExternalId === "CogniteFile") {
					// Only pass space/externalId - Files API doesn't accept view fields
					return resolveFileById({
						space: instance.space,
						externalId: instance.externalId,
					});
				}

				// Only CogniteFile instances can be resolved through the Files API
				console.warn("Could not extract file URL from FDM instance");
				return { url: "", mimeType: null };
			}

			return { url: "", mimeType: null };
		},
		// Enable query if source exists and either it's a direct URL (no client needed) or a client is available
		enabled:
			enabled && source !== undefined && (source.type === "direct" || !!client),
		// Use extended cache time to match file URL expiration (59 minutes)
		staleTime: DEFAULT_URL_CACHE_EXPIRE_MS,
	});

	return {
		url: query.data?.url ?? null,
		mimeType: query.data?.mimeType ?? null,
		isLoading: query.isLoading,
		error: query.error as Error | null,
	};
}

// ============================================================================
// Table/Property Data Resolution
// ============================================================================

export interface ResolvedTableData {
	columns: TableColumn[];
	rows: Record<string, string | number>[];
	isLoading: boolean;
	error: Error | null;
}

/**
 * Hook to resolve table data from any source type
 *
 * For FDM sources, this fetches the actual instance from the source view
 * (e.g., CogniteActivity, CogniteAsset) and converts all properties to rows.
 */
export function useResolvedTableData(
	source: TableSource | undefined,
	options?: { enabled?: boolean }
): ResolvedTableData {
	const client = useCogniteClientOptional();
	const schedule = useCdfSchedule();
	const enabled = options?.enabled ?? true;

	const query = useQuery({
		queryKey: source ? resolvedDataKeys.table(source) : ["disabled"],
		queryFn: async (): Promise<{
			columns: TableColumn[];
			rows: Record<string, string | number>[];
		}> => {
			if (!source) {
				return { columns: [], rows: [] };
			}

			if (source.type === "direct") {
				// Direct data - return as-is
				return { columns: source.columns, rows: source.rows };
			}

			// Client required for non-direct sources
			if (!client) {
				return { columns: [], rows: [] };
			}

			if (source.type === "fdm") {
				// FDM source - fetch the actual instance from the source view
				const { instance } = source;

				// Use the view info to fetch the actual instance
				const viewSpace = instance.viewSpace || instance.space;
				const viewExternalId = instance.viewExternalId;
				const viewVersion = instance.viewVersion || "v1";

				if (!viewExternalId) {
					throw new Error("View externalId is required for FDM table source");
				}

				try {
					// Fetch the actual instance from CDF using the view
					const response = await schedule(() =>
						client.instances.retrieve({
							items: [
								{
									instanceType: "node" as const,
									space: instance.space,
									externalId: instance.externalId,
								},
							],
							sources: [
								{
									source: {
										type: "view" as const,
										space: viewSpace,
										externalId: viewExternalId,
										version: viewVersion,
									},
								},
							],
						})
					);

					if (!response.items || response.items.length === 0) {
						throw new Error(
							`FDM instance not found: ${instance.space}:${instance.externalId}`
						);
					}

					const fdmInstance = response.items[0];

					// Extract properties from the instance
					// Properties are nested under: properties[viewSpace][viewExternalId/viewVersion]
					const viewKey = `${viewExternalId}/${viewVersion}`;
					const instanceProps = fdmInstance.properties?.[viewSpace]?.[
						viewKey
					] as Record<string, unknown> | undefined;

					if (!instanceProps) {
						// Try without version
						const altViewKey = viewExternalId;
						const altProps = fdmInstance.properties?.[viewSpace]?.[
							altViewKey
						] as Record<string, unknown> | undefined;
						if (!altProps) {
							console.warn("No properties found for view:", viewSpace, viewKey);
							return { columns: [], rows: [] };
						}
					}

					const props =
						instanceProps ||
						(fdmInstance.properties?.[viewSpace]?.[viewExternalId] as Record<
							string,
							unknown
						>) ||
						{};

					// Convert properties to table rows
					const columns: TableColumn[] = [
						{ key: "property", label: "Property", width: 180 },
						{ key: "value", label: "Value" },
					];

					const rows: Record<string, string | number>[] = [];

					// Add standard instance info first
					rows.push({ property: "External ID", value: instance.externalId });
					rows.push({ property: "Space", value: instance.space });

					// Add all properties from the actual instance
					for (const [key, value] of Object.entries(props)) {
						// Skip internal/layout properties
						if (
							[
								"zIndex",
								"unscaledWidth",
								"unscaledHeight",
								"externalId",
								"space",
							].includes(key)
						) {
							continue;
						}

						// Format the value for display
						let displayValue: string | number;
						if (value === null || value === undefined) {
							displayValue = "—";
						} else if (typeof value === "object") {
							// Handle nested objects (like direct relations)
							if (
								"space" in (value as object) &&
								"externalId" in (value as object)
							) {
								const ref = value as { space: string; externalId: string };
								displayValue = `${ref.space}:${ref.externalId}`;
							} else if (Array.isArray(value)) {
								displayValue = `[${value.length} items]`;
							} else {
								displayValue = JSON.stringify(value);
							}
						} else if (typeof value === "boolean") {
							displayValue = value ? "Yes" : "No";
						} else {
							displayValue = value as string | number;
						}

						// Format the key for display (camelCase to Title Case)
						const displayKey = key
							.replace(/([A-Z])/g, " $1")
							.replace(/^./, (str) => str.toUpperCase())
							.trim();

						rows.push({ property: displayKey, value: displayValue });
					}

					return { columns, rows };
				} catch (err) {
					console.error("Failed to fetch FDM instance:", err);
					throw new Error(
						`Failed to fetch instance data: ${(err as Error).message}`
					);
				}
			}

			return { columns: [], rows: [] };
		},
		// Enable query if source exists and either it's direct data (no client needed) or a client is available
		enabled:
			enabled && source !== undefined && (source.type === "direct" || !!client),
		staleTime: 5 * 60 * 1000, // 5 minutes
	});

	return {
		columns: query.data?.columns ?? [],
		rows: query.data?.rows ?? [],
		isLoading: query.isLoading,
		error: query.error as Error | null,
	};
}
