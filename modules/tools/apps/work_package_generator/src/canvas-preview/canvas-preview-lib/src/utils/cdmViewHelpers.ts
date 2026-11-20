/**
 * CDM View Helpers
 *
 * Utilities for resolving FDM views and checking if they implement
 * CDF Common Data Model (CDM) view types like CogniteFile, CogniteAsset, etc.
 */

import type { CogniteClient, ViewDefinition } from "@cognite/sdk";

import { type CdfSchedule, defaultCdfSchedule } from "../../../cdfSchedule";

export type { ViewDefinition };

// ============================================================================
// Types
// ============================================================================

/**
 * Input for view resolution - version is optional to allow fetching latest
 */
export interface ViewRetrieveInput {
	space: string;
	externalId: string;
	version?: string;
}

// ============================================================================
// CDM View Constants
// ============================================================================

/** CDF Common Data Model view external IDs */
export const CDF_CDM_VIEWS = {
	FILE: "CogniteFile",
	ASSET: "CogniteAsset",
	TIMESERIES: "CogniteTimeSeries",
	ACTIVITY: "CogniteActivity",
	EQUIPMENT: "CogniteEquipment",
	FILE_CATEGORY: "CogniteFileCategory",
	THREE_D: "Cognite3DRevision",
	THREE_D_MODEL: "Cognite3DModel",
	CAD_MODEL: "CogniteCADModel",
	POINT_CLOUD_MODEL: "CognitePointCloudModel",
} as const;

export type CdmViewType = keyof typeof CDF_CDM_VIEWS;

// ============================================================================
// Cache Key
// ============================================================================

/** Create a cache key from a view reference */
export function getViewCacheKey(ref: ViewRetrieveInput): string {
	const base = `${ref.space}:${ref.externalId}`;
	return ref.version ? `${base}:${ref.version}` : base;
}

// ============================================================================
// View Resolution - Internal Helpers
// ============================================================================

/** Select the view with the most recent createdTime */
function selectLatestView(views: ViewDefinition[]): ViewDefinition {
	if (views.length === 1) return views[0];
	return views.reduce((latest, current) =>
		current.createdTime > latest.createdTime ? current : latest
	);
}

/** Group views by space:externalId */
function groupViewsByIdentifier(
	views: ViewDefinition[]
): Map<string, ViewDefinition[]> {
	const grouped = new Map<string, ViewDefinition[]>();
	for (const view of views) {
		const key = `${view.space}:${view.externalId}`;
		const existing = grouped.get(key);
		if (existing) {
			existing.push(view);
		} else {
			grouped.set(key, [view]);
		}
	}
	return grouped;
}

interface PartitionedRefs {
	cached: Map<string, ViewDefinition>;
	withVersion: ViewRetrieveInput[];
	withoutVersion: ViewRetrieveInput[];
}

/** Partition refs into cached, versioned, and unversioned groups */
function partitionViewRefs(
	viewRefs: ViewRetrieveInput[],
	viewCache: Map<string, ViewDefinition>
): PartitionedRefs {
	const cached = new Map<string, ViewDefinition>();
	const withVersionKeys = new Set<string>();
	const withoutVersionKeys = new Set<string>();
	const withVersion: ViewRetrieveInput[] = [];
	const withoutVersion: ViewRetrieveInput[] = [];

	for (const ref of viewRefs) {
		const cacheKey = getViewCacheKey(ref);
		const cachedView = viewCache.get(cacheKey);

		if (cachedView) {
			cached.set(cacheKey, cachedView);
		} else if (ref.version) {
			if (!withVersionKeys.has(cacheKey)) {
				withVersionKeys.add(cacheKey);
				withVersion.push(ref);
			}
		} else {
			const key = `${ref.space}:${ref.externalId}`;
			if (!withoutVersionKeys.has(key)) {
				withoutVersionKeys.add(key);
				withoutVersion.push(ref);
			}
		}
	}

	return { cached, withVersion, withoutVersion };
}

/** Fetch versioned views from the SDK */
async function fetchVersionedViews(
	sdk: CogniteClient,
	refs: ViewRetrieveInput[],
	schedule: CdfSchedule
): Promise<ViewDefinition[]> {
	if (refs.length === 0) return [];

	const response = await schedule(() =>
		sdk.views.retrieve(
			refs.map((ref) => ({
				space: ref.space,
				externalId: ref.externalId,
				version: ref.version!,
			}))
		)
	);
	return response.items;
}

/** Fetch unversioned views and select latest version for each */
async function fetchLatestViews(
	sdk: CogniteClient,
	refs: ViewRetrieveInput[],
	schedule: CdfSchedule
): Promise<Map<string, ViewDefinition>> {
	if (refs.length === 0) return new Map();

	const response = await schedule(() =>
		sdk.views.retrieve(
			refs.map((ref) => ({
				space: ref.space,
				externalId: ref.externalId,
			}))
		)
	);

	const result = new Map<string, ViewDefinition>();
	const grouped = groupViewsByIdentifier(response.items);

	for (const [key, views] of grouped) {
		result.set(key, selectLatestView(views));
	}

	return result;
}

// ============================================================================
// View Resolution - Public API
// ============================================================================

/**
 * Resolve multiple views in batch.
 *
 * - Views with version specified are fetched directly
 * - Views without version fetch all versions and select the latest
 * - Results are stored in the provided viewCache to avoid repeated API calls
 * - SDK requires separate requests for versioned vs unversioned (run in parallel)
 */
export async function resolveViews(
	sdk: CogniteClient,
	viewRefs: ViewRetrieveInput[],
	viewCache: Map<string, ViewDefinition>,
	schedule: CdfSchedule = defaultCdfSchedule
): Promise<Map<string, ViewDefinition>> {
	if (viewRefs.length === 0) return new Map();

	const { cached, withVersion, withoutVersion } = partitionViewRefs(
		viewRefs,
		viewCache
	);
	const result = new Map(cached);

	// Fetch versioned and unversioned in parallel
	const [versionedViews, latestViews] = await Promise.all([
		fetchVersionedViews(sdk, withVersion, schedule).catch(
			() => [] as ViewDefinition[]
		),
		fetchLatestViews(sdk, withoutVersion, schedule).catch(
			() => new Map<string, ViewDefinition>()
		),
	]);

	// Cache and add versioned views
	for (const view of versionedViews) {
		const cacheKey = getViewCacheKey(view);
		viewCache.set(cacheKey, view);
		result.set(cacheKey, view);
	}

	// Cache and add latest views
	for (const [key, view] of latestViews) {
		viewCache.set(key, view);
		result.set(key, view);
	}

	return result;
}

/**
 * Resolve a single view
 */
export async function resolveView(
	sdk: CogniteClient,
	viewRef: ViewRetrieveInput,
	viewCache: Map<string, ViewDefinition>,
	schedule: CdfSchedule = defaultCdfSchedule
): Promise<ViewDefinition | undefined> {
	const result = await resolveViews(sdk, [viewRef], viewCache, schedule);
	return result.get(getViewCacheKey(viewRef));
}

// ============================================================================
// CDM View Type Checking
// ============================================================================

/**
 * Check if a view matches or implements a specific CDM view type.
 * Checks both direct match and implements chain.
 */
function matchesCdmView(
	view: ViewDefinition | undefined,
	viewType: CdmViewType
): boolean {
	if (!view) return false;

	const targetExternalId = CDF_CDM_VIEWS[viewType];

	return (
		view.externalId === targetExternalId ||
		view.implements?.some((impl) => impl.externalId === targetExternalId) ===
			true
	);
}

// ============================================================================
// View Type Determination
// ============================================================================

/** CDM view types in order of specificity for determination */
const CDM_TYPE_PRIORITY: CdmViewType[] = [
	"TIMESERIES",
	"FILE",
	"EQUIPMENT",
	"ACTIVITY",
	"ASSET",
	"THREE_D_MODEL",
	"CAD_MODEL",
	"POINT_CLOUD_MODEL",
	"THREE_D",
	"FILE_CATEGORY",
];

/**
 * Determine the CDM view type from a resolved view definition.
 * Returns the most specific matching type, or 'UNKNOWN' if no match.
 */
export function determineCdmViewType(
	view: ViewDefinition | undefined
): CdmViewType | "UNKNOWN" {
	if (!view) return "UNKNOWN";

	for (const viewType of CDM_TYPE_PRIORITY) {
		if (matchesCdmView(view, viewType)) {
			return viewType;
		}
	}

	return "UNKNOWN";
}
