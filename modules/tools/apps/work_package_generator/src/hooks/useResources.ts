import type { CogniteClient } from "@cognite/sdk";
import { useQuery } from "@tanstack/react-query";
import { createContext, useContext, useEffect } from "react";
import {
	getActivityResourcesGeneric,
	getAssetResourcesGeneric,
	getResourceInstancesByRefs,
} from "../api";
import {
	type AppConfig,
	getResourceConfigs,
	useAppConfig,
} from "../app-config";
import { useDefaultWorkPackageStore } from "../contexts/WorkPackageStoreContext";
import type { Asset, ResourceInstance, WorkOrder } from "../types";
import { getRefKey, getRefKeyFromNode } from "../utils/cdf-helpers";
import { useCdfClientInfo } from "./useClientInfo";

// ─── Helpers ────────────────────────────────────────────────────────────────

/** Deduplicate resource instances by externalId. */
function dedupeResources(resources: ResourceInstance[]): ResourceInstance[] {
	const seen = new Set<string>();
	return resources.filter((r) => {
		if (seen.has(r.externalId)) return false;
		seen.add(r.externalId);
		return true;
	});
}

/**
 * Merge fetched resources with existing candidates, omitting ids the user
 * removed from the list. Manual adds that were never in the fetch stay.
 */
export function mergeFetchedCandidates(
	fetched: ResourceInstance[],
	existing: ResourceInstance[],
	removedIds: readonly string[]
): ResourceInstance[] {
	const removed = new Set(removedIds);
	const fetchedKept = fetched.filter((r) => !removed.has(r.externalId));
	const fetchedIds = new Set(fetched.map((r) => r.externalId));
	const manualOnly = existing.filter(
		(r) => !fetchedIds.has(r.externalId) && !removed.has(r.externalId)
	);
	return [...fetchedKept, ...manualOnly];
}

/**
 * Collect all unique asset refs to search resources against:
 * - The primary resolved asset
 * - Every asset ref listed on the work order (workOrder.assets[])
 */
function buildUniqueAssetRefs(
	asset: Asset | null,
	workOrder: WorkOrder | null
): Array<{ externalId: string; space: string }> {
	const seen = new Set<string>();
	const refs: Array<{ externalId: string; space: string }> = [];

	const add = (externalId: string, space: string) => {
		if (!externalId || !space) return;
		const key = getRefKey(externalId, space);
		if (!seen.has(key)) {
			seen.add(key);
			refs.push({ externalId, space });
		}
	};

	if (asset) add(asset.assetExternalId, asset.space);

	for (const ref of workOrder?.assets ?? []) {
		add(ref.externalId, ref.space ?? asset?.space ?? "");
	}

	return refs;
}

/** Build a stable, sorted cache key from a list of node refs. */
function assetRefsKey(
	refs: Array<{ externalId: string; space: string }>
): string {
	return refs.map(getRefKeyFromNode).sort().join(",");
}

/** Context entity types seeded from the selected work order and asset refs. */
const CONTEXT_ENTITY_TYPES = new Set(["activity", "asset", "equipment"]);

const defaultAssetResourcesDeps = {
	getActivityResourcesGeneric: (
		...args: Parameters<typeof getActivityResourcesGeneric>
	) => getActivityResourcesGeneric(...args),
	getAssetResourcesGeneric: (
		...args: Parameters<typeof getAssetResourcesGeneric>
	) => getAssetResourcesGeneric(...args),
	getResourceInstancesByRefs: (
		...args: Parameters<typeof getResourceInstancesByRefs>
	) => getResourceInstancesByRefs(...args),
	useWorkPackageStore: useDefaultWorkPackageStore,
	useCdfClientInfo,
};

export type UseAssetResourcesContextType = typeof defaultAssetResourcesDeps;
export const UseAssetResourcesContext =
	createContext<UseAssetResourcesContextType>(defaultAssetResourcesDeps);

async function fetchContextEntities(
	client: CogniteClient,
	typeKey: string,
	workOrder: WorkOrder | null,
	assetRefs: Array<{ externalId: string; space: string }>,
	getByRefs: typeof getResourceInstancesByRefs,
	appConfig: AppConfig
): Promise<ResourceInstance[]> {
	if (!CONTEXT_ENTITY_TYPES.has(typeKey)) return [];

	if (typeKey === "activity" && workOrder) {
		return getByRefs(
			client,
			typeKey,
			[{ externalId: workOrder.externalId, space: workOrder.space }],
			appConfig
		);
	}

	if (typeKey === "asset" || typeKey === "equipment") {
		return getByRefs(client, typeKey, assetRefs, appConfig);
	}

	return [];
}

// ─── Hook ───────────────────────────────────────────────────────────────────

export function useAssetResources() {
	const {
		getActivityResourcesGeneric: getActivityResources,
		getAssetResourcesGeneric: getAssetResources,
		getResourceInstancesByRefs: getByRefs,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseAssetResourcesContext);
	const { client } = useCdfClient();
	const {
		asset,
		workOrder,
		setCandidatesGeneric,
		setLoading,
		candidatesByType,
	} = useStore();

	const allAssetRefs = buildUniqueAssetRefs(asset, workOrder);
	const appConfig = useAppConfig();
	const resourceConfigs = getResourceConfigs(appConfig);

	const query = useQuery({
		queryKey: [
			"assetResources",
			assetRefsKey(allAssetRefs),
			workOrder?.externalId,
		],
		queryFn: async () => {
			if (!client || allAssetRefs.length === 0)
				throw new Error("Missing CDF client or asset refs");

			// Parallel fetch across all resource types
			const resourceTypePromises = Object.entries(resourceConfigs).map(
				async ([typeKey]) => {
					// Parallel fetch across all asset refs for this resource type
					const assetResults = (
						await Promise.all(
							allAssetRefs.map((ref) =>
								getAssetResources(
									client,
									ref.externalId,
									ref.space,
									typeKey,
									appConfig
								)
							)
						)
					).flat();

					// Graph-traversal fetch from the work order (activity) node
					const activityResults = workOrder
						? await getActivityResources(
								client,
								workOrder.externalId,
								workOrder.space,
								typeKey,
								appConfig
							)
						: [];

					const contextResults = await fetchContextEntities(
						client,
						typeKey,
						workOrder,
						allAssetRefs,
						getByRefs,
						appConfig
					);

					const merged = dedupeResources([
						...assetResults,
						...activityResults,
						...contextResults,
					]);
					return [typeKey, merged] as const;
				}
			);

			const resourceTypeResults = await Promise.all(resourceTypePromises);

			// Build results object from parallel results
			const results: Record<string, ResourceInstance[]> = {};
			for (const [typeKey, resources] of resourceTypeResults) {
				results[typeKey] = resources;
			}

			return results;
		},
		enabled: !!(client && allAssetRefs.length > 0),
		staleTime: 0,
		gcTime: 0,
	});

	// Handle loading state and store updates via useEffect to avoid side effects in queryFn
	useEffect(() => {
		if (query.isFetching && allAssetRefs.length > 0) {
			const plural = allAssetRefs.length !== 1 ? "s" : "";
			setLoading(
				true,
				`Fetching resources for ${allAssetRefs.length} asset${plural} and work order...`
			);
		} else if (!query.isFetching) {
			setLoading(false);
		}
	}, [query.isFetching, allAssetRefs.length, setLoading]);

	// Update store with fetched data, preserving manually added candidates
	// and omitting resources the user removed from the list.
	useEffect(() => {
		if (query.data) {
			const state = useStore.getState();
			for (const [typeKey, fetched] of Object.entries(query.data)) {
				const existing = state.candidatesByType[typeKey] ?? [];
				const removedIds = state.removedCandidateIdsByType?.[typeKey] ?? [];
				setCandidatesGeneric(
					typeKey,
					mergeFetchedCandidates(fetched, existing, removedIds)
				);
			}
		}
	}, [query.data, setCandidatesGeneric, useStore]);

	return {
		resources: candidatesByType,
		isLoading: query.isLoading || query.isFetching,
		error: query.error,
		refetch: query.refetch,
	};
}
