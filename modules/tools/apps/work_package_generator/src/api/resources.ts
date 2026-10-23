/**
 * Resource API Operations
 * Fetch-by-ref and name-search helpers used by the add-resource modal and context seeding.
 * Asset/activity-linked resource listing lives in `retrieve-data/resources.ts`.
 */

import type { CogniteClient } from "@cognite/sdk";
import {
	type AppConfig,
	resolveAppConfig,
	viewIdToSource,
} from "../app-config";
import type { ResourceInstance } from "../types";
import { chunk } from "../utils/chunk";
import {
	itemsToResourceInstances,
	resolveResourceConfig,
} from "./resource-instances";
import { retrieveInstanceNodes } from "./retrieve-data/list-nodes";
import { resolveApiRuntime } from "./runtime-deps";

// ============================================================================
// Resource Functions
// ============================================================================

/**
 * Fetch resource instances of a specific type by direct node references.
 * Used to seed context entities (work order, resolved asset/equipment).
 */
export async function getResourceInstancesByRefs(
	client: CogniteClient,
	resourceType: string,
	refs: Array<{ externalId: string; space: string }>,
	appConfig?: AppConfig
): Promise<ResourceInstance[]> {
	const resourceConfig = resolveResourceConfig(resourceType, appConfig);
	if (!resourceConfig || refs.length === 0) return [];

	const { viewId } = resourceConfig;
	const uniqueRefs = refs.filter((ref) => ref.externalId && ref.space);
	if (uniqueRefs.length === 0) return [];

	try {
		const allItems: ResourceInstance[] = [];
		const batchSize = resolveAppConfig(appConfig).limits.instancePageSize;

		for (const batch of chunk(uniqueRefs, batchSize)) {
			const items = await retrieveInstanceNodes(client, batch, [
				viewIdToSource(viewId),
			]);
			allItems.push(...itemsToResourceInstances(items, viewId));
		}

		return allItems;
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "resources.getResourceInstancesByRefs",
			message: "Failed to fetch resources by refs",
			cause: error,
			context: { resourceType, refCount: uniqueRefs.length },
		});
		return [];
	}
}

/**
 * Search resources of a specific type by free-text query (name, description, etc.).
 */
export async function searchResourcesByName(
	client: CogniteClient,
	query: string,
	resourceType: string,
	appConfig?: AppConfig
): Promise<ResourceInstance[]> {
	const trimmed = query.trim();
	if (!trimmed) return [];

	const resourceConfig = resolveResourceConfig(resourceType, appConfig);
	if (!resourceConfig) return [];

	const { viewId, properties } = resourceConfig;

	try {
		const searchResponse = await resolveApiRuntime().schedule(() =>
			client.instances.search({
				instanceType: "node",
				view: {
					type: "view",
					space: viewId.space,
					externalId: viewId.externalId,
					version: viewId.version,
				},
				query: trimmed,
				properties,
				limit: resolveAppConfig(appConfig).limits.searchResultLimit,
				operator: "OR",
			})
		);

		return itemsToResourceInstances(searchResponse.items ?? [], viewId);
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "resources.searchResourcesByName",
			message: "Failed to search resources by name",
			cause: error,
			context: { resourceType, queryLen: trimmed.length },
		});
		return [];
	}
}
