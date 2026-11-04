/**
 * Asset- and activity-linked resource fetches.
 */

import type { CogniteClient } from "@cognite/sdk";
import {
	type AppConfig,
	resolveAppConfig,
	viewIdToSource,
} from "../../app-config";
import type { ResourceInstance } from "../../types";
import { fetchPagesWithItemCap } from "../../utils/dms-pagination";
import {
	itemsToResourceInstances,
	resolveResourceConfig,
} from "../resource-instances";
import { resolveApiRuntime } from "../runtime-deps";
import { listInstanceNodesWithCap } from "./list-nodes";

/**
 * Fetch resources of a specific type linked to an asset.
 * Filters by the asset reference in the resource's `assets` (or singular) property.
 * Paginates automatically until all results are collected.
 */
export async function getAssetResourcesGeneric(
	client: CogniteClient,
	assetExternalId: string,
	assetSpace: string,
	resourceType: string,
	appConfig?: AppConfig
): Promise<ResourceInstance[]> {
	const resourceConfig = resolveResourceConfig(resourceType, appConfig);
	if (!resourceConfig) return [];

	const { viewId, assetRelationProperty, assetRelationIsArray } =
		resourceConfig;
	const propPath = [
		viewId.space,
		`${viewId.externalId}/${viewId.version}`,
		assetRelationProperty,
	];
	const assetRef = { space: assetSpace, externalId: assetExternalId };

	const filter = assetRelationIsArray
		? { containsAny: { property: propPath, values: [assetRef] } }
		: { equals: { property: propPath, value: assetRef } };

	try {
		const rawItems = await listInstanceNodesWithCap(client, {
			sources: [viewIdToSource(viewId)],
			filter,
			maxTotalItems:
				resolveAppConfig(appConfig).limits.maxResourceInstancesTotal,
			warnContext: `getAssetResourcesGeneric:${resourceType}`,
			config: appConfig,
		});

		return itemsToResourceInstances(rawItems, viewId);
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.getAssetResourcesGeneric",
			message: "Failed to fetch asset-linked resources",
			cause: error,
			context: { resourceType },
		});
		return [];
	}
}

/**
 * Fetch resources of a specific type linked to a work order (activity) via a
 * graph-traversal query. Traverses outwards through the CogniteActivity
 * container property (e.g. "timeSeries") and paginates until exhausted.
 */
export async function getActivityResourcesGeneric(
	client: CogniteClient,
	activityExternalId: string,
	activitySpace: string,
	resourceType: string,
	appConfig?: AppConfig
): Promise<ResourceInstance[]> {
	const resourceConfig = resolveResourceConfig(resourceType, appConfig);
	if (!resourceConfig?.activityContainerIdentifier) return [];

	const { viewId, activityContainerIdentifier } = resourceConfig;
	const activityContainer =
		resolveAppConfig(appConfig).dataModel.activityContainer;

	const viewSource = {
		type: "view" as const,
		space: viewId.space,
		externalId: viewId.externalId,
		version: viewId.version,
	};

	try {
		const { items: rawItems } = await fetchPagesWithItemCap({
			maxTotalItems:
				resolveAppConfig(appConfig).limits.maxResourceInstancesTotal,
			warnContext: `getActivityResourcesGeneric:${resourceType}`,
			fetchPage: async (cursor) => {
				const result = await resolveApiRuntime().schedule(() =>
					client.instances.query({
						with: {
							startNode: {
								nodes: {
									filter: {
										and: [
											{
												equals: {
													property: ["node", "externalId"],
													value: activityExternalId,
												},
											},
											{
												equals: {
													property: ["node", "space"],
													value: activitySpace,
												},
											},
										],
									},
								},
							},
							direct: {
								limit: resolveAppConfig(appConfig).limits.instancePageSize,
								nodes: {
									from: "startNode",
									filter: { hasData: [viewSource] },
									through: {
										source: {
											type: "container",
											space: activityContainer.space,
											externalId: activityContainer.externalId,
										},
										identifier: activityContainerIdentifier,
									},
									direction: "outwards",
								},
							},
						},
						select: {
							direct: { sources: [{ source: viewSource, properties: ["*"] }] },
						},
						cursors: cursor ? { direct: cursor } : {},
					})
				);

				return {
					items: result?.items?.direct ?? [],
					nextCursor: result?.nextCursor?.direct,
				};
			},
		});

		return itemsToResourceInstances(rawItems, viewId);
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.getActivityResourcesGeneric",
			message: "Failed to fetch activity-linked resources",
			cause: error,
			context: { resourceType },
		});
		return [];
	}
}
