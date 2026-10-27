/**
 * Work order search, list, and asset resolution.
 */

import type { CogniteClient } from "@cognite/sdk";
import {
	type AppConfig,
	dataModelView,
	resolveAppConfig,
	viewIdToSource,
} from "../../app-config";
import type { Asset, WorkOrder, WorkOrderFilters } from "../../types";
import {
	extractInstanceProperties,
	getArrayProp,
	getOptionalStringProp,
	toCdfNode,
} from "../../utils/cdf-helpers";
import { readOptionalNextCursor } from "../../utils/dms-pagination";
import { resolveApiRuntime } from "../runtime-deps";
import { queryInstanceNodesPage, retrieveInstanceNodes } from "./list-nodes";

export type PaginatedWorkOrders = {
	items: WorkOrder[];
	nextCursor?: string;
};

function mapNodeToWorkOrder(
	item: unknown,
	config?: AppConfig
): WorkOrder | null {
	const node = toCdfNode(item);
	if (!node) return null;
	const props = extractInstanceProperties(
		node,
		dataModelView("activity", config)
	);
	const fields = resolveAppConfig(config).workOrderProperties;
	return {
		externalId: node.externalId,
		space: node.space,
		name: getOptionalStringProp(props, fields.name),
		description: getOptionalStringProp(props, fields.description),
		type: getOptionalStringProp(props, fields.type),
		status: getOptionalStringProp(props, fields.status),
		priority: getOptionalStringProp(props, fields.priority),
		sourceId: getOptionalStringProp(props, fields.sourceId),
		scheduledStartTime: getOptionalStringProp(props, fields.scheduledStartTime),
		scheduledEndTime: getOptionalStringProp(props, fields.scheduledEndTime),
		workOrderNumber: getOptionalStringProp(props, fields.workOrderNumber),
		assets: getArrayProp<{ space: string; externalId: string }>(
			props,
			fields.assets
		),
	};
}

/**
 * Search work orders by query (external ID, name, or description).
 * Pass `cursor` to fetch subsequent pages; the response includes `nextCursor` when more results exist.
 */
export async function searchWorkOrders(
	client: CogniteClient,
	query: string,
	cursor?: string,
	config?: AppConfig
): Promise<PaginatedWorkOrders> {
	if (!query.trim()) return { items: [] };
	try {
		const trimmed = query.trim();

		// Only attempt exact-ID match on the first page (no cursor)
		if (!cursor) {
			const exactMatchResponse = await queryInstanceNodesPage(client, {
				sources: [viewIdToSource(dataModelView("activity", config))],
				filter: {
					equals: {
						property: ["node", "externalId"],
						value: trimmed,
					},
				},
				limit: 1,
			});

			const exactItems = exactMatchResponse.items ?? [];
			if (exactItems.length > 0) {
				const wo = mapNodeToWorkOrder(exactItems[0], config);
				return { items: wo ? [wo] : [] };
			}
		}

		const activityView = dataModelView("activity", config);
		const fields = resolveAppConfig(config).workOrderProperties;
		// SearchRequestV3 includes `operator` but not the request cursor the API accepts.
		const searchRequest: Parameters<typeof client.instances.search>[0] & {
			cursor?: string;
		} = {
			instanceType: "node",
			view: {
				type: "view",
				space: activityView.space,
				externalId: activityView.externalId,
				version: activityView.version,
			},
			query: trimmed,
			properties: [fields.name, fields.description],
			limit: resolveAppConfig(config).limits.searchResultLimit,
			operator: "OR",
			...(cursor ? { cursor } : {}),
		};
		const searchResponse = await resolveApiRuntime().schedule(() =>
			client.instances.search(searchRequest)
		);

		const out: WorkOrder[] = [];
		for (const item of searchResponse.items ?? []) {
			const wo = mapNodeToWorkOrder(item, config);
			if (wo) out.push(wo);
		}
		return {
			items: out,
			nextCursor: readOptionalNextCursor(searchResponse),
		};
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.searchWorkOrders",
			message: "Failed to search work orders",
			cause: error,
			context: { queryLen: query.trim().length },
		});
		throw error;
	}
}

/**
 * List the most recently scheduled work orders, with optional filtering by date range,
 * status, and priority. Sorted by scheduledStartTime descending server-side.
 * Pass `cursor` to fetch subsequent pages; the response includes `nextCursor` when more exist.
 */
export async function listRecentWorkOrders(
	client: CogniteClient,
	filters?: WorkOrderFilters,
	cursor?: string,
	config?: AppConfig
): Promise<PaginatedWorkOrders> {
	const appConfig = resolveAppConfig(config);
	const fields = appConfig.workOrderProperties;
	const viewPropPrefix = [
		dataModelView("activity", config).space,
		`${dataModelView("activity", config).externalId}/${dataModelView("activity", config).version}`,
	];

	const filterClauses: unknown[] = [];

	if (filters?.status) {
		filterClauses.push({
			equals: {
				property: [...viewPropPrefix, fields.status],
				value: filters.status,
			},
		});
	}

	if (filters?.priority) {
		filterClauses.push({
			equals: {
				property: [...viewPropPrefix, fields.priority],
				value: filters.priority,
			},
		});
	}

	if (filters?.dateFrom || filters?.dateTo) {
		const rangeFilter: Record<string, string> = {};
		if (filters.dateFrom)
			rangeFilter.gte = new Date(filters.dateFrom).toISOString();
		if (filters.dateTo) {
			const endDate = new Date(filters.dateTo);
			endDate.setHours(23, 59, 59, 999);
			rangeFilter.lte = endDate.toISOString();
		}
		filterClauses.push({
			range: {
				property: [...viewPropPrefix, fields.scheduledStartTime],
				...rangeFilter,
			},
		});
	}

	const filter =
		filterClauses.length === 0
			? undefined
			: filterClauses.length === 1
				? filterClauses[0]
				: { and: filterClauses };

	try {
		const response = await queryInstanceNodesPage(client, {
			sources: [viewIdToSource(dataModelView("activity", config))],
			filter,
			sort: [
				{
					property: [...viewPropPrefix, fields.scheduledStartTime],
					direction: "descending",
					nullsFirst: false,
				},
			],
			limit: resolveAppConfig(config).limits.searchResultLimit,
			cursor,
		});

		const out: WorkOrder[] = [];
		for (const item of response.items ?? []) {
			const wo = mapNodeToWorkOrder(item, config);
			if (wo) out.push(wo);
		}

		return { items: out, nextCursor: response.nextCursor };
	} catch (error) {
		resolveApiRuntime().logError({
			scope: "retrieve-data.listRecentWorkOrders",
			message: "Failed to list recent work orders",
			cause: error,
		});
		throw error;
	}
}

/**
 * Find a work order by external ID, description, or name (returns first match only)
 */
export async function findWorkOrder(
	client: CogniteClient,
	identifier: string,
	config?: AppConfig
): Promise<WorkOrder | null> {
	const result = await searchWorkOrders(client, identifier, undefined, config);
	return result.items.length > 0 ? result.items[0] : null;
}

async function lookupNodeById(
	client: CogniteClient,
	space: string | undefined,
	externalId: string,
	sources: ReturnType<typeof viewIdToSource>[]
): Promise<unknown[]> {
	if (space) {
		return retrieveInstanceNodes(client, [{ space, externalId }], sources);
	}
	const page = await queryInstanceNodesPage(client, {
		sources,
		filter: {
			equals: {
				property: ["node", "externalId"],
				value: externalId,
			},
		},
		limit: 1,
	});
	return page.items;
}

/**
 * Get the asset associated with a work order
 */
export async function getAssetForWorkOrder(
	client: CogniteClient,
	workOrder: WorkOrder,
	config?: AppConfig
): Promise<Asset | null> {
	if (!workOrder.externalId) return null;

	try {
		// First get the work order to find linked assets
		const woItems = await lookupNodeById(
			client,
			workOrder.space,
			workOrder.externalId,
			[viewIdToSource(dataModelView("activity", config))]
		);
		const woItem = woItems[0];
		if (!woItem) return null;

		const woNode = toCdfNode(woItem);
		if (!woNode) return null;

		const woProps = extractInstanceProperties(
			woNode,
			dataModelView("activity", config)
		);
		const assets = getArrayProp<{ externalId: string; space?: string }>(
			woProps,
			"assets"
		);

		if (assets.length === 0) return null;

		if (assets.length > 1) {
			// Multi-asset work orders are not a supported use case in this app.
			// Instead of silently falling back to the first asset, we inform the user.
			const error = new Error(
				`This work order references ${assets.length} assets, but only single-asset work orders are supported. Please use a work order that references only one asset.`
			);
			// Mark this as a user-facing error that should be re-thrown
			(error as Error & { isUserFacing?: boolean }).isUserFacing = true;
			throw error;
		}

		const assetRef = assets[0];
		const assetExt = assetRef.externalId;

		// Fetch the asset (or equipment — work order `assets` may point at an equipment node)
		const assetItems = await lookupNodeById(client, assetRef.space, assetExt, [
			viewIdToSource(dataModelView("asset", config)),
		]);

		let resolvedItem = assetItems[0];
		let propsViewId = dataModelView("asset", config);

		if (!resolvedItem) {
			const equipmentItems = await lookupNodeById(
				client,
				assetRef.space,
				assetExt,
				[viewIdToSource(dataModelView("equipment", config))]
			);
			resolvedItem = equipmentItems[0];
			propsViewId = dataModelView("equipment", config);
		}

		if (!resolvedItem) return null;

		const assetNode = toCdfNode(resolvedItem);
		if (!assetNode) return null;

		const assetProps = extractInstanceProperties(assetNode, propsViewId);

		return {
			assetExternalId: assetNode.externalId,
			space: assetNode.space,
			assetName: getOptionalStringProp(assetProps, "name"),
			description: getOptionalStringProp(assetProps, "description"),
			type: getOptionalStringProp(assetProps, "type"),
		};
	} catch (error) {
		// Re-throw user-facing errors (like multi-asset validation)
		if (
			error &&
			typeof error === "object" &&
			(error as Error & { isUserFacing?: boolean }).isUserFacing
		) {
			throw error;
		}

		// Log and swallow technical/network errors
		resolveApiRuntime().logError({
			scope: "retrieve-data.getAssetForWorkOrder",
			message: "Failed to resolve asset for work order",
			cause: error,
			context: { workOrderExternalId: workOrder.externalId },
		});
		return null;
	}
}
