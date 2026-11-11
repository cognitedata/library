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
import {
	listInstanceNodesWithCap,
	queryInstanceNodesPage,
	retrieveInstanceNodes,
} from "./list-nodes";

export type PaginatedWorkOrders = {
	items: WorkOrder[];
	nextCursor?: string;
};

/**
 * Inclusive local-day bound for a `YYYY-MM-DD` filter.
 * `timeZone` is injectable so tests do not depend on the host time zone.
 */
export function localDayBoundIso(
	dateOnly: string,
	bound: "start" | "end",
	timeZone: string = Intl.DateTimeFormat().resolvedOptions().timeZone
): string {
	const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateOnly);
	if (!match) return new Date(dateOnly).toISOString();

	const year = Number(match[1]);
	const month = Number(match[2]);
	const day = Number(match[3]);
	const wallAsUtc = Date.UTC(
		year,
		month - 1,
		day,
		bound === "end" ? 23 : 0,
		bound === "end" ? 59 : 0,
		bound === "end" ? 59 : 0,
		bound === "end" ? 999 : 0
	);

	let utcMillis = wallAsUtc;
	for (let attempt = 0; attempt < 3; attempt++) {
		const offset = timeZoneOffsetMs(new Date(utcMillis), timeZone);
		const next = wallAsUtc - offset;
		if (next === utcMillis) break;
		utcMillis = next;
	}
	return new Date(utcMillis).toISOString();
}

function timeZoneOffsetMs(instant: Date, timeZone: string): number {
	const parts = new Intl.DateTimeFormat("en-US", {
		timeZone,
		hourCycle: "h23",
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
	}).formatToParts(instant);
	const value: Record<string, string> = {};
	for (const part of parts) {
		if (part.type !== "literal") value[part.type] = part.value;
	}
	const asUtc = Date.UTC(
		Number(value.year),
		Number(value.month) - 1,
		Number(value.day),
		Number(value.hour) % 24,
		Number(value.minute),
		Number(value.second)
	);
	return asUtc - Math.floor(instant.getTime() / 1000) * 1000;
}

const MAX_ID_CONTAINS_SCAN_ITEMS = 1000;

function externalIdPrefixFilter(value: string): unknown {
	const variants = Array.from(
		new Set([value, value.toLowerCase(), value.toUpperCase()])
	);
	const clauses = variants.map((variant) => ({
		prefix: { property: ["node", "externalId"], value: variant },
	}));
	return clauses.length === 1 ? clauses[0] : { or: clauses };
}

function externalIdMatchRank(externalId: string, needleLower: string): number {
	const lower = externalId.toLowerCase();
	if (lower === needleLower) return 0;
	if (lower.startsWith(needleLower)) return 1;
	if (lower.includes(needleLower)) return 2;
	return 3;
}

function mergeWorkOrders(
	idMatches: WorkOrder[],
	nameMatches: WorkOrder[],
	query: string,
	limit: number
): WorkOrder[] {
	const needleLower = query.toLowerCase();
	const seen = new Set<string>();
	const deduped: WorkOrder[] = [];
	for (const workOrder of [...idMatches, ...nameMatches]) {
		const key = `${workOrder.space}/${workOrder.externalId}`;
		if (seen.has(key)) continue;
		seen.add(key);
		deduped.push(workOrder);
	}
	deduped.sort((left, right) => {
		return (
			externalIdMatchRank(left.externalId, needleLower) -
			externalIdMatchRank(right.externalId, needleLower)
		);
	});
	return deduped.slice(0, limit);
}

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
		const appConfig = resolveAppConfig(config);
		const limit = appConfig.limits.searchResultLimit;
		const idMatches: WorkOrder[] = [];

		// DMS filters only support equals/prefix on externalId, not contains.
		// On the first page, combine server-side prefix matches with a bounded client-side contains scan.
		if (!cursor) {
			const activitySource = viewIdToSource(dataModelView("activity", config));
			const prefixResponse = await queryInstanceNodesPage(client, {
				sources: [activitySource],
				filter: externalIdPrefixFilter(trimmed),
				limit,
			});
			for (const item of prefixResponse.items ?? []) {
				const wo = mapNodeToWorkOrder(item, config);
				if (wo) idMatches.push(wo);
			}

			const needleLower = trimmed.toLowerCase();
			const containsScanCap = Math.min(
				appConfig.limits.instancePageSize,
				MAX_ID_CONTAINS_SCAN_ITEMS
			);
			const { items: scannedItems } = await listInstanceNodesWithCap(client, {
				sources: [activitySource],
				maxTotalItems: containsScanCap,
				warnContext: "searchWorkOrders:idContains",
				pageSize: containsScanCap,
				config,
			});
			for (const item of scannedItems) {
				const wo = mapNodeToWorkOrder(item, config);
				if (wo?.externalId.toLowerCase().includes(needleLower)) {
					idMatches.push(wo);
				}
			}
		}

		const activityView = dataModelView("activity", config);
		const fields = appConfig.workOrderProperties;
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
			limit,
			operator: "OR",
			...(cursor ? { cursor } : {}),
		};
		const searchResponse = await resolveApiRuntime().schedule(() =>
			client.instances.search(searchRequest)
		);

		const out: WorkOrder[] = [];
		for (const item of searchResponse?.items ?? []) {
			const wo = mapNodeToWorkOrder(item, config);
			if (wo) out.push(wo);
		}
		return {
			items: mergeWorkOrders(idMatches, out, trimmed, limit),
			nextCursor: searchResponse
				? readOptionalNextCursor(searchResponse)
				: undefined,
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
	config?: AppConfig,
	timeZone?: string
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
		if (filters.dateFrom) {
			rangeFilter.gte = localDayBoundIso(filters.dateFrom, "start", timeZone);
		}
		if (filters.dateTo) {
			rangeFilter.lte = localDayBoundIso(filters.dateTo, "end", timeZone);
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
		const assetProperty = resolveAppConfig(config).workOrderProperties.assets;
		const assets = getArrayProp<{ externalId: string; space?: string }>(
			woProps,
			assetProperty
		);

		if (assets.length === 0) return null;

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
		resolveApiRuntime().logError({
			scope: "retrieve-data.getAssetForWorkOrder",
			message: "Failed to resolve asset for work order",
			cause: error,
			context: { workOrderExternalId: workOrder.externalId },
		});
		throw error;
	}
}
