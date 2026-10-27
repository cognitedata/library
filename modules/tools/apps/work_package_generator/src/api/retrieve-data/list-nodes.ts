/**
 * Shared DMS `instances.query` pagination for retrieve-data modules.
 */

import type { CogniteClient, ViewReference } from "@cognite/sdk";
import { type AppConfig, resolveAppConfig } from "../../app-config";
import { fetchPagesWithItemCap } from "../../utils/dms-pagination";
import { resolveApiRuntime } from "../runtime-deps";

type QueryRequest = Parameters<CogniteClient["instances"]["query"]>[0];
export type QueryNodeSource = { source: ViewReference };

/**
 * `instances.list({ sources })` only returns nodes that have data in those views.
 * `instances.query` select.sources only projects properties — it does not restrict
 * the node set. Combine a `hasData` clause so listing stays view-scoped.
 */
export function combineQueryNodeFilter(
	sources: QueryNodeSource[],
	extraFilter?: unknown
): unknown | undefined {
	const views = sources.map((item) => item.source).filter(Boolean);
	const hasDataFilter = views.length > 0 ? { hasData: views } : undefined;
	if (hasDataFilter && extraFilter) {
		return { and: [hasDataFilter, extraFilter] };
	}
	return hasDataFilter ?? extraFilter;
}

/**
 * Fetch one page of nodes via `instances.query`.
 */
export async function queryInstanceNodesPage(
	client: CogniteClient,
	options: {
		sources?: QueryNodeSource[];
		filter?: unknown;
		sort?: unknown[];
		limit: number;
		cursor?: string;
	}
): Promise<{ items: unknown[]; nextCursor?: string }> {
	const sourceList = options.sources ?? [];
	const selectNodes =
		sourceList.length === 0
			? {}
			: {
					sources: sourceList.map((source) => ({
						source: source.source,
						properties: ["*"],
					})),
				};
	const nodeFilter = combineQueryNodeFilter(sourceList, options.filter);

	const query: QueryRequest = {
		with: {
			nodes: {
				limit: options.limit,
				nodes: {
					...(nodeFilter ? { filter: nodeFilter as never } : {}),
				},
				...(options.sort && options.sort.length > 0
					? { sort: options.sort as never }
					: {}),
			},
		},
		select: { nodes: selectNodes as never },
		cursors: options.cursor ? { nodes: options.cursor } : {},
	};

	const response = await resolveApiRuntime().schedule(() =>
		client.instances.query(query)
	);

	return {
		items: response.items?.nodes ?? [],
		nextCursor: response.nextCursor?.nodes,
	};
}

/**
 * `instances/byids` has no `ignoreUnknownIds`. A missing node is a 400 whose
 * body lists `missing`. Treat that as an empty result; other failures throw.
 */
function isMissingInstancesError(error: unknown): boolean {
	if (!error || typeof error !== "object") return false;
	const candidate = error as { status?: number; missing?: unknown };
	return candidate.status === 400 && Array.isArray(candidate.missing);
}

/**
 * Fetch known nodes by space and external id.
 * Missing ids return an empty list. Other retrieve failures propagate.
 */
export async function retrieveInstanceNodes(
	client: CogniteClient,
	items: Array<{ space: string; externalId: string }>,
	sources: QueryNodeSource[]
): Promise<unknown[]> {
	if (items.length === 0) return [];
	const request = {
		items: items.map((item) => ({
			instanceType: "node" as const,
			space: item.space,
			externalId: item.externalId,
		})),
		sources,
	};
	try {
		const response = await resolveApiRuntime().schedule(() =>
			client.instances.retrieve(
				request as Parameters<CogniteClient["instances"]["retrieve"]>[0]
			)
		);
		return response.items ?? [];
	} catch (error) {
		if (isMissingInstancesError(error)) return [];
		throw error;
	}
}

/**
 * Paginate `instances.query` (nodes) until exhausted or `maxTotalItems` is reached.
 */
export async function listInstanceNodesWithCap(
	client: CogniteClient,
	options: {
		sources: QueryNodeSource[];
		filter?: unknown;
		maxTotalItems: number;
		warnContext: string;
		pageSize?: number;
		config?: AppConfig;
	}
): Promise<unknown[]> {
	const { items } = await fetchPagesWithItemCap({
		maxTotalItems: options.maxTotalItems,
		warnContext: options.warnContext,
		fetchPage: (cursor) =>
			queryInstanceNodesPage(client, {
				sources: options.sources,
				filter: options.filter,
				limit:
					options.pageSize ??
					resolveAppConfig(options.config).limits.instancePageSize,
				cursor,
			}),
	});
	return items;
}
