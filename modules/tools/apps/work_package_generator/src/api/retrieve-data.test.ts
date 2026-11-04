/**
 * Tests for retrieve-data.ts — the CDF data retrieval layer.
 *
 * CDF calls run through the inline API runtime. The CogniteClient instances
 * API is a test double.
 *
 * Property structure for CDF nodes: { [space]: { [externalId/version]: { [prop]: value } } }
 */
import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ACTIVITY_VIEW_ID, ASSET_VIEW_ID } from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import {
	findCanvasesByName,
	findWorkOrder,
	getActivityResourcesGeneric,
	getAssetForWorkOrder,
	getAssetResourcesGeneric,
	listRecentWorkOrders,
	listTemplateCanvases,
	searchWorkOrders,
} from "./retrieve-data";

installInlineApiRuntime();

// ─── helpers ────────────────────────────────────────────────────────────────

// Properties format expected by extractInstanceProperties:
// { [space]: { [externalId/version]: { [propName]: value } } }
const ACTIVITY_VIEW_KEY = "cdf_cdm";
const ACTIVITY_PROP_KEY = "CogniteActivity/v1";
const ASSET_VIEW_KEY = "cdf_cdm";
const ASSET_PROP_KEY = "CogniteAsset/v1";
const EQUIPMENT_PROP_KEY = "CogniteEquipment/v1";
const CANVAS_VIEW_KEY = "cdf_industrial_canvas";
const CANVAS_PROP_KEY = "Canvas/v7";

function makeNode(
	externalId: string,
	space = "s",
	properties: Record<string, Record<string, Record<string, unknown>>> = {}
) {
	return {
		instanceType: "node",
		externalId,
		space,
		version: 1,
		createdTime: 0,
		lastUpdatedTime: 0,
		properties,
	};
}

function activityNode(externalId: string, extra: Record<string, unknown> = {}) {
	return makeNode(externalId, "s", {
		[ACTIVITY_VIEW_KEY]: {
			[ACTIVITY_PROP_KEY]: { name: "WO Name", ...extra },
		},
	});
}

function assetNode(externalId: string, extra: Record<string, unknown> = {}) {
	return makeNode(externalId, "s", {
		[ASSET_VIEW_KEY]: {
			[ASSET_PROP_KEY]: { name: "Asset Name", ...extra },
		},
	});
}

function equipmentNode(
	externalId: string,
	extra: Record<string, unknown> = {}
) {
	return makeNode(externalId, "s", {
		[ASSET_VIEW_KEY]: {
			[EQUIPMENT_PROP_KEY]: { name: "Equipment Name", ...extra },
		},
	});
}

function woWithAsset(
	externalId: string,
	assetRef: { externalId: string; space: string }
) {
	return makeNode(externalId, "s", {
		[ACTIVITY_VIEW_KEY]: {
			[ACTIVITY_PROP_KEY]: { name: "WO", assets: [assetRef] },
		},
	});
}

function canvasNode(
	externalId: string,
	name: string,
	extra: Record<string, unknown> = {}
) {
	return makeNode(externalId, "s", {
		[CANVAS_VIEW_KEY]: {
			[CANVAS_PROP_KEY]: { name, visibility: "public", ...extra },
		},
	});
}

function buildClient(overrides: object = {}): CogniteClient {
	return cdfClientTestDouble({
		project: "proj",
		getBaseUrl: () => "https://cluster.cognitedata.com",
		...overrides,
	});
}

// ─── searchWorkOrders ────────────────────────────────────────────────────────

describe("searchWorkOrders", () => {
	let client: CogniteClient;
	const queryMock = vi.fn();
	const searchMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: {
				query: queryMock,
				search: searchMock,
			},
		});
	});

	it("returns empty items for empty query", async () => {
		const result = await searchWorkOrders(client, "  ");
		expect(result).toEqual({ items: [] });
		expect(queryMock).not.toHaveBeenCalled();
	});

	it("returns exact match on first page when found", async () => {
		const node = activityNode("wo-1");
		queryMock.mockResolvedValue({ items: { nodes: [node] } });

		const result = await searchWorkOrders(client, "wo-1");
		expect(result.items).toHaveLength(1);
		expect(result.items[0].externalId).toBe("wo-1");
	});

	it("falls back to text search when exact match returns nothing", async () => {
		queryMock.mockResolvedValue({ items: { nodes: [] } });
		const node = activityNode("wo-2");
		searchMock.mockResolvedValue({ items: [node] });

		const result = await searchWorkOrders(client, "maintenance");
		expect(searchMock).toHaveBeenCalled();
		expect(result.items[0].externalId).toBe("wo-2");
	});

	it("skips exact match when cursor is provided", async () => {
		const node = activityNode("wo-3");
		searchMock.mockResolvedValue({ items: [node] });

		const result = await searchWorkOrders(client, "query", "cursor-abc");
		expect(queryMock).not.toHaveBeenCalled();
		expect(result.items[0].externalId).toBe("wo-3");
	});

	it("includes nextCursor from search response", async () => {
		queryMock.mockResolvedValue({ items: { nodes: [] } });
		searchMock.mockResolvedValue({
			items: [],
			nextCursor: "next-page",
		});

		const result = await searchWorkOrders(client, "q");
		expect(result.nextCursor).toBe("next-page");
	});

	it("rethrows search failures instead of returning empty items", async () => {
		queryMock.mockRejectedValue(new Error("network"));
		await expect(searchWorkOrders(client, "q")).rejects.toThrow("network");
	});
});

// ─── listRecentWorkOrders ────────────────────────────────────────────────────

describe("listRecentWorkOrders", () => {
	let client: CogniteClient;
	const queryMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: { query: queryMock },
		});
		queryMock.mockResolvedValue({ items: { nodes: [] }, nextCursor: {} });
	});

	function queryFilter() {
		return queryMock.mock.calls[0][0].with.nodes.nodes.filter;
	}

	it("lists without extra filters but still scopes to hasData", async () => {
		const result = await listRecentWorkOrders(client);
		expect(queryMock).toHaveBeenCalled();
		expect(queryFilter()).toEqual({ hasData: [ACTIVITY_VIEW_ID] });
		expect(result.items).toEqual([]);
	});

	it("applies status filter", async () => {
		await listRecentWorkOrders(client, { status: "active" });
		expect(JSON.stringify(queryFilter())).toContain("status");
	});

	it("applies priority filter", async () => {
		await listRecentWorkOrders(client, { priority: "High" });
		expect(JSON.stringify(queryFilter())).toContain("priority");
	});

	it("applies dateFrom filter", async () => {
		await listRecentWorkOrders(client, { dateFrom: "2024-01-01" });
		expect(JSON.stringify(queryFilter())).toContain("range");
	});

	it("applies dateTo filter", async () => {
		await listRecentWorkOrders(client, { dateTo: "2024-12-31" });
		expect(JSON.stringify(queryFilter())).toContain("range");
	});

	it("combines multiple filters with and", async () => {
		await listRecentWorkOrders(client, {
			status: "open",
			priority: "Critical",
			dateFrom: "2024-01-01",
		});
		expect(JSON.stringify(queryFilter())).toContain("and");
	});

	it("returns items and nextCursor", async () => {
		const node = activityNode("wo-r");
		queryMock.mockResolvedValue({
			items: { nodes: [node] },
			nextCursor: { nodes: "cur" },
		});

		const result = await listRecentWorkOrders(client);
		expect(result.items[0].externalId).toBe("wo-r");
		expect(result.nextCursor).toBe("cur");
	});

	it("rethrows list failures instead of returning empty items", async () => {
		queryMock.mockRejectedValue(new Error("fail"));
		await expect(listRecentWorkOrders(client)).rejects.toThrow("fail");
	});
});

// ─── findWorkOrder ───────────────────────────────────────────────────────────

describe("findWorkOrder", () => {
	let client: CogniteClient;
	const queryMock = vi.fn();
	const searchMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: {
				query: queryMock,
				search: searchMock,
			},
		});
	});

	it("returns first match when found", async () => {
		queryMock.mockResolvedValue({ items: { nodes: [activityNode("wo-1")] } });
		const result = await findWorkOrder(client, "wo-1");
		expect(result?.externalId).toBe("wo-1");
	});

	it("returns null when no match", async () => {
		queryMock.mockResolvedValue({ items: { nodes: [] } });
		searchMock.mockResolvedValue({ items: [] });
		const result = await findWorkOrder(client, "missing");
		expect(result).toBeNull();
	});
});

// ─── getAssetForWorkOrder ─────────────────────────────────────────────────────

describe("getAssetForWorkOrder", () => {
	let client: CogniteClient;
	const retrieveMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: { retrieve: retrieveMock },
		});
	});

	it("returns null when workOrder has no externalId", async () => {
		const result = await getAssetForWorkOrder(client, {
			externalId: "",
			space: "s",
		});
		expect(result).toBeNull();
		expect(retrieveMock).not.toHaveBeenCalled();
	});

	it("returns null when work order node not found in DMS", async () => {
		retrieveMock.mockResolvedValue({ items: [] });
		const result = await getAssetForWorkOrder(client, {
			externalId: "wo-1",
			space: "s",
		});
		expect(result).toBeNull();
	});

	it("returns null when work order has no assets", async () => {
		retrieveMock.mockResolvedValueOnce({
			items: [
				makeNode("wo-1", "s", {
					[ACTIVITY_VIEW_KEY]: { [ACTIVITY_PROP_KEY]: {} },
				}),
			],
		});
		const result = await getAssetForWorkOrder(client, {
			externalId: "wo-1",
			space: "s",
		});
		expect(result).toBeNull();
	});

	it("throws user-facing error for multi-asset work orders", async () => {
		const node = makeNode("wo-multi", "s", {
			[ACTIVITY_VIEW_KEY]: {
				[ACTIVITY_PROP_KEY]: {
					assets: [
						{ externalId: "a1", space: "s" },
						{ externalId: "a2", space: "s" },
					],
				},
			},
		});
		retrieveMock.mockResolvedValueOnce({ items: [node] });
		await expect(
			getAssetForWorkOrder(client, { externalId: "wo-multi", space: "s" })
		).rejects.toThrow(/only single-asset/i);
	});

	it("returns asset from ASSET view when found", async () => {
		const wo = woWithAsset("wo-1", { externalId: "asset-1", space: "s" });
		retrieveMock
			.mockResolvedValueOnce({ items: [wo] })
			.mockResolvedValueOnce({ items: [assetNode("asset-1")] });

		const result = await getAssetForWorkOrder(client, {
			externalId: "wo-1",
			space: "s",
		});
		expect(result?.assetExternalId).toBe("asset-1");
	});

	it("falls back to EQUIPMENT view when ASSET view returns nothing", async () => {
		const wo = woWithAsset("wo-2", { externalId: "equip-1", space: "s" });
		retrieveMock
			.mockResolvedValueOnce({ items: [wo] })
			.mockResolvedValueOnce({ items: [] }) // ASSET view miss
			.mockResolvedValueOnce({ items: [equipmentNode("equip-1")] }); // EQUIPMENT hit

		const result = await getAssetForWorkOrder(client, {
			externalId: "wo-2",
			space: "s",
		});
		expect(result?.assetExternalId).toBe("equip-1");
	});

	it("returns null when neither ASSET nor EQUIPMENT view has the node", async () => {
		const wo = woWithAsset("wo-3", { externalId: "no-asset", space: "s" });
		retrieveMock
			.mockResolvedValueOnce({ items: [wo] })
			.mockResolvedValueOnce({ items: [] })
			.mockResolvedValueOnce({ items: [] });

		const result = await getAssetForWorkOrder(client, {
			externalId: "wo-3",
			space: "s",
		});
		expect(result).toBeNull();
	});

	it("swallows technical errors and returns null", async () => {
		retrieveMock.mockRejectedValue(new Error("network timeout"));
		const result = await getAssetForWorkOrder(client, {
			externalId: "wo-err",
			space: "s",
		});
		expect(result).toBeNull();
	});
});

// ─── getAssetResourcesGeneric ─────────────────────────────────────────────────

describe("getAssetResourcesGeneric", () => {
	let client: CogniteClient;
	const queryMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: { query: queryMock },
		});
		queryMock.mockResolvedValue({ items: { nodes: [] } });
	});

	it("returns empty for unknown resource type", async () => {
		const result = await getAssetResourcesGeneric(
			client,
			"asset-1",
			"s",
			"__unknown__"
		);
		expect(result).toEqual([]);
		expect(queryMock).not.toHaveBeenCalled();
	});

	it("fetches resources for a known resource type", async () => {
		const result = await getAssetResourcesGeneric(
			client,
			"asset-1",
			"s",
			"file"
		);
		expect(queryMock).toHaveBeenCalled();
		expect(result).toEqual([]);
	});

	it("filters child assets by CDM parent relation", async () => {
		await getAssetResourcesGeneric(client, "asset-1", "s", "asset");
		expect(queryMock).toHaveBeenCalledWith(
			expect.objectContaining({
				with: expect.objectContaining({
					nodes: expect.objectContaining({
						nodes: {
							filter: {
								and: [
									{ hasData: [ASSET_VIEW_ID] },
									{
										equals: {
											property: [
												ASSET_VIEW_ID.space,
												`${ASSET_VIEW_ID.externalId}/${ASSET_VIEW_ID.version}`,
												"parent",
											],
											value: { space: "s", externalId: "asset-1" },
										},
									},
								],
							},
						},
					}),
				}),
			})
		);
	});

	it("returns empty on error", async () => {
		queryMock.mockRejectedValue(new Error("fail"));
		const result = await getAssetResourcesGeneric(
			client,
			"asset-1",
			"s",
			"file"
		);
		expect(result).toEqual([]);
	});
});

// ─── getActivityResourcesGeneric ─────────────────────────────────────────────

describe("getActivityResourcesGeneric", () => {
	let client: CogniteClient;
	const queryMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: { query: queryMock },
		});
	});

	it("returns empty for unknown resource type", async () => {
		const result = await getActivityResourcesGeneric(
			client,
			"act-1",
			"s",
			"__unknown__"
		);
		expect(result).toEqual([]);
		expect(queryMock).not.toHaveBeenCalled();
	});

	it("returns empty for resource type without activityContainerIdentifier", async () => {
		// "file" type has no activityContainerIdentifier in the default config
		queryMock.mockResolvedValue({ items: { direct: [] } });
		const result = await getActivityResourcesGeneric(
			client,
			"act-1",
			"s",
			"file"
		);
		expect(result).toEqual([]);
	});

	it("returns empty on error", async () => {
		queryMock.mockRejectedValue(new Error("query failed"));
		const result = await getActivityResourcesGeneric(
			client,
			"act-1",
			"s",
			"timeseries"
		);
		expect(result).toEqual([]);
	});
});

// ─── listTemplateCanvases ────────────────────────────────────────────────────

describe("listTemplateCanvases", () => {
	let client: CogniteClient;
	const queryMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: { query: queryMock },
		});
	});

	it("returns empty when no canvases", async () => {
		queryMock.mockResolvedValue({ items: { nodes: [] } });
		const result = await listTemplateCanvases(client);
		expect(result).toEqual([]);
	});

	it("filters canvases by template prefix", async () => {
		const templateCanvas = canvasNode("c-1", "template_pump");
		const regularCanvas = canvasNode("c-2", "My canvas");
		queryMock.mockResolvedValue({
			items: { nodes: [templateCanvas, regularCanvas] },
		});

		const result = await listTemplateCanvases(client);
		expect(result).toHaveLength(1);
		expect(result[0].externalId).toBe("c-1");
	});

	it("deduplicates by name keeping most recently updated", async () => {
		const older = canvasNode("c-old", "template_pump", {
			updatedAt: "2024-01-01",
		});
		const newer = canvasNode("c-new", "template_pump", {
			updatedAt: "2024-06-01",
		});
		queryMock.mockResolvedValue({ items: { nodes: [older, newer] } });

		const result = await listTemplateCanvases(client);
		expect(result).toHaveLength(1);
		expect(result[0].externalId).toBe("c-new");
	});

	it("returns empty on error", async () => {
		queryMock.mockRejectedValue(new Error("fail"));
		const result = await listTemplateCanvases(client);
		expect(result).toEqual([]);
	});
});

describe("findCanvasesByName", () => {
	let client: CogniteClient;
	const queryMock = vi.fn();

	beforeEach(() => {
		vi.clearAllMocks();
		client = buildClient({
			instances: { query: queryMock },
		});
	});

	it("returns empty for blank name", async () => {
		const result = await findCanvasesByName(client, "   ");
		expect(result).toEqual([]);
		expect(queryMock).not.toHaveBeenCalled();
	});

	it("returns matching canvases", async () => {
		const canvas = canvasNode("c-1", "My Canvas");
		queryMock.mockResolvedValue({ items: { nodes: [canvas] } });

		const result = await findCanvasesByName(client, "My Canvas");
		expect(result).toHaveLength(1);
		expect(result[0].externalId).toBe("c-1");
	});

	it("returns empty on error", async () => {
		queryMock.mockRejectedValue(new Error("fail"));
		const result = await findCanvasesByName(client, "test");
		expect(result).toEqual([]);
	});
});
