import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	ACTIVITY_VIEW_ID,
	ASSET_VIEW_ID,
	EQUIPMENT_VIEW_ID,
} from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import {
	findWorkOrder,
	getAssetForWorkOrder,
	listRecentWorkOrders,
	searchWorkOrders,
} from "./work-orders";

installInlineApiRuntime();

const actKey = `${ACTIVITY_VIEW_ID.externalId}/${ACTIVITY_VIEW_ID.version}`;
const assetKey = `${ASSET_VIEW_ID.externalId}/${ASSET_VIEW_ID.version}`;
const equipKey = `${EQUIPMENT_VIEW_ID.externalId}/${EQUIPMENT_VIEW_ID.version}`;

function activityNode(externalId: string, extra: Record<string, unknown> = {}) {
	return {
		externalId,
		space: "s",
		properties: {
			[ACTIVITY_VIEW_ID.space]: { [actKey]: { name: "WO", ...extra } },
		},
	};
}

describe("retrieve-data/work-orders", () => {
	let client: CogniteClient;
	let retrieve: ReturnType<typeof vi.fn>;
	let search: ReturnType<typeof vi.fn>;
	let query: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		retrieve = vi.fn();
		search = vi.fn();
		query = vi.fn();
		client = cdfClientTestDouble({
			instances: { retrieve, search, query },
		});
	});

	describe("searchWorkOrders", () => {
		it("returns empty items for blank query", async () => {
			await expect(searchWorkOrders(client, "  ")).resolves.toEqual({
				items: [],
			});
			expect(query).not.toHaveBeenCalled();
		});

		it("returns exact match on first page", async () => {
			query.mockResolvedValueOnce({
				items: { nodes: [activityNode("wo-1")] },
			});
			const result = await searchWorkOrders(client, "wo-1");
			expect(result.items[0].externalId).toBe("wo-1");
			expect(search).not.toHaveBeenCalled();
		});

		it("skips exact match when cursor is provided", async () => {
			search.mockResolvedValueOnce({
				items: [activityNode("wo-2"), { invalid: true }],
				nextCursor: "n",
			});
			const result = await searchWorkOrders(client, "q", "cursor");
			expect(query).not.toHaveBeenCalled();
			expect(result.items).toHaveLength(1);
			expect(result.nextCursor).toBe("n");
		});

		it("omits nextCursor when search payload has a non-string cursor", async () => {
			search.mockResolvedValueOnce({
				items: [activityNode("wo-2")],
				nextCursor: { nested: true },
			});
			const result = await searchWorkOrders(client, "q", "cursor");
			expect(result.nextCursor).toBeUndefined();
		});

		it("falls back to search when exact match is empty", async () => {
			query.mockResolvedValueOnce({ items: { nodes: [] } });
			search.mockResolvedValueOnce({ items: [activityNode("wo-3")] });
			const result = await searchWorkOrders(client, "pump");
			expect(result.items[0].externalId).toBe("wo-3");
		});

		it("rethrows search failures instead of returning empty items", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(searchWorkOrders(client, "q")).rejects.toThrow("fail");
		});
	});

	describe("listRecentWorkOrders", () => {
		it("scopes listing to CogniteActivity via hasData when no extra filters", async () => {
			query.mockResolvedValueOnce({ items: { nodes: [] } });
			await listRecentWorkOrders(client);
			expect(query.mock.calls[0][0].with.nodes.nodes.filter).toEqual({
				hasData: [ACTIVITY_VIEW_ID],
			});
		});

		it("ands hasData with a single status filter", async () => {
			query.mockResolvedValueOnce({ items: { nodes: [] } });
			await listRecentWorkOrders(client, { status: "open" });
			const filter = query.mock.calls[0][0].with.nodes.nodes.filter;
			expect(filter.and).toHaveLength(2);
			expect(filter.and[0]).toEqual({ hasData: [ACTIVITY_VIEW_ID] });
			expect(filter.and[1].equals).toBeDefined();
		});

		it("combines status, priority, and date range with and", async () => {
			query.mockResolvedValueOnce({
				items: { nodes: [activityNode("wo-r"), { invalid: true }] },
				nextCursor: { nodes: "c" },
			});
			const result = await listRecentWorkOrders(client, {
				status: "open",
				priority: "High",
				dateFrom: "2024-01-01",
				dateTo: "2024-12-31",
			});
			expect(
				JSON.stringify(query.mock.calls[0][0].with.nodes.nodes.filter)
			).toContain("and");
			expect(result.items).toHaveLength(1);
			expect(result.nextCursor).toBe("c");
		});

		it("rethrows list failures instead of returning empty items", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(listRecentWorkOrders(client, {})).rejects.toThrow("fail");
		});
	});

	describe("findWorkOrder", () => {
		it("returns first match or null", async () => {
			query.mockResolvedValueOnce({
				items: { nodes: [activityNode("wo-1")] },
			});
			await expect(findWorkOrder(client, "wo-1")).resolves.toMatchObject({
				externalId: "wo-1",
			});

			query.mockResolvedValueOnce({ items: { nodes: [] } });
			search.mockResolvedValueOnce({ items: [] });
			await expect(findWorkOrder(client, "missing")).resolves.toBeNull();
		});
	});

	describe("getAssetForWorkOrder", () => {
		it("returns null when work order has no externalId", async () => {
			await expect(
				getAssetForWorkOrder(client, { externalId: "", space: "s" })
			).resolves.toBeNull();
			expect(retrieve).not.toHaveBeenCalled();
		});

		it("returns null when work order node is missing or has no assets", async () => {
			retrieve.mockResolvedValueOnce({ items: [] });
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).resolves.toBeNull();

			retrieve.mockResolvedValueOnce({ items: [activityNode("wo")] });
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).resolves.toBeNull();
		});

		it("throws a user-facing error for multi-asset work orders", async () => {
			retrieve.mockResolvedValueOnce({
				items: [
					activityNode("wo", {
						assets: [
							{ externalId: "a1", space: "s" },
							{ externalId: "a2", space: "s" },
						],
					}),
				],
			});
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).rejects.toThrow(/only single-asset/i);
		});

		it("resolves an asset, then equipment fallback, then null", async () => {
			const withAsset = activityNode("wo", {
				assets: [{ externalId: "a1", space: "s" }],
			});
			retrieve
				.mockResolvedValueOnce({ items: [withAsset] })
				.mockResolvedValueOnce({
					items: [
						{
							externalId: "a1",
							space: "s",
							properties: {
								[ASSET_VIEW_ID.space]: { [assetKey]: { name: "Asset" } },
							},
						},
					],
				});
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).resolves.toMatchObject({ assetExternalId: "a1", assetName: "Asset" });

			retrieve
				.mockResolvedValueOnce({ items: [withAsset] })
				.mockResolvedValueOnce({ items: [] })
				.mockResolvedValueOnce({
					items: [
						{
							externalId: "a1",
							space: "s",
							properties: {
								[EQUIPMENT_VIEW_ID.space]: { [equipKey]: { name: "Eq" } },
							},
						},
					],
				});
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).resolves.toMatchObject({ assetExternalId: "a1", assetName: "Eq" });

			retrieve
				.mockResolvedValueOnce({ items: [withAsset] })
				.mockResolvedValueOnce({ items: [] })
				.mockResolvedValueOnce({ items: [] });
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).resolves.toBeNull();
		});

		it("swallows technical errors", async () => {
			retrieve.mockRejectedValueOnce(new Error("network"));
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).resolves.toBeNull();
		});
	});
});
