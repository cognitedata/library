import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	ACTIVITY_VIEW_ID,
	ASSET_VIEW_ID,
	createMockAppConfig,
	EQUIPMENT_VIEW_ID,
} from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import {
	findWorkOrder,
	getAssetForWorkOrder,
	listRecentWorkOrders,
	localDayBoundIso,
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

		it("N06 returns case-insensitive externalId exact, prefix, and contains matches ranked ahead of name matches", async () => {
			query
				.mockResolvedValueOnce({
					items: {
						nodes: [
							activityNode("WO-1", { name: "Alpha" }),
							activityNode("wo-12", { name: "Beta" }),
						],
					},
				})
				.mockResolvedValueOnce({
					items: {
						nodes: [
							activityNode("WO-1", { name: "Alpha" }),
							activityNode("wo-12", { name: "Beta" }),
							activityNode("QA-WPG-WO-ONE", {
								name: "Replace mechanical seal",
							}),
							activityNode("WO", { name: "Exact" }),
						],
					},
					nextCursor: undefined,
				});
			search.mockResolvedValueOnce({
				items: [activityNode("name-hit", { name: "wo pump" })],
			});
			const result = await searchWorkOrders(client, "wo");
			expect(result.items.map((item) => item.externalId)).toEqual([
				"WO",
				"WO-1",
				"wo-12",
				"QA-WPG-WO-ONE",
				"name-hit",
			]);
			const filter = query.mock.calls[0][0].with.nodes.nodes.filter;
			expect(JSON.stringify(filter)).toContain("wo");
			expect(JSON.stringify(filter)).toContain("WO");
			expect(search.mock.calls[0][0].properties).toEqual([
				"name",
				"description",
			]);
		});

		it("N06 finds QA-WPG-WO-ONE for mid-ID WO-ONE, lowercase qa-wpg-wo-one, and numeric 01", async () => {
			const nodes = [
				activityNode("QA-WPG-WO-PAGE-01", {
					name: "QA WPG Pagination filler 01",
				}),
				activityNode("QA-WPG-WO-ONE", {
					name: "QA WPG Replace mechanical seal",
				}),
				activityNode("MORD_REST-136024", { name: "Other Operation" }),
			];

			// 1. Mid-ID substring `WO-ONE` finds `QA-WPG-WO-ONE` even when prefix and name search miss
			query
				.mockResolvedValueOnce({ items: { nodes: [] } })
				.mockResolvedValueOnce({ items: { nodes }, nextCursor: undefined });
			search.mockResolvedValueOnce({ items: [] });
			const midId = await searchWorkOrders(client, "WO-ONE");
			expect(midId.items.map((item) => item.externalId)).toEqual([
				"QA-WPG-WO-ONE",
			]);

			// 2. Lowercase full ID `qa-wpg-wo-one` ranks `QA-WPG-WO-ONE` first ahead of noisy word hits
			const fillerHits = Array.from({ length: 20 }, (_, i) =>
				activityNode(`QA-FILLER-${i}`, { name: `qa wpg filler ${i}` })
			);
			query
				.mockResolvedValueOnce({
					items: { nodes: [nodes[1]] },
				})
				.mockResolvedValueOnce({ items: { nodes }, nextCursor: undefined });
			search.mockResolvedValueOnce({ items: fillerHits });
			const lowerFull = await searchWorkOrders(client, "qa-wpg-wo-one");
			expect(lowerFull.items[0]?.externalId).toBe("QA-WPG-WO-ONE");

			// 3. `01` still finds `QA-WPG-WO-PAGE-01`
			query
				.mockResolvedValueOnce({ items: { nodes: [] } })
				.mockResolvedValueOnce({ items: { nodes }, nextCursor: undefined });
			search.mockResolvedValueOnce({ items: [nodes[0]] });
			const numeric = await searchWorkOrders(client, "01");
			expect(numeric.items.map((item) => item.externalId)).toContain(
				"QA-WPG-WO-PAGE-01"
			);
		});

		it("N11 returns every matching work order across spaces", async () => {
			const twinA = { ...activityNode("QA-WPG-WO-TWIN"), space: "space-a" };
			const twinB = { ...activityNode("QA-WPG-WO-TWIN"), space: "space-b" };
			query
				.mockResolvedValueOnce({ items: { nodes: [twinA, twinB] } })
				.mockResolvedValueOnce({
					items: { nodes: [twinA, twinB] },
					nextCursor: undefined,
				});
			search.mockResolvedValueOnce({ items: [] });
			const result = await searchWorkOrders(client, "TWIN");
			expect(
				result.items.map((item) => `${item.space}/${item.externalId}`)
			).toEqual(["space-a/QA-WPG-WO-TWIN", "space-b/QA-WPG-WO-TWIN"]);
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
			query.mockResolvedValue({ items: { nodes: [] } });
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

		it("C12 uses local start and end of day for an injected time zone", async () => {
			query.mockResolvedValueOnce({ items: { nodes: [] } });
			const timeZone = "Pacific/Auckland";
			await listRecentWorkOrders(
				client,
				{ dateFrom: "2024-01-01", dateTo: "2024-01-01" },
				undefined,
				undefined,
				timeZone
			);
			const filter = JSON.stringify(
				query.mock.calls[0][0].with.nodes.nodes.filter
			);
			expect(filter).toContain(
				localDayBoundIso("2024-01-01", "start", timeZone)
			);
			expect(filter).toContain(localDayBoundIso("2024-01-01", "end", timeZone));
			expect(localDayBoundIso("2024-01-01", "start", "UTC")).toBe(
				"2024-01-01T00:00:00.000Z"
			);
			expect(localDayBoundIso("2024-01-01", "start", timeZone)).not.toBe(
				"2024-01-01T00:00:00.000Z"
			);
		});

		it("C12 covers the full 24-hour local day in Europe/Oslo and America/New_York", () => {
			// Oslo (UTC+1 in November): local day 2026-11-25 runs 2026-11-24T23:00:00.000Z..2026-11-25T22:59:59.999Z,
			// and local day 2026-11-26 includes 2026-11-25T23:30:00.000Z (00:30 local on Nov 26).
			const osloStart25 = localDayBoundIso(
				"2026-11-25",
				"start",
				"Europe/Oslo"
			);
			const osloEnd25 = localDayBoundIso("2026-11-25", "end", "Europe/Oslo");
			expect(osloStart25).toBe("2026-11-24T23:00:00.000Z");
			expect(osloEnd25).toBe("2026-11-25T22:59:59.999Z");
			expect("2026-11-25T00:30:00.000Z" >= osloStart25).toBe(true);
			expect("2026-11-25T00:30:00.000Z" <= osloEnd25).toBe(true);

			const osloStart26 = localDayBoundIso(
				"2026-11-26",
				"start",
				"Europe/Oslo"
			);
			const osloEnd26 = localDayBoundIso("2026-11-26", "end", "Europe/Oslo");
			expect("2026-11-25T23:30:00.000Z" >= osloStart26).toBe(true);
			expect("2026-11-25T23:30:00.000Z" <= osloEnd26).toBe(true);

			// New York (UTC-5 in November): local day 2026-11-25 runs 2026-11-25T05:00:00.000Z..2026-11-26T04:59:59.999Z
			// (full 24 hours, including 23:30Z on the chosen day, instead of the broken 5-hour window).
			const nyStart = localDayBoundIso(
				"2026-11-25",
				"start",
				"America/New_York"
			);
			const nyEnd = localDayBoundIso("2026-11-25", "end", "America/New_York");
			expect(nyStart).toBe("2026-11-25T05:00:00.000Z");
			expect(nyEnd).toBe("2026-11-26T04:59:59.999Z");
			expect(new Date(nyEnd).getTime() - new Date(nyStart).getTime()).toBe(
				24 * 60 * 60 * 1000 - 1
			);
			expect("2026-11-25T23:30:00.000Z" >= nyStart).toBe(true);
			expect("2026-11-25T23:30:00.000Z" <= nyEnd).toBe(true);
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
			query.mockResolvedValueOnce({ items: { nodes: [] } });
			search.mockResolvedValueOnce({ items: [] });
			await expect(findWorkOrder(client, "wo-1")).resolves.toMatchObject({
				externalId: "wo-1",
			});

			query.mockResolvedValue({ items: { nodes: [] } });
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

		it("C06 uses the first asset when a work order has several", async () => {
			retrieve
				.mockResolvedValueOnce({
					items: [
						activityNode("wo", {
							assets: [
								{ externalId: "a1", space: "s" },
								{ externalId: "a2", space: "s" },
							],
						}),
					],
				})
				.mockResolvedValueOnce({
					items: [
						{
							externalId: "a1",
							space: "s",
							properties: {
								[ASSET_VIEW_ID.space]: { [assetKey]: { name: "First" } },
							},
						},
					],
				});
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).resolves.toMatchObject({ assetExternalId: "a1", assetName: "First" });
		});

		it("C13 reads the asset link property from workOrderProperties", async () => {
			const config = createMockAppConfig();
			config.workOrderProperties = {
				...config.workOrderProperties,
				assets: "equipmentLinks",
			};
			retrieve
				.mockResolvedValueOnce({
					items: [
						activityNode("wo", {
							equipmentLinks: [{ externalId: "a1", space: "s" }],
						}),
					],
				})
				.mockResolvedValueOnce({
					items: [
						{
							externalId: "a1",
							space: "s",
							properties: {
								[ASSET_VIEW_ID.space]: { [assetKey]: { name: "Linked" } },
							},
						},
					],
				});
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" }, config)
			).resolves.toMatchObject({ assetExternalId: "a1", assetName: "Linked" });
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

		it("C05 throws when asset lookup fails so the caller can leave the loading state", async () => {
			retrieve.mockRejectedValueOnce(new Error("network"));
			await expect(
				getAssetForWorkOrder(client, { externalId: "wo", space: "s" })
			).rejects.toThrow("network");
		});
	});
});
