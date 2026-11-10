import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ACTIVITY_VIEW_ID, FILE_VIEW_ID } from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import {
	getActivityResourcesGeneric,
	getAssetResourcesGeneric,
} from "./resources";

installInlineApiRuntime();

describe("retrieve-data/resources", () => {
	let client: CogniteClient;
	let list: ReturnType<typeof vi.fn>;
	let query: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		list = vi.fn();
		query = vi.fn();
		client = cdfClientTestDouble({
			instances: { list, query },
		});
	});

	describe("getAssetResourcesGeneric", () => {
		it("returns [] for unknown resource type", async () => {
			await expect(
				getAssetResourcesGeneric(client, "a", "s", "unknown")
			).resolves.toEqual([]);
			expect(query).not.toHaveBeenCalled();
		});

		it("uses containsAny filter for array asset relations", async () => {
			query.mockResolvedValueOnce({ items: { nodes: [] } });
			await getAssetResourcesGeneric(client, "a1", "s", "file");
			expect(query).toHaveBeenCalledWith(
				expect.objectContaining({
					with: expect.objectContaining({
						nodes: expect.objectContaining({
							nodes: {
								filter: {
									and: [
										{ hasData: [FILE_VIEW_ID] },
										{
											containsAny: {
												property: [
													FILE_VIEW_ID.space,
													`${FILE_VIEW_ID.externalId}/${FILE_VIEW_ID.version}`,
													"assets",
												],
												values: [{ space: "s", externalId: "a1" }],
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

		it("maps valid nodes and skips invalid items", async () => {
			const fileKey = `${FILE_VIEW_ID.externalId}/${FILE_VIEW_ID.version}`;
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						{ notANode: true },
						{
							externalId: "f1",
							space: "s",
							properties: {
								[FILE_VIEW_ID.space]: { [fileKey]: { name: "Doc" } },
							},
						},
					],
				},
			});
			const out = await getAssetResourcesGeneric(client, "a", "s", "file");
			expect(out).toHaveLength(1);
			expect(out[0].externalId).toBe("f1");
		});

		it("N13: throws when the asset resource read fails", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(
				getAssetResourcesGeneric(client, "a", "s", "file")
			).rejects.toThrow("fail");
		});

		it("N11: returns every id match with its space", async () => {
			const fileKey = `${FILE_VIEW_ID.externalId}/${FILE_VIEW_ID.version}`;
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						{
							externalId: "twin",
							space: "space-a",
							properties: {
								[FILE_VIEW_ID.space]: { [fileKey]: { name: "Twin A" } },
							},
						},
						{
							externalId: "twin",
							space: "space-b",
							properties: {
								[FILE_VIEW_ID.space]: { [fileKey]: { name: "Twin B" } },
							},
						},
					],
				},
			});
			const out = await getAssetResourcesGeneric(client, "a", "s", "file");
			expect(out).toEqual([
				expect.objectContaining({ externalId: "twin", space: "space-a" }),
				expect.objectContaining({ externalId: "twin", space: "space-b" }),
			]);
		});
	});

	describe("getActivityResourcesGeneric", () => {
		it("returns [] without activityContainerIdentifier", async () => {
			await expect(
				getActivityResourcesGeneric(client, "act", "s", "file")
			).resolves.toEqual([]);
			expect(query).not.toHaveBeenCalled();
		});

		it("queries timeseries via activity container and maps results", async () => {
			const tsKey = "CogniteTimeSeries/v1";
			query.mockResolvedValueOnce({
				items: {
					direct: [
						{ notANode: true },
						{
							externalId: "ts1",
							space: "s",
							properties: { cdf_cdm: { [tsKey]: { name: "T" } } },
						},
					],
				},
				nextCursor: {},
			});
			const out = await getActivityResourcesGeneric(
				client,
				"act",
				ACTIVITY_VIEW_ID.space,
				"timeseries"
			);
			expect(out).toHaveLength(1);
			expect(out[0].externalId).toBe("ts1");
			expect(JSON.stringify(query.mock.calls[0][0])).toContain("timeSeries");
		});

		it("N13: throws when the activity resource read fails", async () => {
			query.mockRejectedValueOnce(new Error("query failed"));
			await expect(
				getActivityResourcesGeneric(client, "act", "s", "timeseries")
			).rejects.toThrow("query failed");
		});
	});
});
