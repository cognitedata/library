import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { FILE_VIEW_ID } from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import { getResourceInstancesByRefs, searchResourcesByName } from "./resources";

installInlineApiRuntime();

describe("getResourceInstancesByRefs", () => {
	let client: CogniteClient;
	let retrieve: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		retrieve = vi.fn();
		client = cdfClientTestDouble({
			instances: { retrieve },
		});
	});

	it("returns [] for unknown type or empty refs", async () => {
		expect(await getResourceInstancesByRefs(client, "unknown", [])).toEqual([]);
		expect(
			await getResourceInstancesByRefs(client, "asset", [
				{ externalId: "", space: "s" },
			])
		).toEqual([]);
	});

	it("fetches instances by external id and space", async () => {
		const assetKey = `CogniteAsset/v1`;
		retrieve.mockResolvedValueOnce({
			items: [
				{
					externalId: "a1",
					space: "s",
					properties: {
						cdf_cdm: {
							[assetKey]: { name: "Asset 1" },
						},
					},
				},
			],
		});

		const out = await getResourceInstancesByRefs(client, "asset", [
			{ externalId: "a1", space: "s" },
		]);
		expect(out).toHaveLength(1);
		expect(out[0].name).toBe("Asset 1");
	});
});

describe("searchResourcesByName", () => {
	let client: CogniteClient;
	let search: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		search = vi.fn();
		client = cdfClientTestDouble({
			instances: { search },
		});
	});

	it("returns [] for empty query or unknown type", async () => {
		expect(await searchResourcesByName(client, "  ", "file")).toEqual([]);
		expect(await searchResourcesByName(client, "q", "unknown")).toEqual([]);
	});

	it("searches by name for a resource type", async () => {
		const fileKey = `${FILE_VIEW_ID.externalId}/${FILE_VIEW_ID.version}`;
		search.mockResolvedValueOnce({
			items: [
				{
					externalId: "f-search",
					space: "s",
					properties: {
						[FILE_VIEW_ID.space]: {
							[fileKey]: { name: "Found" },
						},
					},
				},
			],
		});

		const out = await searchResourcesByName(client, "found", "file");
		expect(out).toHaveLength(1);
		expect(out[0].externalId).toBe("f-search");
		expect(search).toHaveBeenCalled();
	});
});
