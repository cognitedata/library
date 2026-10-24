/**
 * Tests for listInstanceNodesWithCap — shared DMS query paginator.
 */
import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import {
	combineQueryNodeFilter,
	listInstanceNodesWithCap,
	retrieveInstanceNodes,
} from "./list-nodes";

installInlineApiRuntime();

const canvasSource = {
	source: {
		type: "view" as const,
		space: "cdf_industrial_canvas",
		externalId: "Canvas",
		version: "v1",
	},
};

describe("combineQueryNodeFilter", () => {
	it("scopes listing to hasData on the requested views", () => {
		expect(combineQueryNodeFilter([canvasSource])).toEqual({
			hasData: [canvasSource.source],
		});
	});

	it("ands hasData with an extra property filter", () => {
		const extra = {
			prefix: { property: ["node", "externalId"], value: "tmpl" },
		};
		expect(combineQueryNodeFilter([canvasSource], extra)).toEqual({
			and: [{ hasData: [canvasSource.source] }, extra],
		});
	});

	it("returns only the extra filter when no sources are given", () => {
		const extra = { equals: { property: ["node", "space"], value: "s" } };
		expect(combineQueryNodeFilter([], extra)).toEqual(extra);
	});
});

describe("listInstanceNodesWithCap", () => {
	let client: CogniteClient;
	let queryMock: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		queryMock = vi.fn();
		client = cdfClientTestDouble({
			instances: {
				query: queryMock,
			},
		});
	});

	it("paginates until the cursor is exhausted", async () => {
		queryMock
			.mockResolvedValueOnce({
				items: { nodes: [{ externalId: "a" }] },
				nextCursor: { nodes: "c1" },
			})
			.mockResolvedValueOnce({
				items: { nodes: [{ externalId: "b" }] },
			});

		const items = await listInstanceNodesWithCap(client, {
			sources: [
				{
					source: {
						type: "view",
						space: "s",
						externalId: "v",
						version: "v1",
					},
				},
			],
			maxTotalItems: 100,
			warnContext: "test",
		});

		expect(items).toHaveLength(2);
		expect(queryMock).toHaveBeenCalledTimes(2);
		expect(queryMock.mock.calls[1][0].cursors.nodes).toBe("c1");
		expect(queryMock.mock.calls[0][0].with.nodes.nodes.filter).toEqual({
			hasData: [
				{
					type: "view",
					space: "s",
					externalId: "v",
					version: "v1",
				},
			],
		});
	});

	it("stops at maxTotalItems", async () => {
		queryMock.mockResolvedValue({
			items: {
				nodes: [{ externalId: "a" }, { externalId: "b" }, { externalId: "c" }],
			},
			nextCursor: { nodes: "more" },
		});

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

		const items = await listInstanceNodesWithCap(client, {
			sources: [
				{
					source: {
						type: "view",
						space: "s",
						externalId: "v",
						version: "v1",
					},
				},
			],
			maxTotalItems: 2,
			warnContext: "cap-test",
		});

		expect(items).toHaveLength(2);
		expect(warnSpy).toHaveBeenCalled();
		warnSpy.mockRestore();
	});
});

describe("retrieveInstanceNodes", () => {
	const sources = [canvasSource];
	const refs = [{ space: "valhall-events", externalId: "WO-2023-001" }];

	it("retrieves nodes without ignoreUnknownIds", async () => {
		const retrieve = vi
			.fn()
			.mockResolvedValue({ items: [{ externalId: "WO-2023-001" }] });
		const client = cdfClientTestDouble({ instances: { retrieve } });

		const items = await retrieveInstanceNodes(client, refs, sources);

		expect(items).toEqual([{ externalId: "WO-2023-001" }]);
		expect(retrieve).toHaveBeenCalledWith({
			items: [{ instanceType: "node", ...refs[0] }],
			sources,
		});
	});

	it("returns an empty list when the instances are missing", async () => {
		const retrieve = vi.fn().mockRejectedValue({
			status: 400,
			missing: [{ space: "valhall-events", externalId: "WO-2023-001" }],
		});
		const client = cdfClientTestDouble({ instances: { retrieve } });

		await expect(retrieveInstanceNodes(client, refs, sources)).resolves.toEqual(
			[]
		);
	});

	it("rethrows retrieve failures that are not missing ids", async () => {
		const retrieve = vi.fn().mockRejectedValue({
			status: 400,
			errorMessage: "Unexpected field - ignoreUnknownIds",
		});
		const client = cdfClientTestDouble({ instances: { retrieve } });

		await expect(retrieveInstanceNodes(client, refs, sources)).rejects.toEqual({
			status: 400,
			errorMessage: "Unexpected field - ignoreUnknownIds",
		});
	});
});
