import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	CANVAS_ANNOTATION_VIEW_ID,
	CANVAS_INSTANCE_VIEW_ID,
	CANVAS_VIEW_ID,
	DEFAULT_CANVAS_DIMENSIONS,
} from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";
import {
	findCanvasesByName,
	getTemplateCanvasAnnotations,
	getTemplateCanvasInstances,
	listTemplateCanvases,
} from "./template-canvases";

installInlineApiRuntime();

function makeNode(
	externalId: string,
	space: string,
	viewSpace: string,
	viewKey: string,
	props: Record<string, unknown>
) {
	return {
		externalId,
		space,
		properties: { [viewSpace]: { [viewKey]: props } },
	};
}

const canvasKey = `${CANVAS_VIEW_ID.externalId}/${CANVAS_VIEW_ID.version}`;
const instanceKey = `${CANVAS_INSTANCE_VIEW_ID.externalId}/${CANVAS_INSTANCE_VIEW_ID.version}`;
const annotationKey = `${CANVAS_ANNOTATION_VIEW_ID.externalId}/${CANVAS_ANNOTATION_VIEW_ID.version}`;

describe("retrieve-data/template-canvases", () => {
	let client: CogniteClient;
	let query: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		query = vi.fn();
		client = cdfClientTestDouble({ instances: { query } });
	});

	describe("listTemplateCanvases", () => {
		it("filters by template prefix, skips invalid nodes, and keeps newest per name", async () => {
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						{ invalid: true },
						makeNode("old", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "template_pump",
							visibility: "public",
							updatedAt: "2024-01-01",
						}),
						makeNode("new", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "template_pump",
							visibility: "public",
							updatedAt: "2024-06-01",
							createdBy: "u",
						}),
						makeNode("other", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "My canvas",
						}),
					],
				},
			});
			const result = await listTemplateCanvases(client);
			expect(result).toHaveLength(1);
			expect(result[0].externalId).toBe("new");
			expect(result[0].visibility).toBe("public");
			expect(query.mock.calls[0][0].with.nodes.nodes.filter).toEqual({
				hasData: [CANVAS_VIEW_ID],
			});
		});

		it("returns [] on error", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(listTemplateCanvases(client)).resolves.toEqual([]);
		});
	});

	describe("findCanvasesByName", () => {
		it("returns [] for blank names", async () => {
			await expect(findCanvasesByName(client, "  ")).resolves.toEqual([]);
			expect(query).not.toHaveBeenCalled();
		});

		it("returns exact name matches and skips invalid nodes", async () => {
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						{ invalid: true },
						makeNode("c1", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "  My Canvas  ",
							visibility: "private",
						}),
					],
				},
			});
			const result = await findCanvasesByName(client, " My Canvas ");
			expect(result).toHaveLength(1);
			expect(result[0].externalId).toBe("c1");
			expect(result[0].visibility).toBe("private");
		});

		it("returns [] on error", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(findCanvasesByName(client, "x")).resolves.toEqual([]);
		});
	});

	describe("getTemplateCanvasInstances", () => {
		it("maps instance properties and skips invalid nodes", async () => {
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						{ invalid: true },
						makeNode("i1", "s", CANVAS_INSTANCE_VIEW_ID.space, instanceKey, {
							label: "Main asset",
							viewExternalId: "CogniteAsset",
							viewSpace: "cdf_cdm",
							x: 10,
						}),
					],
				},
			});
			const result = await getTemplateCanvasInstances(client, "tmpl");
			expect(result).toHaveLength(1);
			expect(result[0].label).toBe("Main asset");
			expect(result[0].x).toBe(10);
			expect(result[0].width).toBe(DEFAULT_CANVAS_DIMENSIONS.width);
		});

		it("returns [] on error", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(getTemplateCanvasInstances(client, "t")).resolves.toEqual(
				[]
			);
		});
	});

	describe("getTemplateCanvasAnnotations", () => {
		it("maps annotations and skips invalid nodes", async () => {
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						{ invalid: true },
						makeNode(
							"a1",
							"s",
							CANVAS_ANNOTATION_VIEW_ID.space,
							annotationKey,
							{
								annotationType: "note",
							}
						),
					],
				},
			});
			const result = await getTemplateCanvasAnnotations(client, "tmpl");
			expect(result).toHaveLength(1);
			expect(result[0].type).toBe("note");
		});

		it("returns [] on error", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(getTemplateCanvasAnnotations(client, "t")).resolves.toEqual(
				[]
			);
		});
	});
});
