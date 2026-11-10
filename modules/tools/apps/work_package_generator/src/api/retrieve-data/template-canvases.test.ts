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
		it("filters by template prefix and skips invalid nodes", async () => {
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
			expect(result.map((canvas) => canvas.externalId).sort()).toEqual([
				"new",
				"old",
			]);
			expect(
				result.find((canvas) => canvas.externalId === "new")?.visibility
			).toBe("public");
			expect(
				JSON.stringify(query.mock.calls[0][0].with.nodes.nodes.filter)
			).toContain("hasData");
		});

		it("N13: throws when the template list read fails", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(listTemplateCanvases(client)).rejects.toThrow("fail");
		});

		it("B-D4: lists every template with the same name", async () => {
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						makeNode("old", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "template_pump",
							visibility: "public",
							updatedAt: "2024-01-01",
							createdBy: "a",
							isArchived: false,
						}),
						makeNode("new", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "template_pump",
							visibility: "public",
							updatedAt: "2024-06-01",
							createdBy: "b",
							isArchived: false,
						}),
					],
				},
			});
			const result = await listTemplateCanvases(client);
			expect(result.map((canvas) => canvas.externalId).sort()).toEqual([
				"new",
				"old",
			]);
			expect(result.every((canvas) => canvas.createdBy)).toBe(true);
		});

		it("N10: lists public canvases and only the current user's private ones", async () => {
			query.mockResolvedValueOnce({
				items: {
					nodes: [
						makeNode("pub", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "template_public",
							visibility: "public",
							createdBy: "someone-else",
						}),
						makeNode("mine", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "template_mine",
							visibility: "private",
							createdBy: "work-package-generator",
						}),
						makeNode("theirs", "s", CANVAS_VIEW_ID.space, canvasKey, {
							name: "template_theirs",
							visibility: "private",
							createdBy: "other-user",
						}),
					],
				},
			});
			const result = await listTemplateCanvases(client);
			expect(result.map((canvas) => canvas.externalId).sort()).toEqual([
				"mine",
				"pub",
			]);
			const filter = query.mock.calls[0][0].with.nodes.nodes.filter;
			expect(JSON.stringify(filter)).toContain("isArchived");
			expect(JSON.stringify(filter)).toContain("template");
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

		it("N13: throws when the name lookup fails", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(findCanvasesByName(client, "x")).rejects.toThrow("fail");
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
			expect(result[0].id).toBe("i1");
			expect(result[0].label).toBe("Main asset");
			expect(result[0].x).toBe(10);
			expect(
				query.mock.calls[0][0].with.nodes.nodes.filter.and[1].prefix.value
			).toBe("tmpl_");
			expect(result[0].width).toBe(DEFAULT_CANVAS_DIMENSIONS.width);
		});

		it("N13: throws when the instance read fails", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(getTemplateCanvasInstances(client, "t")).rejects.toThrow(
				"fail"
			);
		});

		it("returns [] when the template has no instances", async () => {
			query.mockResolvedValueOnce({ items: { nodes: [] } });
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
								containerId: "shape-1",
							}
						),
					],
				},
			});
			const result = await getTemplateCanvasAnnotations(client, "tmpl");
			expect(result).toHaveLength(1);
			expect(result[0].type).toBe("note");
			expect(result[0].id).toBe("a1");
			expect(result[0].containerId).toBe("shape-1");
		});

		it("C20: throws when the annotation read fails", async () => {
			query.mockRejectedValueOnce(new Error("fail"));
			await expect(getTemplateCanvasAnnotations(client, "t")).rejects.toThrow(
				"fail"
			);
		});
	});
});
