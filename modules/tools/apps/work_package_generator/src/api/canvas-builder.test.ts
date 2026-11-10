import type { CogniteClient } from "@cognite/sdk";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { installInlineApiRuntime } from "@/__mocks__/inline-api-runtime";

installInlineApiRuntime();

// vi.mock: UUID is non-deterministic; fixed value keeps canvas item IDs assertable.
vi.mock("../utils/uuid", () => ({
	generateUuid: vi.fn().mockReturnValue("uuid-fixed"),
}));

import {
	BASE_TEMPLATE_ID,
	DEFAULT_CANVAS_CONFIG,
	DEFAULT_CANVAS_DIMENSIONS,
} from "@/__mocks__/app-config";
import type {
	Asset,
	CanvasConfig,
	TemplateInstance,
	WorkOrder,
} from "../types";
import { workPackageSolutionTagInstanceRef } from "../utils/canvas-solution-tags";
import { resetCanvasUserIdCache } from "../utils/canvas-user";
import { generateUuid } from "../utils/uuid";

const tmpl = {
	...DEFAULT_CANVAS_DIMENSIONS,
	properties: {} as Record<string, unknown>,
};

import {
	buildCanvasItemsFromTemplate,
	createCanvasFromItems,
	createCanvasFromTemplateAndSelections,
	extractClusterFromUrl,
	generateCanvasLink,
	updateCanvasFromTemplateAndSelections,
	validateCanvasCreation,
} from "./canvas-builder";

const getTemplateCanvasInstances = vi.fn();
const getTemplateCanvasAnnotations = vi.fn();
const templateReads = {
	getTemplateCanvasInstances,
	getTemplateCanvasAnnotations,
};

const config: CanvasConfig = DEFAULT_CANVAS_CONFIG;

beforeEach(() => {
	resetCanvasUserIdCache();
});

describe("buildCanvasItemsFromTemplate", () => {
	const asset: Asset = { assetExternalId: "a1", space: "s", assetName: "A" };
	const workOrder: WorkOrder = { externalId: "wo1", space: "s", name: "W" };

	it("includes annotations and applies main asset replacement", () => {
		const instances: TemplateInstance[] = [
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				instanceExternalId: "old",
				instanceSpace: "os",
				...tmpl,
			},
		];
		const items = buildCanvasItemsFromTemplate(
			instances,
			[{ type: "note", properties: { x: 1 } }],
			{},
			asset,
			workOrder,
			config
		);
		expect(items).toHaveLength(2);
		expect(items[0].type).toBe("CanvasAnnotation");
		const inst = items[1];
		expect(inst.type).toBe("fdmInstance");
		if (inst.type === "fdmInstance") {
			expect(inst.instanceExternalId).toBe("a1");
		}
	});

	it("applies work order and AI selection replacements", () => {
		const instances: TemplateInstance[] = [
			{
				label: "Main work order",
				viewExternalId: "CogniteActivity",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
			{
				label: "Pick me",
				viewExternalId: "CogniteFile",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		];
		const items = buildCanvasItemsFromTemplate(
			instances,
			[],
			{
				"Pick me": {
					externalId: "sel1",
					space: "sp",
					name: "File",
				},
			},
			asset,
			workOrder,
			config
		);
		const woItem = items.find(
			(i) => i.type === "fdmInstance" && i.label === "Main work order"
		);
		expect(woItem?.type).toBe("fdmInstance");
		if (woItem?.type === "fdmInstance") {
			expect(woItem.instanceExternalId).toBe("wo1");
		}
		const sel = items.find(
			(i) => i.type === "fdmInstance" && i.instanceExternalId === "sel1"
		);
		expect(sel?.type).toBe("fdmInstance");
		if (sel?.type === "fdmInstance") {
			expect(sel.label).toBe("File");
		}
	});

	it("C15: two slots labelled P&ID keep separate picks", () => {
		const instances: TemplateInstance[] = [
			{
				id: "item-a",
				label: "P&ID",
				viewExternalId: "CogniteFile",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
			{
				id: "item-b",
				label: "P&ID",
				viewExternalId: "CogniteFile",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		];
		const items = buildCanvasItemsFromTemplate(
			instances,
			[],
			{
				"item-a": { externalId: "file-a", space: "sp", name: "A" },
				"item-b": { externalId: "file-b", space: "sp", name: "B" },
			},
			asset,
			workOrder,
			config
		);
		const ids = items
			.filter((item) => item.type === "fdmInstance")
			.map((item) =>
				item.type === "fdmInstance" ? item.instanceExternalId : ""
			);
		expect(ids).toEqual(["file-a", "file-b"]);
	});

	it("omits unselected resource slots so the original template item is not kept", () => {
		const instances: TemplateInstance[] = [
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				instanceExternalId: "template-asset",
				instanceSpace: "os",
				...tmpl,
			},
			{
				label: "P&ID",
				viewExternalId: "CogniteFile",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				instanceExternalId: "template-file",
				instanceSpace: "os",
				...tmpl,
			},
			{
				label: "Sensor",
				viewExternalId: "CogniteTimeSeries",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				instanceExternalId: "template-ts",
				instanceSpace: "os",
				...tmpl,
			},
		];
		const items = buildCanvasItemsFromTemplate(
			instances,
			[],
			{ "P&ID": null },
			asset,
			workOrder,
			config
		);
		const fdmItems = items.filter((i) => i.type === "fdmInstance");
		expect(fdmItems).toHaveLength(1);
		expect(fdmItems[0]).toMatchObject({
			type: "fdmInstance",
			instanceExternalId: "a1",
			label: "Main asset",
		});
	});

	it("keeps auto template items that are not selectable resource slots", () => {
		const instances: TemplateInstance[] = [
			{
				label: "Legend",
				viewExternalId: "CustomCanvasNote",
				viewSpace: "my_space",
				viewVersion: "v1",
				instanceExternalId: "legend-1",
				instanceSpace: "os",
				...tmpl,
			},
		];
		const items = buildCanvasItemsFromTemplate(
			instances,
			[],
			{},
			asset,
			workOrder,
			config
		);
		expect(items).toHaveLength(1);
		expect(items[0]).toMatchObject({
			type: "fdmInstance",
			instanceExternalId: "legend-1",
			label: "Legend",
		});
	});
});

describe("generateCanvasLink / extractClusterFromUrl", () => {
	it("builds fusion URL", () => {
		const url = generateCanvasLink("cid", "proj", "westeurope-1", "acme");
		expect(url).toContain("acme.fusion.cognite.com");
		expect(url).toContain("workspace=industrial-tools");
		expect(url).toContain("canvasId=cid");
		expect(url).toContain("cluster=westeurope-1.cognitedata.com");
	});

	it("C35: encodes canvas link parts", () => {
		const url = generateCanvasLink(
			"id/a b",
			"proj ect",
			"west europe-1",
			"acme"
		);
		expect(url).toContain("/proj%20ect/industrial-canvas/canvas?");
		expect(url).toContain("canvasId=id%2Fa%20b");
		expect(url).toContain("cluster=west%20europe-1.cognitedata.com");
		expect(url).toContain("workspace=industrial-tools");
	});

	it("extracts cluster from base URL", () => {
		expect(extractClusterFromUrl("https://foo.cognitedata.com")).toBe("foo");
	});
});

describe("validateCanvasCreation", () => {
	let retrieve: ReturnType<typeof vi.fn>;
	let query: ReturnType<typeof vi.fn>;
	let client: CogniteClient;

	beforeEach(() => {
		retrieve = vi.fn();
		query = vi.fn();
		client = cdfClientTestDouble({
			instances: { retrieve, query, upsert: vi.fn(), delete: vi.fn() },
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
		});
	});

	it("returns success when counts match", async () => {
		retrieve.mockResolvedValueOnce({ items: [{ externalId: "c" }] });
		query.mockResolvedValueOnce({
			items: { nodes: [{}, {}] },
			nextCursor: {},
		});
		const r = await validateCanvasCreation(client, config, "c", 2);
		expect(r.success).toBe(true);
		expect(r.itemsCount).toBe(2);
	});

	it("aggregates paginated instance counts", async () => {
		retrieve.mockResolvedValueOnce({ items: [{ externalId: "c" }] });
		query
			.mockResolvedValueOnce({
				items: { nodes: [{}] },
				nextCursor: { nodes: "n" },
			})
			.mockResolvedValueOnce({
				items: { nodes: [{}] },
				nextCursor: {},
			});
		const r = await validateCanvasCreation(client, config, "c", 2);
		expect(r.itemsCount).toBe(2);
	});

	it("stops counting instance pages when count already exceeds expected", async () => {
		retrieve.mockResolvedValueOnce({ items: [{ externalId: "c" }] });
		query.mockResolvedValueOnce({
			items: { nodes: [{}, {}, {}, {}, {}] },
			nextCursor: { nodes: "would_fetch_more" },
		});
		const r = await validateCanvasCreation(client, config, "c", 2);
		expect(r.itemsCount).toBe(5);
		expect(r.success).toBe(false);
		expect(retrieve).toHaveBeenCalledTimes(1);
		expect(query).toHaveBeenCalledTimes(1);
	});

	it("returns errors when canvas missing", async () => {
		retrieve.mockResolvedValue({ items: [] });
		query.mockResolvedValue({ items: { nodes: [] }, nextCursor: {} });
		const r = await validateCanvasCreation(client, config, "missing", 0);
		expect(r.success).toBe(false);
		expect(r.errors.some((e) => e.includes("not found"))).toBe(true);
	});

	it("handles validation exception", async () => {
		retrieve.mockRejectedValueOnce(new Error("boom"));
		const r = await validateCanvasCreation(client, config, "c", 1);
		expect(r.success).toBe(false);
		expect(r.errors[0]).toContain("Validation error");
	});
});

function canvasRootProperties(upsert: ReturnType<typeof vi.fn>) {
	for (const call of upsert.mock.calls) {
		const items = (
			call[0] as {
				items?: Array<{
					sources?: Array<{ properties?: Record<string, unknown> }>;
				}>;
			}
		).items;
		for (const item of items ?? []) {
			const props = item.sources?.[0]?.properties;
			if (
				props &&
				"visibility" in props &&
				("createdBy" in props || "updatedBy" in props)
			) {
				return props;
			}
		}
	}
	return undefined;
}

describe("createCanvasFromItems", () => {
	it("upserts a public canvas owned by the current user with the Work Package tag", async () => {
		const upsert = vi.fn().mockResolvedValue(undefined);
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				delete: vi.fn(),
				retrieve: vi
					.fn()
					.mockResolvedValue({ items: [{ externalId: "wpg-work-package" }] }),
			},
			profiles: {
				me: vi.fn().mockResolvedValue({
					userIdentifier: "principal-123",
					displayName: "Unknown",
				}),
			},
			getBaseUrl: () => "https://x",
		});

		const items = buildCanvasItemsFromTemplate(
			[
				{
					label: "Main asset",
					viewExternalId: "CogniteAsset",
					viewSpace: "cdf_cdm",
					viewVersion: "v1",
					...tmpl,
				},
			],
			[],
			{},
			{ assetExternalId: "a", space: "s" },
			{ externalId: "w", space: "s" },
			config
		);

		const id = await createCanvasFromItems(client, config, "My", items);
		expect(id).toBe("uuid-fixed");
		expect(upsert.mock.calls.length).toBeGreaterThanOrEqual(2);

		const props = canvasRootProperties(upsert);
		expect(props?.visibility).toBe("public");
		expect(props?.createdBy).toBe("principal-123");
		expect(props?.updatedBy).toBe("principal-123");
		expect(props?.solutionTags).toEqual([workPackageSolutionTagInstanceRef()]);
	});

	it("C09: deletes the root, items, and edges already written when a later create batch fails", async () => {
		const upsert = vi
			.fn()
			.mockResolvedValueOnce(undefined)
			.mockResolvedValueOnce(undefined)
			.mockRejectedValueOnce(new Error("edge write failed"));
		const deleteFn = vi.fn().mockResolvedValue(undefined);
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				delete: deleteFn,
				retrieve: vi
					.fn()
					.mockResolvedValue({ items: [{ externalId: "wpg-work-package" }] }),
			},
			profiles: {
				me: vi.fn().mockResolvedValue({ userIdentifier: "principal-123" }),
			},
			getBaseUrl: () => "https://x",
		});

		await expect(
			createCanvasFromItems(
				client,
				config,
				"My",
				buildCanvasItemsFromTemplate(
					[
						{
							label: "Main asset",
							viewExternalId: "CogniteAsset",
							viewSpace: "cdf_cdm",
							viewVersion: "v1",
							...tmpl,
						},
					],
					[],
					{},
					{ assetExternalId: "a", space: "s" },
					{ externalId: "w", space: "s" },
					config
				)
			)
		).rejects.toThrow(/edge write failed/);

		const deletedIds = deleteFn.mock.calls.flatMap((call) => {
			const batch = call[0] as Array<{ externalId: string }>;
			return batch.map((item) => item.externalId);
		});
		expect(deletedIds).toContain("uuid-fixed");
		expect(deletedIds.some((id) => id.startsWith("uuid-fixed_"))).toBe(true);
	});

	it("C09: surfaces both errors when a create batch fails and rollback also fails", async () => {
		const upsert = vi
			.fn()
			.mockResolvedValueOnce(undefined)
			.mockResolvedValueOnce(undefined)
			.mockRejectedValueOnce(new Error("edge write failed"));
		const deleteFn = vi
			.fn()
			.mockRejectedValue(new Error("rollback delete failed"));
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				delete: deleteFn,
				retrieve: vi
					.fn()
					.mockResolvedValue({ items: [{ externalId: "wpg-work-package" }] }),
			},
			profiles: {
				me: vi.fn().mockResolvedValue({ userIdentifier: "principal-123" }),
			},
			getBaseUrl: () => "https://x",
		});

		await expect(
			createCanvasFromItems(
				client,
				config,
				"My",
				buildCanvasItemsFromTemplate(
					[
						{
							label: "Main asset",
							viewExternalId: "CogniteAsset",
							viewSpace: "cdf_cdm",
							viewVersion: "v1",
							...tmpl,
						},
					],
					[],
					{},
					{ assetExternalId: "a", space: "s" },
					{ externalId: "w", space: "s" },
					config
				)
			)
		).rejects.toThrow(/edge write failed[\s\S]*rollback delete failed/);
	});

	it("uses profile fallbacks for user id", async () => {
		const upsert = vi.fn().mockResolvedValue(undefined);
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				delete: vi.fn(),
				retrieve: vi
					.fn()
					.mockResolvedValue({ items: [{ externalId: "wpg-work-package" }] }),
			},
			profiles: {
				me: vi.fn().mockRejectedValue(new Error("no profile")),
			},
			getBaseUrl: () => "https://x",
		});

		await createCanvasFromItems(
			client,
			config,
			"N",
			buildCanvasItemsFromTemplate(
				[
					{
						label: "Main asset",
						viewExternalId: "CogniteAsset",
						viewSpace: "cdf_cdm",
						viewVersion: "v1",
						...tmpl,
					},
				],
				[],
				{},
				{ assetExternalId: "a", space: "s" },
				{ externalId: "w", space: "s" },
				config
			)
		);
		expect(upsert).toHaveBeenCalled();
	});
});

describe("createCanvasFromTemplateAndSelections / updateCanvasFromTemplateAndSelections", () => {
	beforeEach(() => {
		getTemplateCanvasInstances.mockReset();
		getTemplateCanvasAnnotations.mockReset();
	});

	it("throws when no template instances", async () => {
		getTemplateCanvasInstances.mockResolvedValue([]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const client = cdfClientTestDouble({
			instances: { upsert: vi.fn(), list: vi.fn(), delete: vi.fn() },
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
		});

		await expect(
			createCanvasFromTemplateAndSelections(
				client,
				config,
				"n",
				"tid",
				{},
				{ assetExternalId: "a", space: "s" },
				{ externalId: "w", space: "s" },
				undefined,
				undefined,
				templateReads
			)
		).rejects.toThrow("No template instances");
	});

	it("C10: remaps annotation anchors onto copied item ids and drops missing ones", async () => {
		let nextId = 0;
		vi.mocked(generateUuid).mockImplementation(() => `id-${nextId++}`);
		getTemplateCanvasInstances.mockResolvedValue([
			{
				id: "old-asset",
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
			{
				id: "old-diagram",
				label: "Diagram",
				viewExternalId: "NotAResource",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
			{
				id: "old-file",
				label: "P&ID",
				viewExternalId: "CogniteFile",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([
			{
				id: "old-arrow",
				type: "polylineAnnotation",
				containerId: "old-asset",
				properties: {
					fromId: "old-asset",
					toId: "old-diagram",
					vertices: [{ x: 0, y: 0 }],
				},
			},
			{
				id: "old-note",
				type: "note",
				containerId: "old-file",
				properties: { fromId: "old-file", toId: "missing-item", text: "x" },
			},
		]);

		const upsert = vi.fn().mockResolvedValue(undefined);
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn().mockResolvedValue({ items: [], nextCursor: undefined }),
				query: vi.fn().mockResolvedValue({
					items: { nodes: [], edges: [] },
					nextCursor: {},
				}),
				delete: vi.fn(),
				retrieve: vi
					.fn()
					.mockResolvedValue({ items: [{ externalId: "wpg-work-package" }] }),
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		try {
			await createCanvasFromTemplateAndSelections(
				client,
				config,
				"Canvas",
				"tpl",
				{},
				{ assetExternalId: "a", space: "s" },
				{ externalId: "w", space: "s" },
				undefined,
				undefined,
				templateReads
			);

			const written = upsert.mock.calls.flatMap(
				(call) =>
					(
						call[0] as {
							items: Array<{
								sources?: Array<{ properties: Record<string, unknown> }>;
							}>;
						}
					).items
			);
			const annotations = written.filter(
				(item) => item.sources?.[0]?.properties.annotationType
			);
			const instances = written.filter(
				(item) =>
					item.sources?.[0]?.properties.containerReferenceType === "fdmInstance"
			);
			const assetId = instances.find(
				(item) => item.sources?.[0]?.properties.instanceExternalId === "a"
			)?.sources?.[0]?.properties.id;
			const diagramId = instances.find(
				(item) =>
					item.sources?.[0]?.properties.viewExternalId === "NotAResource"
			)?.sources?.[0]?.properties.id;

			const arrow = annotations.find(
				(item) =>
					item.sources?.[0]?.properties.annotationType === "polylineAnnotation"
			)?.sources?.[0]?.properties;
			const note = annotations.find(
				(item) => item.sources?.[0]?.properties.annotationType === "note"
			)?.sources?.[0]?.properties;
			const arrowProps = arrow?.properties as Record<string, unknown>;
			const noteProps = note?.properties as Record<string, unknown>;

			expect(arrow?.containerId).toBe(assetId);
			expect(arrowProps.fromId).toBe(assetId);
			expect(arrowProps.toId).toBe(diagramId);
			expect(note?.containerId).toBeUndefined();
			expect(noteProps.fromId).toBeUndefined();
			expect(noteProps.toId).toBeUndefined();
			expect(noteProps.text).toBe("x");
		} finally {
			vi.mocked(generateUuid).mockReturnValue("uuid-fixed");
		}
	});

	it("creates canvas from template data", async () => {
		getTemplateCanvasInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const upsert = vi.fn().mockResolvedValue(undefined);
		const list = vi
			.fn()
			.mockResolvedValue({ items: [], nextCursor: undefined });
		const query = vi.fn().mockResolvedValue({
			items: { nodes: [], edges: [] },
			nextCursor: {},
		});
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list,
				query,
				delete: vi.fn(),
				retrieve: vi
					.fn()
					.mockResolvedValue({ items: [{ externalId: "wpg-work-package" }] }),
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		const id = await createCanvasFromTemplateAndSelections(
			client,
			config,
			"Canvas",
			"tpl",
			{},
			{ assetExternalId: "a", space: "s" },
			{ externalId: "w", space: "s" },
			undefined,
			undefined,
			templateReads
		);
		expect(id).toBe("uuid-fixed");
	});

	it("routes sentinel id to base template builder without fetching template", async () => {
		const upsert = vi.fn().mockResolvedValue(undefined);
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				delete: vi.fn(),
				retrieve: vi
					.fn()
					.mockResolvedValue({ items: [{ externalId: "wpg-work-package" }] }),
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		const id = await createCanvasFromTemplateAndSelections(
			client,
			config,
			"Canvas",
			BASE_TEMPLATE_ID,
			{},
			{ assetExternalId: "a", space: "s", assetName: "Asset" },
			{ externalId: "w", space: "s", name: "WO" },
			{
				file: [{ externalId: "f1", space: "s", properties: {} }],
			},
			undefined,
			templateReads
		);

		expect(id).toBe("uuid-fixed");
		expect(getTemplateCanvasInstances).not.toHaveBeenCalled();
		expect(upsert).toHaveBeenCalled();
	});

	it("throws a resource-focused error when base template has no items", async () => {
		const client = cdfClientTestDouble({
			instances: { upsert: vi.fn(), list: vi.fn(), delete: vi.fn() },
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
		});

		await expect(
			createCanvasFromTemplateAndSelections(
				client,
				config,
				"Canvas",
				BASE_TEMPLATE_ID,
				{},
				{ assetExternalId: "a", space: "s", assetName: "Asset" },
				{ externalId: "w", space: "s", name: "WO" },
				{},
				undefined,
				templateReads
			)
		).rejects.toThrow(
			"No resources found to place on the canvas. Please add or search for resources first."
		);
		expect(getTemplateCanvasInstances).not.toHaveBeenCalled();
	});
});

describe("updateCanvasFromTemplateAndSelections", () => {
	beforeEach(() => {
		getTemplateCanvasInstances.mockReset();
		getTemplateCanvasAnnotations.mockReset();
	});

	it("C09: writes new items before deleting the previous ones", async () => {
		getTemplateCanvasInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const calls: string[] = [];
		const upsert = vi.fn().mockImplementation(async () => {
			calls.push("upsert");
		});
		const list = vi
			.fn()
			.mockResolvedValue({ items: [], nextCursor: undefined });
		const query = vi.fn().mockImplementation(async () => {
			calls.push("query");
			return {
				items: {
					nodes: [
						{
							externalId: "existing-id_old",
							space: config.canvasSpaceInstance,
						},
					],
					edges: [
						{
							externalId: "existing-id_existing-id_old",
							space: config.canvasSpaceInstance,
						},
					],
				},
				nextCursor: {},
			};
		});
		const deleteFn = vi.fn().mockImplementation(async () => {
			calls.push("delete");
		});
		const retrieve = vi
			.fn()
			.mockImplementation(
				async (request: { items?: Array<{ externalId?: string }> }) => {
					const externalId = request.items?.[0]?.externalId;
					if (externalId === "existing-id") {
						return {
							items: [
								{
									externalId,
									space: config.canvasSpaceInstance,
									properties: {
										cdf_industrial_canvas: {
											"Canvas/v7": {
												visibility: "private",
												createdBy: "owner-1",
												solutionTags: [
													{
														space: "SolutionTagsInstanceSpace",
														externalId: "user-label",
													},
												],
											},
										},
									},
								},
							],
						};
					}
					return { items: [{ externalId }] };
				}
			);
		const client = cdfClientTestDouble({
			instances: { upsert, list, query, delete: deleteFn, retrieve },
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		const id = await updateCanvasFromTemplateAndSelections(
			client,
			config,
			"existing-id",
			"Renamed",
			"tpl",
			{},
			{ assetExternalId: "a", space: "s" },
			{ externalId: "w", space: "s" },
			{},
			undefined,
			templateReads
		);
		expect(id).toBe("existing-id");
		const firstDelete = calls.indexOf("delete");
		const firstUpsert = calls.indexOf("upsert");
		expect(firstUpsert).toBeGreaterThanOrEqual(0);
		expect(firstDelete).toBeGreaterThan(firstUpsert);
		const deletedIds = deleteFn.mock.calls.flatMap((call) => {
			const batch = call[0] as Array<{ externalId: string }>;
			return batch.map((item) => item.externalId);
		});
		expect(deletedIds).toContain("existing-id_old");
		expect(deletedIds).not.toContain("existing-id");
	});

	it("C09: keeps the previous canvas when writing the replacement fails", async () => {
		getTemplateCanvasInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const upsert = vi.fn().mockRejectedValue(new Error("write failed"));
		const deleteFn = vi.fn().mockResolvedValue(undefined);
		const query = vi.fn().mockResolvedValue({
			items: {
				nodes: [
					{
						externalId: "existing-id_old",
						space: config.canvasSpaceInstance,
					},
				],
				edges: [],
			},
			nextCursor: {},
		});
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				query,
				delete: deleteFn,
				retrieve: vi.fn().mockResolvedValue({ items: [{ externalId: "tag" }] }),
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		await expect(
			updateCanvasFromTemplateAndSelections(
				client,
				config,
				"existing-id",
				"Renamed",
				"tpl",
				{},
				{ assetExternalId: "a", space: "s" },
				{ externalId: "w", space: "s" },
				{},
				undefined,
				templateReads
			)
		).rejects.toThrow(/write failed/);
		const deletedIds = deleteFn.mock.calls.flatMap((call) => {
			const batch = call[0] as Array<{ externalId: string }>;
			return batch.map((item) => item.externalId);
		});
		expect(deletedIds).not.toContain("existing-id_old");
	});

	it("C09: rolls back new items and throws when deleting previous children fails", async () => {
		getTemplateCanvasInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const upsert = vi.fn().mockResolvedValue(undefined);
		const deleteFn = vi
			.fn()
			.mockRejectedValueOnce(new Error("old child delete failed"))
			.mockResolvedValue(undefined);
		const query = vi.fn().mockResolvedValue({
			items: {
				nodes: [
					{
						externalId: "existing-id_old",
						space: config.canvasSpaceInstance,
					},
				],
				edges: [],
			},
			nextCursor: {},
		});
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				query,
				delete: deleteFn,
				retrieve: vi.fn().mockResolvedValue({ items: [{ externalId: "tag" }] }),
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		await expect(
			updateCanvasFromTemplateAndSelections(
				client,
				config,
				"existing-id",
				"Renamed",
				"tpl",
				{},
				{ assetExternalId: "a", space: "s" },
				{ externalId: "w", space: "s" },
				{},
				undefined,
				templateReads
			)
		).rejects.toThrow(/old child delete failed/);

		const rollbackIds = deleteFn.mock.calls.slice(1).flatMap((call) => {
			const batch = call[0] as Array<{ externalId: string }>;
			return batch.map((item) => item.externalId);
		});
		expect(rollbackIds).toContain("existing-id_uuid-fixed");
		expect(rollbackIds).toContain("existing-id_existing-id_uuid-fixed");
		expect(rollbackIds).not.toContain("existing-id_old");
		expect(canvasRootProperties(upsert)).toBeUndefined();
	});

	it("C09: reports both errors when deleting previous children and rolling back new items both fail", async () => {
		getTemplateCanvasInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const upsert = vi.fn().mockResolvedValue(undefined);
		const deleteFn = vi
			.fn()
			.mockRejectedValueOnce(new Error("old child delete failed"))
			.mockRejectedValueOnce(new Error("rollback failed"));
		const query = vi.fn().mockResolvedValue({
			items: {
				nodes: [
					{
						externalId: "existing-id_old",
						space: config.canvasSpaceInstance,
					},
				],
				edges: [],
			},
			nextCursor: {},
		});
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				query,
				delete: deleteFn,
				retrieve: vi.fn().mockResolvedValue({ items: [{ externalId: "tag" }] }),
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		await expect(
			updateCanvasFromTemplateAndSelections(
				client,
				config,
				"existing-id",
				"Renamed",
				"tpl",
				{},
				{ assetExternalId: "a", space: "s" },
				{ externalId: "w", space: "s" },
				{},
				undefined,
				templateReads
			)
		).rejects.toThrow(/old child delete failed[\s\S]*rollback failed/);
	});

	it("C09: keeps the new items if the final root upsert fails after old children are deleted", async () => {
		getTemplateCanvasInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const upsert = vi
			.fn()
			.mockResolvedValueOnce(undefined)
			.mockResolvedValueOnce(undefined)
			.mockRejectedValueOnce(new Error("root update failed"));
		const deleteFn = vi.fn().mockResolvedValue(undefined);
		const query = vi.fn().mockResolvedValue({
			items: {
				nodes: [
					{
						externalId: "existing-id_old",
						space: config.canvasSpaceInstance,
					},
				],
				edges: [],
			},
			nextCursor: {},
		});
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				query,
				delete: deleteFn,
				retrieve: vi.fn().mockResolvedValue({ items: [{ externalId: "tag" }] }),
			},
			profiles: { me: vi.fn().mockResolvedValue({ displayName: "u" }) },
			getBaseUrl: () => "https://x",
		});

		await expect(
			updateCanvasFromTemplateAndSelections(
				client,
				config,
				"existing-id",
				"Renamed",
				"tpl",
				{},
				{ assetExternalId: "a", space: "s" },
				{ externalId: "w", space: "s" },
				{},
				undefined,
				templateReads
			)
		).rejects.toThrow(/root update failed/);

		const deletedIds = deleteFn.mock.calls.flatMap((call) => {
			const batch = call[0] as Array<{ externalId: string }>;
			return batch.map((item) => item.externalId);
		});
		expect(deletedIds).toEqual(["existing-id_old"]);
	});

	it("C18: keeps visibility and merges solution tags on update", async () => {
		getTemplateCanvasInstances.mockResolvedValue([
			{
				label: "Main asset",
				viewExternalId: "CogniteAsset",
				viewSpace: "cdf_cdm",
				viewVersion: "v1",
				...tmpl,
			},
		]);
		getTemplateCanvasAnnotations.mockResolvedValue([]);

		const upsert = vi.fn().mockResolvedValue(undefined);
		const query = vi.fn().mockResolvedValue({
			items: { nodes: [], edges: [] },
			nextCursor: {},
		});
		const retrieve = vi
			.fn()
			.mockImplementation(
				async (request: { items?: Array<{ externalId?: string }> }) => {
					const externalId = request.items?.[0]?.externalId;
					if (externalId === "existing-id") {
						return {
							items: [
								{
									externalId,
									space: config.canvasSpaceInstance,
									properties: {
										cdf_industrial_canvas: {
											"Canvas/v7": {
												visibility: "private",
												solutionTags: [
													{
														space: "SolutionTagsInstanceSpace",
														externalId: "user-label",
													},
												],
											},
										},
									},
								},
							],
						};
					}
					return { items: [{ externalId }] };
				}
			);
		const client = cdfClientTestDouble({
			instances: {
				upsert,
				list: vi.fn(),
				query,
				delete: vi.fn().mockResolvedValue(undefined),
				retrieve,
			},
			profiles: { me: vi.fn().mockResolvedValue({ userIdentifier: "u" }) },
			getBaseUrl: () => "https://x",
		});

		await updateCanvasFromTemplateAndSelections(
			client,
			config,
			"existing-id",
			"Renamed",
			"tpl",
			{},
			{ assetExternalId: "a", space: "s" },
			{ externalId: "w", space: "s" },
			{},
			undefined,
			templateReads
		);

		const root = canvasRootProperties(upsert);
		expect(root?.visibility).toBe("private");
		expect(root?.solutionTags).toEqual([
			{ space: "SolutionTagsInstanceSpace", externalId: "user-label" },
			workPackageSolutionTagInstanceRef(),
		]);
	});
});
