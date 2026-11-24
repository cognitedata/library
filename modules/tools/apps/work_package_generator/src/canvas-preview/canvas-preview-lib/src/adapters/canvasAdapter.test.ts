import type { CogniteClient, ViewDefinition } from "@cognite/sdk";
import { describe, expect, it, vi } from "vitest";

import type {
	CanvasAnnotation,
	FdmInstanceContainerReference,
} from "../canvasQueries";

import {
	aggregateCanvasNodes,
	annotationToNode,
	fdmContainerToNode,
} from "./canvasAdapter";

function resolved(externalId: string): ViewDefinition {
	return {
		space: "cdf_cdm",
		externalId,
		version: "v1",
		createdTime: 1,
		lastUpdatedTime: 1,
		writable: true,
		usedFor: "node",
		isGlobal: false,
		properties: {},
	} as ViewDefinition;
}

function container(
	viewExternalId: string,
	extra: Record<string, unknown> = {}
): FdmInstanceContainerReference {
	return {
		space: "canvas",
		externalId: `node-${viewExternalId}`,
		properties: {
			containerReferenceType: "fdmInstance",
			instanceExternalId: "inst",
			instanceSpace: "src",
			viewExternalId,
			viewSpace: "cdf_cdm",
			viewVersion: "v1",
			label: "Pump",
			x: 12,
			y: 8,
			width: 40,
			height: 30,
			properties: extra,
		},
	};
}

describe("fdmContainerToNode", () => {
	it("builds a timeseries node", () => {
		const node = fdmContainerToNode(
			container("CogniteTimeSeries", {
				startDate: "2026-01-01",
				color: "#fff",
				minY: 0,
				maxY: 10,
			}),
			0,
			resolved("CogniteTimeSeries")
		);
		expect(node.type).toBe("timeseries");
		expect(node.position).toEqual({ x: 12, y: 8 });
		expect(node.data).toMatchObject({ color: "#fff", label: "Pump" });
	});

	it("builds file and table nodes", () => {
		expect(
			fdmContainerToNode(container("CogniteFile"), 0, resolved("CogniteFile"))
				.type
		).toBe("pdf");
		expect(
			fdmContainerToNode(container("CogniteAsset"), 0, resolved("CogniteAsset"))
				.type
		).toBe("table");
		expect(
			fdmContainerToNode(
				container("CogniteEquipment"),
				0,
				resolved("CogniteEquipment")
			).type
		).toBe("table");
		expect(
			fdmContainerToNode(
				container("CogniteActivity"),
				0,
				resolved("CogniteActivity")
			).type
		).toBe("table");
		expect(fdmContainerToNode(container("CustomThing")).type).toBe("table");
	});

	it("falls back to a table node for 3D model instances", () => {
		const model = fdmContainerToNode(
			container("Cognite3DModel"),
			0,
			resolved("Cognite3DModel")
		);
		const revision = fdmContainerToNode(
			container("Cognite3DRevision"),
			0,
			resolved("Cognite3DRevision")
		);
		expect(model.type).toBe("table");
		expect(revision.type).toBe("table");
		expect(model.data).toMatchObject({ title: "3DModel - Pump" });
	});

	it("uses defaults and a node type override", () => {
		const bare = fdmContainerToNode({
			space: "canvas",
			externalId: "bare",
			properties: {
				containerReferenceType: "fdmInstance",
				instanceExternalId: "inst",
				instanceSpace: "src",
				viewExternalId: "CogniteFile",
				viewSpace: "cdf_cdm",
			},
		});
		expect(bare.position).toEqual({ x: 0, y: 0 });
		expect(bare.size).toEqual({ width: 200, height: 150 });
		expect(
			fdmContainerToNode(container("CogniteFile"), 3, undefined, "image").type
		).toBe("image");
	});
});

describe("annotationToNode", () => {
	it("maps a text annotation and its parent", () => {
		const annotation: CanvasAnnotation = {
			space: "canvas",
			externalId: "ann",
			properties: {
				id: "ann",
				annotationType: "textAnnotation",
				containerId: "parent",
				properties: { text: "Hi", x: 4, y: 5, style: { fontSize: 12 } },
			},
		};
		const node = annotationToNode(annotation, 2);
		expect(node.type).toBe("text");
		expect(node.parentId).toBe("parent");
		expect(node.position).toEqual({ x: 4, y: 5 });
	});
});

describe("aggregateCanvasNodes", () => {
	it("aggregates containers and annotations without a client", async () => {
		const nodes = await aggregateCanvasNodes({
			fdmContainers: [container("CogniteTimeSeries")],
			annotations: [
				{
					space: "canvas",
					externalId: "ann",
					properties: {
						id: "ann",
						annotationType: "stickyAnnotation",
						properties: { text: "note" },
					},
				},
			],
		});
		expect(nodes.map((node) => node.type)).toEqual(["table", "sticky"]);
	});

	it("overrides a file node type from the files API", async () => {
		const sdk = {
			views: {
				retrieve: vi.fn(async () => ({ items: [resolved("CogniteFile")] })),
			},
			files: {
				retrieve: vi.fn(async () => [{ mimeType: "image/png", name: "a.png" }]),
			},
		} as object as CogniteClient;
		const nodes = await aggregateCanvasNodes({
			fdmContainers: [container("CogniteFile")],
			annotations: [],
			client: sdk,
		});
		expect(nodes[0]?.type).toBe("image");
	});

	it("keeps the default file type when lookup fails", async () => {
		const sdk = {
			views: { retrieve: vi.fn(async () => Promise.reject(new Error("no"))) },
			files: { retrieve: vi.fn(async () => Promise.reject(new Error("no"))) },
		} as object as CogniteClient;
		const nodes = await aggregateCanvasNodes({
			fdmContainers: [container("CogniteFile")],
			annotations: [],
			client: sdk,
		});
		expect(nodes[0]?.type).toBe("table");
	});
});
