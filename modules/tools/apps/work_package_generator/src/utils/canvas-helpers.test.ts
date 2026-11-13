import { describe, expect, it, vi } from "vitest";
import { DEFAULT_CANVAS_CONFIG } from "@/__mocks__/app-config";
import type { CanvasItem } from "../types";
import {
	buildCanvasNodesAndEdges,
	createCanvasEdge,
	createCanvasNode,
	generateCanvasItemProperties,
} from "./canvas-helpers";

// vi.mock: UUID is non-deterministic; fixed value keeps canvas helper IDs assertable.
vi.mock("./uuid", () => ({
	generateUuid: vi.fn().mockReturnValue("fixed-uuid"),
}));

describe("generateCanvasItemProperties", () => {
	it("returns {} for falsy item", () => {
		expect(generateCanvasItemProperties(null, "id")).toEqual({});
	});

	it("builds fdmInstance props", () => {
		const item: CanvasItem = {
			type: "fdmInstance",
			containerId: "C",
			instanceExternalId: "i",
			instanceSpace: "s",
			viewExternalId: "v",
			viewSpace: "vs",
			viewVersion: "v1",
			label: "L",
		};
		const p = generateCanvasItemProperties(item, "nid");
		expect(p.id).toBe("nid");
		expect(p.instanceExternalId).toBe("i");
		expect(p.containerReferenceType).toBe("fdmInstance");
	});

	it("builds CanvasAnnotation props", () => {
		const item: CanvasItem = {
			type: "CanvasAnnotation",
			containerId: "C",
			annotationType: "note",
			properties: { x: 1 },
		};
		const p = generateCanvasItemProperties(item, "nid");
		expect(p.annotationType).toBe("note");
		expect(p.properties).toEqual({ x: 1 });
	});
});

describe("createCanvasNode", () => {
	const config = DEFAULT_CANVAS_CONFIG;

	it("returns null when props empty", () => {
		expect(createCanvasNode("c1", "n1", null, config)).toBeNull();
	});

	it("creates node for fdmInstance", () => {
		const item: CanvasItem = {
			type: "fdmInstance",
			containerId: "Cont",
			instanceExternalId: "i",
			instanceSpace: "s",
			viewExternalId: "v",
			viewSpace: "vs",
			viewVersion: "v1",
		};
		const node = createCanvasNode("canvasX", "uuid1", item, config);
		expect(node?.instanceType).toBe("node");
		expect(node?.externalId).toBe("canvasX_uuid1");
		expect(node?.sources?.[0].source.externalId).toBe("Cont");
	});
});

describe("createCanvasEdge", () => {
	it("builds edge between canvas and node", () => {
		const item: CanvasItem = {
			type: "fdmInstance",
			containerId: "Box",
			instanceExternalId: "i",
			instanceSpace: "s",
			viewExternalId: "v",
			viewSpace: "vs",
			viewVersion: "v1",
		};
		const edge = createCanvasEdge(
			"c1",
			"n1",
			"c1_n1",
			item,
			DEFAULT_CANVAS_CONFIG
		);
		expect(edge.instanceType).toBe("edge");
		expect(edge.type.externalId).toBe("referencesBox");
		expect(edge.startNode.externalId).toBe("c1");
		expect(edge.endNode.externalId).toBe("c1_n1");
	});
});

describe("buildCanvasNodesAndEdges", () => {
	it("skips null items and empty nodes", () => {
		const items = [
			null,
			{
				type: "fdmInstance",
				containerId: "C",
				instanceExternalId: "i",
				instanceSpace: "s",
				viewExternalId: "v",
				viewSpace: "vs",
				viewVersion: "v1",
			} satisfies CanvasItem,
		] as CanvasItem[];
		const { nodes, edges } = buildCanvasNodesAndEdges(
			items,
			"cid",
			DEFAULT_CANVAS_CONFIG
		);
		expect(nodes).toHaveLength(1);
		expect(edges).toHaveLength(1);
	});
});
