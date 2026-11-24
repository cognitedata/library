import { describe, expect, it } from "vitest";
import {
	boundsIntersect,
	distance,
	fitBoundsInView,
	getAbsoluteBounds,
	getAbsolutePosition,
	getAbsoluteSize,
	getAllNodesBounds,
	getBoundsFromPoints,
	getChildren,
	getDescendantIds,
	getDescendants,
	getExtendedBounds,
	getNodeById,
	getViewportBounds,
	getViewportTransform,
	isAncestor,
	isPointInBounds,
	midpoint,
	pan,
	rotatePointAroundCenter,
	screenToCanvas,
	snapToAngle,
	toRelativePosition,
	zoomAtPoint,
} from "./transforms";
import type { CanvasNode, ViewportState } from "./types";

const viewport: ViewportState = { x: 10, y: 20, scale: 2 };

function node(
	overrides: Partial<CanvasNode> & Pick<CanvasNode, "id">
): CanvasNode {
	return {
		type: "text",
		position: { x: 0, y: 0 },
		size: { width: 100, height: 50 },
		zIndex: 0,
		data: {},
		...overrides,
	};
}

describe("screenToCanvas", () => {
	it("maps a screen point through offset and scale", () => {
		expect(screenToCanvas({ x: 30, y: 50 }, viewport, { x: 4, y: 6 })).toEqual({
			x: 8,
			y: 12,
		});
	});
});

describe("zoomAtPoint", () => {
	it("keeps the canvas point under the cursor", () => {
		const next = zoomAtPoint(viewport, { x: 30, y: 40 }, 4);
		const before = screenToCanvas({ x: 30, y: 40 }, viewport);
		const after = screenToCanvas({ x: 30, y: 40 }, next);
		expect(after.x).toBeCloseTo(before.x);
		expect(after.y).toBeCloseTo(before.y);
		expect(next.scale).toBe(4);
	});

	it("returns the same viewport when the scale is already clamped", () => {
		expect(zoomAtPoint(viewport, { x: 0, y: 0 }, 2)).toBe(viewport);
		expect(zoomAtPoint(viewport, { x: 0, y: 0 }, 99).scale).toBe(5);
		expect(zoomAtPoint(viewport, { x: 0, y: 0 }, 0.01).scale).toBe(0.1);
	});
});

describe("pan", () => {
	it("adds the delta to the viewport origin", () => {
		expect(pan(viewport, 5, -3)).toEqual({ x: 15, y: 17, scale: 2 });
	});
});

describe("fitBoundsInView", () => {
	it("returns a unit viewport for empty bounds", () => {
		expect(
			fitBoundsInView(
				{ x: 0, y: 0, width: 0, height: 10 },
				{ width: 800, height: 600 }
			)
		).toEqual({ x: 0, y: 0, scale: 1 });
	});

	it("centers content and clamps the scale", () => {
		const fitted = fitBoundsInView(
			{ x: 0, y: 0, width: 100, height: 100 },
			{ width: 1000, height: 1000 },
			50,
			0.05,
			4
		);
		expect(fitted.scale).toBe(4);
		expect(fitted.x).toBeCloseTo(300);
		expect(fitted.y).toBeCloseTo(300);
	});
});

describe("geometry helpers", () => {
	it("formats the viewport transform", () => {
		expect(getViewportTransform(viewport)).toBe(
			"translate(10px, 20px) scale(2)"
		);
	});

	it("measures distance and midpoint", () => {
		expect(distance({ x: 0, y: 0 }, { x: 3, y: 4 })).toBe(5);
		expect(midpoint({ x: 0, y: 2 }, { x: 4, y: 6 })).toEqual({ x: 2, y: 4 });
	});

	it("snaps a point to 90 degree increments", () => {
		const snapped = snapToAngle({ x: 0, y: 0 }, { x: 3, y: 1 }, 90);
		expect(snapped.x).toBeCloseTo(Math.hypot(3, 1));
		expect(snapped.y).toBeCloseTo(0);
	});

	it("builds bounds from either corner order", () => {
		expect(getBoundsFromPoints({ x: 10, y: 4 }, { x: 2, y: 8 })).toEqual({
			x: 2,
			y: 4,
			width: 8,
			height: 4,
		});
	});

	it("tests points against bounds including the edge", () => {
		const bounds = { x: 0, y: 0, width: 10, height: 10 };
		expect(isPointInBounds({ x: 10, y: 10 }, bounds)).toBe(true);
		expect(isPointInBounds({ x: 11, y: 0 }, bounds)).toBe(false);
	});

	it("returns the original point when the angle is zero", () => {
		const point = { x: 3, y: 4 };
		expect(rotatePointAroundCenter(point, { x: 0, y: 0 }, 0)).toBe(point);
	});

	it("rotates 90 degrees around a center", () => {
		expect(
			rotatePointAroundCenter({ x: 2, y: 0 }, { x: 0, y: 0 }, 90)
		).toMatchObject({ x: expect.closeTo(0), y: expect.closeTo(2) });
	});
});

describe("hierarchy", () => {
	const parent = node({
		id: "parent",
		position: { x: 10, y: 20 },
		size: { width: 200, height: 100 },
		rotation: 0,
	});
	const child = node({
		id: "child",
		parentId: "parent",
		position: { x: 0, y: 0 },
		relativePosition: { x: 0.5, y: 0.25 },
		relativeSize: { width: 0.5, height: 0.5 },
	});
	const grandchild = node({
		id: "grandchild",
		parentId: "child",
		position: { x: 1, y: 1 },
	});
	const orphan = node({
		id: "orphan",
		parentId: "missing",
		relativePosition: { x: 0.1, y: 0.1 },
		relativeSize: { width: 0.2, height: 0.2 },
		position: { x: 7, y: 8 },
		size: { width: 9, height: 11 },
	});
	const nodes = [parent, child, grandchild, orphan];

	it("looks up nodes, children, and descendants", () => {
		expect(getNodeById(nodes, "missing")).toBeUndefined();
		expect(getChildren("parent", nodes).map((item) => item.id)).toEqual([
			"child",
		]);
		expect(getDescendants("parent", nodes).map((item) => item.id)).toEqual([
			"child",
			"grandchild",
		]);
		expect(getDescendantIds("parent", nodes)).toEqual(["child", "grandchild"]);
	});

	it("walks the ancestor chain", () => {
		expect(isAncestor("parent", "grandchild", nodes)).toBe(true);
		expect(isAncestor("parent", "orphan", nodes)).toBe(false);
		expect(isAncestor("parent", "missing", nodes)).toBe(false);
		expect(isAncestor("parent", "parent", nodes)).toBe(false);
	});

	it("resolves absolute position and size through parents", () => {
		expect(getAbsolutePosition(parent, nodes)).toEqual({ x: 10, y: 20 });
		expect(getAbsolutePosition(child, nodes)).toEqual({ x: 110, y: 45 });
		expect(getAbsoluteSize(child, nodes)).toEqual({ width: 100, height: 50 });
		expect(getAbsolutePosition(orphan, nodes)).toEqual({ x: 7, y: 8 });
		expect(getAbsoluteSize(orphan, nodes)).toEqual({ width: 9, height: 11 });
		expect(getAbsoluteBounds(child, nodes)).toEqual({
			x: 110,
			y: 45,
			width: 100,
			height: 50,
		});
	});

	it("rotates a child around the parent center", () => {
		const rotatedParent = { ...parent, rotation: 90 };
		const position = getAbsolutePosition(child, [rotatedParent, child]);
		expect(position.x).toBeCloseTo(135);
		expect(position.y).toBeCloseTo(70);
	});

	it("converts an absolute point into a parent-relative fraction", () => {
		expect(
			toRelativePosition(
				{ x: 50, y: 25 },
				{ x: 0, y: 0, width: 100, height: 50 }
			)
		).toEqual({ x: 0.5, y: 0.5 });
	});
});

describe("viewport culling", () => {
	it("detects intersecting and disjoint bounds", () => {
		const a = { x: 0, y: 0, width: 10, height: 10 };
		expect(boundsIntersect(a, { x: 9, y: 9, width: 2, height: 2 })).toBe(true);
		expect(boundsIntersect(a, { x: 11, y: 0, width: 2, height: 2 })).toBe(
			false
		);
	});

	it("converts the viewport into canvas bounds and extends them", () => {
		expect(getViewportBounds(viewport, { width: 200, height: 100 })).toEqual({
			x: -5,
			y: -10,
			width: 100,
			height: 50,
		});
		expect(
			getExtendedBounds({ x: 10, y: 20, width: 40, height: 10 }, 0.5)
		).toEqual({ x: -10, y: 15, width: 80, height: 20 });
	});

	it("fits root nodes and drops sub-pixel nodes", () => {
		expect(getAllNodesBounds([])).toBeNull();
		expect(
			getAllNodesBounds([node({ id: "child-only", parentId: "gone" })])
		).toBeNull();
		expect(
			getAllNodesBounds([
				node({
					id: "tiny",
					position: { x: 1000, y: 1000 },
					size: { width: 0.2, height: 0.2 },
				}),
				node({
					id: "real",
					position: { x: 10, y: 20 },
					size: { width: 30, height: 40 },
				}),
			])
		).toEqual({ x: 10, y: 20, width: 30, height: 40 });
	});

	it("includes children when asked and falls back when every node is tiny", () => {
		const child = node({
			id: "c",
			parentId: "p",
			position: { x: 5, y: 5 },
			size: { width: 8, height: 8 },
		});
		const parent = node({ id: "p", position: { x: 0, y: 0 } });
		expect(getAllNodesBounds([parent, child], true)?.width).toBeGreaterThan(0);
		const tiny = getAllNodesBounds([
			node({
				id: "dot",
				position: { x: 3, y: 4 },
				size: { width: 0.1, height: 0.1 },
			}),
		]);
		expect(tiny?.x).toBeCloseTo(3);
		expect(tiny?.width).toBeCloseTo(0.1);
	});
});
