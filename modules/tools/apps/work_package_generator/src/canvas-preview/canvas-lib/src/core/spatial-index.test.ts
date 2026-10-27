import { describe, expect, it } from "vitest";

import { boundsIntersect, nodeToBounds, pointInBounds } from "./spatial-index";
import type { CanvasNode } from "./types";

describe("spatial-index", () => {
	it("detects overlap and a miss", () => {
		const a = { x: 0, y: 0, width: 10, height: 10 };
		expect(boundsIntersect(a, { x: 5, y: 5, width: 2, height: 2 })).toBe(true);
		expect(boundsIntersect(a, { x: 10, y: 0, width: 2, height: 2 })).toBe(
			false
		);
	});

	it("tests a point on the edge and outside", () => {
		const bounds = { x: 1, y: 1, width: 4, height: 4 };
		expect(pointInBounds({ x: 5, y: 5 }, bounds)).toBe(true);
		expect(pointInBounds({ x: 0, y: 1 }, bounds)).toBe(false);
	});

	it("reads bounds from a node", () => {
		const node = {
			id: "n",
			type: "text",
			position: { x: 2, y: 3 },
			size: { width: 8, height: 9 },
			zIndex: 0,
			data: {},
		} satisfies CanvasNode;
		expect(nodeToBounds(node)).toEqual({ x: 2, y: 3, width: 8, height: 9 });
	});
});
