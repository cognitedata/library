import { describe, expect, it } from "vitest";

import {
	boxSelect,
	hitTestNode,
	hitTestNodes,
	hitTestTopNode,
} from "./hit-testing";
import type { CanvasNode } from "./types";

function node(
	overrides: Partial<CanvasNode> & Pick<CanvasNode, "id">
): CanvasNode {
	return {
		type: "text",
		position: { x: 0, y: 0 },
		size: { width: 100, height: 80 },
		zIndex: 0,
		data: {},
		...overrides,
	};
}

describe("hitTestNode", () => {
	const target = node({ id: "a" });

	it("hits corner and edge handles before the body", () => {
		expect(hitTestNode({ x: 0, y: 0 }, target)?.area).toBe("resize-nw");
		expect(hitTestNode({ x: 100, y: 0 }, target)?.area).toBe("resize-ne");
		expect(hitTestNode({ x: 100, y: 80 }, target)?.area).toBe("resize-se");
		expect(hitTestNode({ x: 0, y: 80 }, target)?.area).toBe("resize-sw");
		expect(hitTestNode({ x: 50, y: 0 }, target)?.area).toBe("resize-n");
		expect(hitTestNode({ x: 100, y: 40 }, target)?.area).toBe("resize-e");
		expect(hitTestNode({ x: 50, y: 80 }, target)?.area).toBe("resize-s");
		expect(hitTestNode({ x: 0, y: 40 }, target)?.area).toBe("resize-w");
	});

	it("ignores edge handles when only corners are active", () => {
		expect(hitTestNode({ x: 50, y: 0 }, target, 1, undefined, true)?.area).toBe(
			"body"
		);
	});

	it("hits the body and misses empty space", () => {
		expect(hitTestNode({ x: 40, y: 40 }, target)?.area).toBe("body");
		expect(hitTestNode({ x: 200, y: 200 }, target)).toBeNull();
	});

	it("uses absolute bounds for a child", () => {
		const parent = node({
			id: "p",
			position: { x: 100, y: 100 },
			size: { width: 200, height: 200 },
		});
		const child = node({
			id: "c",
			parentId: "p",
			position: { x: 0, y: 0 },
			relativePosition: { x: 0, y: 0 },
			relativeSize: { width: 0.5, height: 0.5 },
		});
		expect(hitTestNode({ x: 10, y: 10 }, child, 1, [parent, child])).toBeNull();
		expect(
			hitTestNode({ x: 150, y: 150 }, child, 1, [parent, child])?.area
		).toBe("body");
	});
});

describe("hitTestNodes", () => {
	it("returns the child before its parent and honors z-index", () => {
		const parent = node({ id: "p", zIndex: 5 });
		const child = node({
			id: "c",
			parentId: "p",
			zIndex: 0,
			position: { x: 10, y: 10 },
			size: { width: 20, height: 20 },
		});
		const results = hitTestNodes({ x: 20, y: 20 }, [parent, child]);
		expect(results[0]?.nodeId).toBe("c");
		expect(hitTestTopNode({ x: 20, y: 20 }, [parent, child])?.nodeId).toBe("c");
		expect(hitTestTopNode({ x: 500, y: 500 }, [parent])).toBeNull();
	});

	it("can restrict a node to corner handles", () => {
		const target = node({ id: "a" });
		const hit = hitTestNodes({ x: 50, y: 0 }, [target], 1, () => true);
		expect(hit[0]?.area).toBe("body");
	});
});

describe("boxSelect", () => {
	it("selects nodes fully inside the box", () => {
		const inside = node({
			id: "in",
			position: { x: 10, y: 10 },
			size: { width: 10, height: 10 },
		});
		const outside = node({
			id: "out",
			position: { x: 80, y: 80 },
			size: { width: 40, height: 40 },
		});
		expect(
			boxSelect({ x: 0, y: 0, width: 40, height: 40 }, [inside, outside])
		).toEqual(["in"]);
	});
});
