import { describe, expect, it } from "vitest";

import { generateId, getNodeById } from "./state";
import type { CanvasNode, CanvasState } from "./types";

describe("state helpers", () => {
	it("finds a node by id", () => {
		const node = {
			id: "a",
			type: "text",
			position: { x: 0, y: 0 },
			size: { width: 1, height: 1 },
			zIndex: 0,
			data: {},
		} satisfies CanvasNode;
		const state = {
			nodes: [node],
			connections: [],
			selection: [],
			viewport: { x: 0, y: 0, scale: 1 },
			mode: "select",
			activeTool: "select",
			isDrawing: false,
			isPanning: false,
		} satisfies CanvasState;
		expect(getNodeById(state, "a")).toBe(node);
		expect(getNodeById(state, "b")).toBeUndefined();
	});

	it("generates unique ids", () => {
		expect(generateId()).not.toBe(generateId());
	});
});
