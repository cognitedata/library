import type { CanvasNode, Connection } from "@dune-canvas/canvas-lib";
import { describe, expect, it } from "vitest";

import {
	extractCauseMapGraph,
	getChildIds,
	getIncomingEdges,
	getOutgoingEdges,
	getParentIds,
	hasMultipleParents,
	isRootNode,
} from "./causeMapLayout";

describe("causeMapLayout", () => {
	const nodes = [
		{
			id: "a",
			type: "cause-map",
			position: { x: 1, y: 2 },
			size: { width: 10, height: 20 },
			zIndex: 0,
			data: {},
		},
		{
			id: "b",
			type: "cause-map",
			position: { x: 3, y: 4 },
			size: { width: 10, height: 20 },
			zIndex: 0,
			data: {},
		},
		{
			id: "text",
			type: "text",
			position: { x: 0, y: 0 },
			size: { width: 1, height: 1 },
			zIndex: 0,
			data: {},
		},
	] satisfies CanvasNode[];
	const connections = [
		{ id: "1", from: "a", to: "b" },
		{ id: "2", from: "text", to: "a" },
		{ id: "3", from: "b", to: "a" },
	] satisfies Connection[];

	it("keeps only cause-map nodes and edges between them", () => {
		const graph = extractCauseMapGraph(nodes, connections);
		expect(graph.causeMapNodes).toHaveLength(2);
		expect(graph.causeMapEdges).toEqual([
			{ fromId: "a", toId: "b" },
			{ fromId: "b", toId: "a" },
		]);
	});

	it("answers parent and child queries", () => {
		const edges = [
			{ fromId: "a", toId: "b" },
			{ fromId: "c", toId: "b" },
		];
		expect(getIncomingEdges("b", edges)).toHaveLength(2);
		expect(getOutgoingEdges("a", edges)).toHaveLength(1);
		expect(isRootNode("a", edges)).toBe(true);
		expect(hasMultipleParents("b", edges)).toBe(true);
		expect(getParentIds("b", edges)).toEqual(["a", "c"]);
		expect(getChildIds("a", edges)).toEqual(["b"]);
	});
});
