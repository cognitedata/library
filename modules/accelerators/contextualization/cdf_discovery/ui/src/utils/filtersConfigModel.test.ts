import { describe, expect, it } from "vitest";
import { readFilters, mergeFilters } from "./filtersConfigModel";
import { emptyTransformCanvasDocument } from "../types/transformCanvas";
import { connectEndMenuGroupedOptionsForPane, connectEndMenuOptionsForSourceType } from "../components/transform/connectEndMenuOptions";

describe("filtersConfigModel", () => {
  it("reads filters array from config", () => {
    expect(readFilters({ filters: [{ operator: "EQUALS", target_property: "name" }] })).toHaveLength(1);
    expect(readFilters({})).toEqual([]);
  });

  it("mergeFilters removes empty filters", () => {
    expect(mergeFilters({ description: "x" }, [])).toEqual({ description: "x" });
  });
});

describe("emptyTransformCanvasDocument", () => {
  it("includes start to end edge", () => {
    const doc = emptyTransformCanvasDocument();
    expect(doc.edges).toEqual([{ id: "e_start_end", source: "start", target: "end", kind: "data" }]);
  });
});

describe("connectEndMenuOptionsForSourceType", () => {
  it("offers query stages from start", () => {
    const opts = connectEndMenuOptionsForSourceType("etlStart");
    expect(opts.some((o) => o.payload.stage === "query_view")).toBe(true);
  });
});

describe("connectEndMenuGroupedOptionsForPane", () => {
  it("includes all palette groups on empty canvas", () => {
    const groups = connectEndMenuGroupedOptionsForPane();
    expect(groups.map((g) => g.id)).toEqual(["query", "transform", "load", "orchestration"]);
    expect(groups[0]?.options.some((o) => o.payload.stage === "query_view")).toBe(true);
  });
});

describe("layoutTransformFlowNodes", () => {
  it("places end node after start on lr layout", async () => {
    const { layoutTransformFlowNodes } = await import("../components/transform/transformAutoLayoutFlow");
    const nodes = [
      { id: "start", type: "etlStart", position: { x: 0, y: 0 }, data: { kind: "start" } },
      { id: "end", type: "etlEnd", position: { x: 0, y: 0 }, data: { kind: "end" } },
    ];
    const edges = [{ id: "e1", source: "start", target: "end", data: { kind: "data" } }];
    const laidOut = layoutTransformFlowNodes(nodes, edges, "lr");
    const start = laidOut.find((n) => n.id === "start")!;
    const end = laidOut.find((n) => n.id === "end")!;
    expect(end.position.x).toBeGreaterThan(start.position.x);
  });
});

describe("alignSelectedTransformFlowNodes", () => {
  it("aligns movable nodes to left edge of selection bbox", async () => {
    const { alignSelectedTransformFlowNodes } = await import("../components/transform/alignSelectedNodes");
    const nodes = [
      { id: "a", type: "etlQueryView", position: { x: 10, y: 20 }, data: {} },
      { id: "b", type: "etlQueryView", position: { x: 50, y: 40 }, data: {} },
    ];
    const selected = [nodes[0]!, nodes[1]!];
    const next = alignSelectedTransformFlowNodes(nodes, selected, "left");
    expect(next).not.toBeNull();
    expect(next!.find((n) => n.id === "a")!.position.x).toBe(10);
    expect(next!.find((n) => n.id === "b")!.position.x).toBe(10);
  });

  it("ignores start/end and requires two movable nodes", async () => {
    const { alignSelectedTransformFlowNodes } = await import("../components/transform/alignSelectedNodes");
    const nodes = [
      { id: "start", type: "etlStart", position: { x: 0, y: 0 }, data: {} },
      { id: "a", type: "etlQueryView", position: { x: 10, y: 0 }, data: {} },
    ];
    expect(alignSelectedTransformFlowNodes(nodes, nodes, "left")).toBeNull();
  });
});
