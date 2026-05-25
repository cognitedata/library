import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import type { TreeNode } from "../../types/discoveryNodes";
import {
  applyEntityCanvasDropPair,
  applyTransformCanvasDrop,
  canInsertNodesOnEdge,
  findEdgeAtFlowPoint,
  isSplittableDataEdge,
  replaceEdgeWithInsertedChain,
} from "./paletteDropOnEdge";

const dmView: TreeNode = {
  id: "v1",
  label: "Asset",
  kind: "dm_view",
  open_target: {
    type: "dm_instances",
    view_space: "cdf_cdm",
    view_external_id: "CogniteAsset",
    view_version: "v1",
  },
};

function node(id: string, type: string, pos: { x: number; y: number }): Node {
  return { id, type, position: pos, data: {}, measured: { width: 100, height: 40 } };
}

describe("transform paletteDropOnEdge", () => {
  it("finds edge near segment midpoint", () => {
    const nodes = [
      node("a", "etlTransform", { x: 0, y: 0 }),
      node("b", "etlFilter", { x: 200, y: 0 }),
    ];
    const edges: Edge[] = [
      {
        id: "e1",
        source: "a",
        target: "b",
        data: { kind: "data" },
      },
    ];
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    const hit = findEdgeAtFlowPoint({ x: 100, y: 0 }, edges, getNode, 1);
    expect(hit?.id).toBe("e1");
  });

  it("allows transform insert on data edge", () => {
    const nodes = [
      node("a", "etlQueryView", { x: 0, y: 0 }),
      node("b", "etlSaveView", { x: 200, y: 0 }),
    ];
    const edge: Edge = {
      id: "e1",
      source: "a",
      target: "b",
      data: { kind: "data" },
    };
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    expect(
      canInsertNodesOnEdge(edge, [{ nodeId: "n1", rfType: "etlTransform" }], getNode)
    ).toBe(true);
  });

  it("replaces edge with two-hop chain", () => {
    const nodes = [
      node("a", "etlStart", { x: 0, y: 0 }),
      node("b", "etlEnd", { x: 300, y: 0 }),
      node("m", "etlTransform", { x: 150, y: 0 }),
    ];
    const edges: Edge[] = [
      {
        id: "e1",
        source: "a",
        target: "b",
        data: { kind: "data" },
      },
    ];
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    const edge = edges[0]!;
    const next = replaceEdgeWithInsertedChain(getNode, edges, edge, ["m"]);
    expect(next.some((e) => e.id === "e1")).toBe(false);
    expect(next.some((e) => e.source === "a" && e.target === "m")).toBe(true);
    expect(next.some((e) => e.source === "m" && e.target === "b")).toBe(true);
  });

  it("skips non-data edges", () => {
    expect(
      isSplittableDataEdge({
        id: "v",
        source: "x",
        target: "y",
        data: { kind: "sequence" },
      })
    ).toBe(false);
  });

  it("auto-wires dropped save node to first etlEnd", () => {
    const nodes = [
      node("end1", "etlEnd", { x: 400, y: 0 }),
    ];
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    const event = {
      dataTransfer: {
        getData: () =>
          JSON.stringify({ kind: "etl_stage", stage: "save_view" }),
      },
      clientX: 100,
      clientY: 100,
    } as unknown as React.DragEvent;
    const result = applyTransformCanvasDrop({
      event,
      screenToFlowPosition: () => ({ x: 100, y: 100 }),
      getNode,
      getEdges: () => [],
      nodes,
    });
    expect(result).not.toBeNull();
    const save = result!.nodes.find((n) => n.type === "etlSaveView");
    expect(save).toBeDefined();
    const toEnd = result!.edges.find((e) => e.source === save!.id && e.target === "end1");
    expect(toEnd).toBeDefined();
    expect(toEnd?.sourceHandle).toBe("out");
    expect(toEnd?.targetHandle).toBe("in");
  });

  it("drops entity query+save pair wired query out to save in", () => {
    const nodes = [node("end1", "etlEnd", { x: 600, y: 0 })];
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    const result = applyEntityCanvasDropPair({
      node: dmView,
      flowPosition: { x: 80, y: 40 },
      getNode,
      getEdges: () => [],
      nodes,
    });
    expect(result).not.toBeNull();
    const query = result!.nodes.find((n) => n.type === "etlQueryView");
    const save = result!.nodes.find((n) => n.type === "etlSaveView");
    expect(query).toBeDefined();
    expect(save).toBeDefined();
    expect(save!.position.x).toBeGreaterThan(query!.position.x);
    const wire = result!.edges.find((e) => e.source === query!.id && e.target === save!.id);
    expect(wire).toBeDefined();
    expect(wire?.sourceHandle).toBe("out");
    expect(wire?.targetHandle).toBe("in");
    const toEnd = result!.edges.find((e) => e.source === save!.id && e.target === "end1");
    expect(toEnd).toBeDefined();
  });
});
