import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  canInsertNodesOnEdge,
  findEdgeAtFlowPoint,
  isSplittableDataEdge,
  replaceEdgeWithInsertedChain,
} from "./paletteDropOnEdge";

function node(id: string, type: string, pos: { x: number; y: number }): Node {
  return { id, type, position: pos, data: {}, measured: { width: 100, height: 40 } };
}

describe("paletteDropOnEdge", () => {
  it("finds edge near segment midpoint", () => {
    const nodes = [
      node("a", "discoveryTransform", { x: 0, y: 0 }),
      node("b", "discoveryValidate", { x: 200, y: 0 }),
    ];
    const edges: Edge[] = [
      {
        id: "e1",
        source: "a",
        target: "b",
        sourceHandle: "out",
        targetHandle: "in",
        data: { kind: "data" },
      },
    ];
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    const hit = findEdgeAtFlowPoint({ x: 150, y: 0 }, edges, getNode, 1);
    expect(hit?.id).toBe("e1");
  });

  it("allows transform between transform and validate", () => {
    const nodes = [
      node("a", "discoveryTransform", { x: 0, y: 0 }),
      node("b", "discoveryValidate", { x: 200, y: 0 }),
    ];
    const edge: Edge = {
      id: "e1",
      source: "a",
      target: "b",
      sourceHandle: "out",
      targetHandle: "in",
      data: { kind: "data" },
    };
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    expect(
      canInsertNodesOnEdge(
        edge,
        [{ nodeId: "n1", rfType: "discoveryTransform" }],
        getNode,
        "canvas"
      )
    ).toBe(true);
  });

  it("rejects query insert on transform→validate edge", () => {
    const nodes = [
      node("a", "discoveryTransform", { x: 0, y: 0 }),
      node("b", "discoveryValidate", { x: 200, y: 0 }),
    ];
    const edge: Edge = {
      id: "e1",
      source: "a",
      target: "b",
      sourceHandle: "out",
      targetHandle: "in",
      data: { kind: "data" },
    };
    const getNode = (id: string) => nodes.find((n) => n.id === id);
    expect(
      canInsertNodesOnEdge(
        edge,
        [{ nodeId: "q1", rfType: "discoveryViewQuery" }],
        getNode,
        "canvas"
      )
    ).toBe(false);
  });

  it("replaces edge with two-hop chain", () => {
    const nodes = [
      node("a", "discoveryStart", { x: 0, y: 0 }),
      node("b", "discoveryEnd", { x: 300, y: 0 }),
      node("m", "discoveryTransform", { x: 150, y: 0 }),
    ];
    const edges: Edge[] = [
      {
        id: "e1",
        source: "a",
        target: "b",
        sourceHandle: "out",
        targetHandle: "in",
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

  it("skips validation-branch edges", () => {
    expect(
      isSplittableDataEdge({
        id: "v",
        source: "x",
        target: "y",
        sourceHandle: "validation",
        targetHandle: "in",
        data: { kind: "data" },
      })
    ).toBe(false);
  });
});
