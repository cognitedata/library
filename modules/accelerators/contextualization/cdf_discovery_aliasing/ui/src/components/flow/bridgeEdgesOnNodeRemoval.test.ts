import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import { bridgeEdgesOnNodeRemoval, isBridgeableFlowEdge } from "./bridgeEdgesOnNodeRemoval";

function node(id: string, type: string): Node {
  return { id, type, position: { x: 0, y: 0 }, data: {} };
}

function dataEdge(
  id: string,
  source: string,
  target: string,
  opts?: { sourceHandle?: string; targetHandle?: string }
): Edge {
  return {
    id,
    source,
    target,
    sourceHandle: opts?.sourceHandle ?? "out",
    targetHandle: opts?.targetHandle ?? "in",
    data: { kind: "data" },
  };
}

describe("bridgeEdgesOnNodeRemoval", () => {
  const nodes = [
    node("a", "discoveryTransform"),
    node("b", "discoveryTransform"),
    node("c", "discoveryValidate"),
  ];
  const getNode = (id: string) => nodes.find((n) => n.id === id);

  it("bridges A→B→C to A→C when B is removed", () => {
    const edges = [dataEdge("e1", "a", "b"), dataEdge("e2", "b", "c")];
    const next = bridgeEdgesOnNodeRemoval(edges, new Set(["b"]), getNode);
    expect(next.some((e) => e.source === "a" && e.target === "c")).toBe(true);
    expect(next.some((e) => e.source === "a" && e.target === "b")).toBe(false);
    expect(next.some((e) => e.source === "b")).toBe(false);
  });

  it("bridges through a removed subtree", () => {
    const nodes2 = [
      node("a", "discoveryTransform"),
      node("b", "discoveryTransform"),
      node("c", "discoveryTransform"),
      node("d", "discoveryValidate"),
    ];
    const getNode2 = (id: string) => nodes2.find((n) => n.id === id);
    const edges = [
      dataEdge("e1", "a", "b"),
      dataEdge("e2", "b", "c"),
      dataEdge("e3", "c", "d"),
    ];
    const next = bridgeEdgesOnNodeRemoval(edges, new Set(["b", "c"]), getNode2);
    expect(next.some((e) => e.source === "a" && e.target === "d")).toBe(true);
    expect(next.length).toBe(1);
  });

  it("does not bridge validation-handle edges", () => {
    const edges = [
      dataEdge("e1", "a", "b"),
      {
        id: "e2",
        source: "b",
        target: "c",
        sourceHandle: "validation",
        targetHandle: "in",
        data: { kind: "data" },
      },
    ];
    const next = bridgeEdgesOnNodeRemoval(edges, new Set(["b"]), getNode);
    expect(next.some((e) => e.source === "a" && e.target === "c")).toBe(false);
    expect(next.length).toBe(0);
  });

  it("skips bridging through discoverySubgraph boundaries", () => {
    const sgNodes = [node("a", "discoveryTransform"), node("sg", "discoverySubgraph"), node("d", "discoveryEnd")];
    const getSg = (id: string) => sgNodes.find((n) => n.id === id);
    const edges = [dataEdge("e1", "a", "sg"), dataEdge("e2", "sg", "d")];
    expect(isBridgeableFlowEdge(edges[0]!, getSg)).toBe(false);
    expect(isBridgeableFlowEdge(edges[1]!, getSg)).toBe(false);
    const next = bridgeEdgesOnNodeRemoval(edges, new Set(["sg"]), getSg);
    expect(next.length).toBe(0);
  });
});
