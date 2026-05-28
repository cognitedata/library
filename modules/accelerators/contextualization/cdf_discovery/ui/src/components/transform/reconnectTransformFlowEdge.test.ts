import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import { reconnectTransformFlowEdge } from "./reconnectTransformFlowEdge";

const nodes: Node[] = [
  { id: "start", type: "etlStart", position: { x: 0, y: 0 }, data: {} },
  { id: "a", type: "etlTransform", position: { x: 100, y: 0 }, data: {} },
  { id: "b", type: "etlTransform", position: { x: 200, y: 0 }, data: {} },
  { id: "end", type: "etlEnd", position: { x: 300, y: 0 }, data: {} },
];

const edges: Edge[] = [
  {
    id: "e1",
    source: "start",
    target: "a",
    sourceHandle: "out",
    targetHandle: "in",
  },
  {
    id: "e2",
    source: "a",
    target: "b",
    sourceHandle: "out",
    targetHandle: "in",
  },
];

function getNode(id: string): Node | undefined {
  return nodes.find((n) => n.id === id);
}

describe("reconnectTransformFlowEdge", () => {
  it("reconnects target when valid", () => {
    const old = edges[1]!;
    const next = reconnectTransformFlowEdge(
      old,
      { source: "a", target: "end", sourceHandle: "out", targetHandle: "in" },
      edges,
      getNode
    );
    expect(next).not.toBe(edges);
    expect(next.some((e) => e.source === "a" && e.target === "end")).toBe(true);
  });

  it("rejects invalid connection", () => {
    const old = edges[0]!;
    const next = reconnectTransformFlowEdge(
      old,
      { source: "end", target: "a", sourceHandle: "out", targetHandle: "in" },
      edges,
      getNode
    );
    expect(next).toBe(edges);
  });

  it("rejects cycle", () => {
    const old = edges[0]!;
    const next = reconnectTransformFlowEdge(
      old,
      { source: "b", target: "start", sourceHandle: "out", targetHandle: "in" },
      edges,
      getNode
    );
    expect(next).toBe(edges);
  });
});
