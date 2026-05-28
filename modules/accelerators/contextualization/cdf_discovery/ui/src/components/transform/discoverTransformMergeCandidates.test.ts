import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  buildTransformMergeCandidateRows,
  discoverTransformMergeCandidates,
} from "./discoverTransformMergeCandidates";

function transformNode(id: string, pos: { x: number; y: number } = { x: 0, y: 0 }): Node {
  return {
    id,
    type: "etlTransform",
    position: pos,
    data: {
      kind: "transform",
      label: id,
      config: {
        handler_id: "trim_whitespace",
        fields: [{ field_name: "name" }],
        output_field: "aliases",
        trim_whitespace: {},
      },
    },
  };
}

function queryNode(id: string): Node {
  return { id, type: "etlQueryView", position: { x: 0, y: 0 }, data: { kind: "query_view", label: id, config: {} } };
}

function saveNode(id: string): Node {
  return { id, type: "etlSaveView", position: { x: 400, y: 0 }, data: { kind: "save_view", label: id, config: {} } };
}

function dataEdge(source: string, target: string): Edge {
  return { id: `e_${source}_${target}`, source, target, data: { kind: "data" } };
}

describe("discoverTransformMergeCandidates", () => {
  it("finds a sequential chain", () => {
    const nodes = [
      transformNode("a"),
      transformNode("b", { x: 200, y: 0 }),
      transformNode("c", { x: 400, y: 0 }),
    ];
    const edges = [dataEdge("a", "b"), dataEdge("b", "c")];
    const found = discoverTransformMergeCandidates(nodes, edges);
    expect(found.some((c) => c.kind === "ordered_chain" && c.nodeIds.join(",") === "a,b,c")).toBe(true);
  });

  it("finds parallel siblings with shared boundaries", () => {
    const nodes = [
      queryNode("q"),
      transformNode("a"),
      transformNode("b", { x: 200, y: 0 }),
      saveNode("s"),
    ];
    const edges = [dataEdge("q", "a"), dataEdge("q", "b"), dataEdge("a", "s"), dataEdge("b", "s")];
    const found = discoverTransformMergeCandidates(nodes, edges);
    expect(found.some((c) => c.kind === "parallel_siblings" && c.nodeIds.length === 2)).toBe(true);
  });

  it("marks overlapping candidates as conflicts", () => {
    const candidates = discoverTransformMergeCandidates(
      [transformNode("a"), transformNode("b", { x: 200, y: 0 }), transformNode("c", { x: 400, y: 0 })],
      [dataEdge("a", "b"), dataEdge("b", "c"), dataEdge("a", "c")]
    );
    const rows = buildTransformMergeCandidateRows(candidates);
    const approved = rows.filter((r) => r.approved);
    for (const a of approved) {
      for (const b of approved) {
        if (a.id === b.id) continue;
        expect(a.nodeIds.some((id) => b.nodeIds.includes(id))).toBe(false);
      }
    }
  });
});
