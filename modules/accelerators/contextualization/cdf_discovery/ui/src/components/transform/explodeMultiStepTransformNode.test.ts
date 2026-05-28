import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  explodeMultiStepTransformFlowNode,
  isExplodableMultiStepTransformFlowNode,
} from "./explodeMultiStepTransformNode";

function multiStepNode(id: string, steps: Record<string, unknown>[], mode: "ordered" | "parallel"): Node {
  return {
    id,
    type: "etlTransform",
    position: { x: 100, y: 50 },
    data: {
      kind: "transform",
      label: "Combined",
      config: {
        description: "Combined",
        execution: { mode },
        steps,
      },
    },
  };
}

function queryNode(id: string): Node {
  return { id, type: "etlQueryView", position: { x: 0, y: 0 }, data: { kind: "query_view", label: id, config: {} } };
}

function saveNode(id: string): Node {
  return { id, type: "etlSaveView", position: { x: 500, y: 0 }, data: { kind: "save_view", label: id, config: {} } };
}

function dataEdge(source: string, target: string): Edge {
  return { id: `e_${source}_${target}`, source, target, data: { kind: "data" } };
}

describe("explodeMultiStepTransformFlowNode", () => {
  it("detects explodable multi-step nodes", () => {
    const n = multiStepNode("m", [
      { handler_id: "trim_whitespace", fields: [{ field_name: "name" }], output_field: "a", trim_whitespace: {} },
      { handler_id: "change_case", fields: [{ field_name: "a" }], output_field: "b", change_case: { case: "lower" } },
    ], "ordered");
    expect(isExplodableMultiStepTransformFlowNode(n)).toBe(true);
  });

  it("explodes ordered steps into a chain", () => {
    const nodes = [
      queryNode("q"),
      multiStepNode("m", [
        {
          handler_id: "trim_whitespace",
          fields: [{ field_name: "name" }],
          output_field: "_mergeStep0",
          trim_whitespace: {},
        },
        {
          handler_id: "change_case",
          fields: [{ field_name: "_mergeStep0" }],
          output_field: "aliases",
          change_case: { case: "lower" },
        },
      ], "ordered"),
      saveNode("s"),
    ];
    const edges = [dataEdge("q", "m"), dataEdge("m", "s")];
    const result = explodeMultiStepTransformFlowNode(nodes, edges, "m");
    expect(result?.newNodeIds).toHaveLength(2);
    expect(result?.nodes.some((n) => n.id === "m")).toBe(false);
    const chain = result!.newNodeIds;
    expect(result?.edges.some((e) => e.source === "q" && e.target === chain[0])).toBe(true);
    expect(result?.edges.some((e) => e.source === chain[0] && e.target === chain[1])).toBe(true);
    expect(result?.edges.some((e) => e.source === chain[1] && e.target === "s")).toBe(true);
    const firstCfg = (result!.nodes.find((n) => n.id === chain[0])!.data as { config: Record<string, unknown> })
      .config;
    expect(firstCfg.steps).toBeUndefined();
    expect(firstCfg.handler_id).toBe("trim_whitespace");
  });

  it("explodes parallel steps with shared fan-in and fan-out", () => {
    const nodes = [
      queryNode("q"),
      multiStepNode("m", [
        {
          handler_id: "trim_whitespace",
          fields: [{ field_name: "name" }],
          output_field: "aliases",
          trim_whitespace: {},
        },
        {
          handler_id: "trim_whitespace",
          fields: [{ field_name: "description" }],
          output_field: "aliases",
          trim_whitespace: {},
        },
      ], "parallel"),
      saveNode("s"),
    ];
    const edges = [dataEdge("q", "m"), dataEdge("m", "s")];
    const result = explodeMultiStepTransformFlowNode(nodes, edges, "m");
    const [a, b] = result!.newNodeIds;
    expect(result?.edges.some((e) => e.source === "q" && e.target === a)).toBe(true);
    expect(result?.edges.some((e) => e.source === "q" && e.target === b)).toBe(true);
    expect(result?.edges.some((e) => e.source === a && e.target === "s")).toBe(true);
    expect(result?.edges.some((e) => e.source === b && e.target === "s")).toBe(true);
  });
});
