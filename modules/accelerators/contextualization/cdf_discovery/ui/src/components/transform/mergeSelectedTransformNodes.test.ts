import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  buildMergedTransformConfigFromNodes,
  canMergeTransformSelection,
  isSequentialTransformSelection,
  mergeSelectedTransformFlowNodes,
  wireOrderedMergeSteps,
} from "./mergeSelectedTransformNodes";

function transformNode(
  id: string,
  config: Record<string, unknown>,
  pos: { x: number; y: number } = { x: 0, y: 0 }
): Node {
  return {
    id,
    type: "etlTransform",
    position: pos,
    data: { kind: "transform", label: id, config },
  };
}

function queryNode(id: string): Node {
  return {
    id,
    type: "etlQueryView",
    position: { x: 0, y: 0 },
    data: { kind: "query_view", label: id, config: {} },
  };
}

function saveNode(id: string): Node {
  return {
    id,
    type: "etlSaveView",
    position: { x: 400, y: 0 },
    data: { kind: "save_view", label: id, config: {} },
  };
}

function dataEdge(source: string, target: string): Edge {
  return {
    id: `e_${source}_${target}`,
    source,
    target,
    data: { kind: "data" },
  };
}

const trimStep = {
  handler_id: "trim_whitespace",
  fields: [{ field_name: "name" }],
  output_field: "aliases",
  trim_whitespace: {},
};

describe("canMergeTransformSelection", () => {
  it("allows a continuous internal sequence", () => {
    const nodes = [
      transformNode("a", { ...trimStep, output_field: "x" }),
      transformNode("b", { handler_id: "change_case", fields: [{ field_name: "x" }], output_field: "y" }, { x: 200, y: 0 }),
    ];
    expect(canMergeTransformSelection(nodes, [dataEdge("a", "b")])).toBe(true);
  });

  it("allows parallel siblings with the same predecessors and successors", () => {
    const nodes = [
      queryNode("q"),
      transformNode("a", trimStep),
      transformNode("b", { ...trimStep, fields: [{ field_name: "description" }] }, { x: 200, y: 0 }),
      saveNode("s"),
    ];
    const edges = [dataEdge("q", "a"), dataEdge("q", "b"), dataEdge("a", "s"), dataEdge("b", "s")];
    const transforms = [nodes[1]!, nodes[2]!];
    expect(canMergeTransformSelection(transforms, edges)).toBe(true);
  });

  it("rejects transforms with different successors", () => {
    const nodes = [
      queryNode("q"),
      transformNode("a", trimStep),
      transformNode("b", { ...trimStep, fields: [{ field_name: "description" }] }, { x: 200, y: 0 }),
      saveNode("s1"),
      saveNode("s2"),
    ];
    const edges = [
      dataEdge("q", "a"),
      dataEdge("q", "b"),
      dataEdge("a", "s1"),
      dataEdge("b", "s2"),
    ];
    expect(canMergeTransformSelection([nodes[1]!, nodes[2]!], edges)).toBe(false);
  });

  it("rejects disconnected transforms without shared boundaries", () => {
    const nodes = [
      transformNode("a", trimStep),
      transformNode("b", { ...trimStep, fields: [{ field_name: "description" }] }, { x: 200, y: 0 }),
    ];
    expect(canMergeTransformSelection(nodes, [])).toBe(false);
  });
});

describe("isSequentialTransformSelection", () => {
  it("detects a simple chain", () => {
    const nodes = [
      transformNode("a", { handler_id: "trim_whitespace", fields: [{ field_name: "name" }], output_field: "x" }),
      transformNode("b", { handler_id: "change_case", fields: [{ field_name: "x" }], output_field: "y" }, { x: 200, y: 0 }),
    ];
    const edges = [dataEdge("a", "b")];
    expect(isSequentialTransformSelection(nodes, edges)).toBe(true);
  });

  it("treats disconnected transforms as non-sequential", () => {
    const nodes = [
      transformNode("a", { handler_id: "trim_whitespace", fields: [{ field_name: "name" }], output_field: "aliases" }),
      transformNode("b", { handler_id: "trim_whitespace", fields: [{ field_name: "description" }], output_field: "aliases" }, { x: 200, y: 0 }),
    ];
    expect(isSequentialTransformSelection(nodes, [])).toBe(false);
  });
});

describe("wireOrderedMergeSteps", () => {
  it("chains output_field aliases into downstream field_name", () => {
    const steps = wireOrderedMergeSteps([
      {
        handler_id: "trim_whitespace",
        fields: [{ field_name: "name" }],
        output_field: "indexKey",
        output_template: "{name}",
      },
      {
        handler_id: "change_case",
        fields: [{ field_name: "indexKey" }],
        output_field: "indexKey",
        output_mode: "overwrite",
        change_case: { case: "lower" },
      },
    ]);
    expect(steps[0]?.output_field).toBe("_mergeStep0");
    expect(steps[1]?.fields).toEqual([{ field_name: "_mergeStep0" }]);
    expect(steps[1]?.output_field).toBe("indexKey");
  });
});

describe("mergeSelectedTransformFlowNodes", () => {
  it("merges a chain into ordered multi-step on anchor", () => {
    const nodes = [
      transformNode("a", {
        handler_id: "trim_whitespace",
        fields: [{ field_name: "name" }],
        output_field: "indexKey",
        trim_whitespace: {},
      }),
      transformNode("b", {
        handler_id: "change_case",
        fields: [{ field_name: "indexKey" }],
        output_field: "indexKey",
        change_case: { case: "lower" },
      }, { x: 220, y: 0 }),
    ];
    const edges = [dataEdge("a", "b")];
    const result = mergeSelectedTransformFlowNodes(nodes, edges, ["a", "b"], "a");
    expect(result?.nodes).toHaveLength(1);
    const cfg = (result!.nodes[0]!.data as { config: Record<string, unknown> }).config;
    expect(cfg.execution).toEqual({ mode: "ordered" });
    expect(Array.isArray(cfg.steps)).toBe(true);
    expect((cfg.steps as unknown[]).length).toBe(2);
    expect(result?.edges).toHaveLength(0);
  });

  it("merges parallel transforms with field_policies when output_field collides", () => {
    const nodes = [
      queryNode("q"),
      transformNode("a", trimStep),
      transformNode("b", { ...trimStep, fields: [{ field_name: "description" }] }, { x: 220, y: 0 }),
      saveNode("s"),
    ];
    const edges = [dataEdge("q", "a"), dataEdge("q", "b"), dataEdge("a", "s"), dataEdge("b", "s")];
    const transforms = [nodes[1]!, nodes[2]!];
    expect(canMergeTransformSelection(transforms, edges)).toBe(true);
    const cfg = buildMergedTransformConfigFromNodes(transforms, edges, "a")!;
    expect(cfg.execution).toEqual({ mode: "parallel" });
    const policies = cfg.field_policies as { property: string }[];
    expect(policies.some((p) => p.property === "aliases")).toBe(true);
  });

  it("keeps only anchor wiring for parallel merge (no bypass edges)", () => {
    const nodes = [
      queryNode("q"),
      transformNode("a", trimStep),
      transformNode("b", { ...trimStep, fields: [{ field_name: "description" }] }, { x: 220, y: 0 }),
      saveNode("s"),
    ];
    const edges = [dataEdge("q", "a"), dataEdge("q", "b"), dataEdge("a", "s"), dataEdge("b", "s")];
    const result = mergeSelectedTransformFlowNodes(nodes, edges, ["a", "b"], "a");
    expect(result?.nodes.map((n) => n.id).sort()).toEqual(["a", "q", "s"]);
    expect(result?.edges).toHaveLength(2);
    expect(result?.edges.some((e) => e.source === "q" && e.target === "a")).toBe(true);
    expect(result?.edges.some((e) => e.source === "a" && e.target === "s")).toBe(true);
    expect(result?.edges.some((e) => e.source === "q" && e.target === "s")).toBe(false);
  });

  it("returns null when boundaries do not match", () => {
    const nodes = [
      queryNode("q"),
      transformNode("a", trimStep),
      transformNode("b", { ...trimStep, fields: [{ field_name: "description" }] }, { x: 220, y: 0 }),
      saveNode("s1"),
      saveNode("s2"),
    ];
    const edges = [dataEdge("q", "a"), dataEdge("q", "b"), dataEdge("a", "s1"), dataEdge("b", "s2")];
    const result = mergeSelectedTransformFlowNodes(nodes, edges, ["a", "b"], "a");
    expect(result).toBeNull();
  });
});
