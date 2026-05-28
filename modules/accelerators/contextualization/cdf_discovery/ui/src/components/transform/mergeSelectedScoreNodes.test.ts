import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import { explodeScoreFlowNode } from "./explodeScoreFlowNode";
import { mergeSelectedScoreFlowNodes } from "./mergeSelectedScoreNodes";

function scoreNode(id: string, config: Record<string, unknown>, pos = { x: 0, y: 0 }): Node {
  return {
    id,
    type: "etlScore",
    position: pos,
    data: { kind: "score", label: id, config },
  };
}

function dataEdge(source: string, target: string): Edge {
  return { id: `e_${source}_${target}`, source, target, data: { kind: "data" } };
}

const baseCfg = {
  description: "score",
  score_fields: ["aliases"],
  scoring_rules: [{ name: "r1", match: { keywords: ["a"] }, score_modifier: { mode: "offset", value: 0 } }],
};

describe("mergeSelectedScoreFlowNodes", () => {
  it("merges a chain of score nodes", () => {
    const nodes = [
      scoreNode("a", { ...baseCfg, scoring_rules: [{ name: "r1", match: {}, score_modifier: { mode: "offset", value: 0 } }] }),
      scoreNode(
        "b",
        { ...baseCfg, scoring_rules: [{ name: "r2", match: {}, score_modifier: { mode: "offset", value: 0 } }] },
        { x: 200, y: 0 }
      ),
    ];
    const result = mergeSelectedScoreFlowNodes(nodes, [dataEdge("a", "b")], ["a", "b"], "a");
    expect(result?.nodes).toHaveLength(1);
    const cfg = (result!.nodes[0]!.data as { config: { scoring_rules: unknown[] } }).config;
    expect(cfg.scoring_rules).toHaveLength(2);
  });
});

describe("explodeScoreFlowNode", () => {
  it("explodes multiple scoring rules into a chain", () => {
    const nodes = [
      scoreNode("s", {
        description: "combined",
        score_fields: ["aliases"],
        scoring_rules: [
          { name: "r1", match: {}, score_modifier: { mode: "offset", value: 0 } },
          { name: "r2", match: {}, score_modifier: { mode: "offset", value: 0 } },
        ],
      }),
    ];
    const result = explodeScoreFlowNode(nodes, [], "s");
    expect(result?.newNodeIds).toHaveLength(2);
    expect(result?.mode).toBe("rules_chain");
  });

  it("explodes multiple score fields in parallel", () => {
    const nodes = [
      scoreNode("s", {
        description: "combined",
        score_fields: ["aliases", "indexKey"],
        scoring_rules: [{ name: "r1", match: {}, score_modifier: { mode: "offset", value: 0 } }],
      }),
    ];
    const result = explodeScoreFlowNode(nodes, [], "s");
    expect(result?.newNodeIds).toHaveLength(2);
    expect(result?.mode).toBe("fields_parallel");
  });
});
