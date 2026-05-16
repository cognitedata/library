import { describe, expect, it } from "vitest";
import { syncWorkflowScopeFromCanvas } from "./canvasScopeSync";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";

function alNode(
  id: string,
  ruleName: string
): WorkflowCanvasDocument["nodes"][0] {
  return {
    id,
    kind: "aliasing",
    data: {
      ref: { aliasing_rule_name: ruleName },
    },
  } as WorkflowCanvasDocument["nodes"][0];
}

function seqEdge(source: string, target: string): WorkflowCanvasDocument["edges"][0] {
  return {
    id: `e-${source}-${target}`,
    source,
    target,
    kind: "sequence",
  } as WorkflowCanvasDocument["edges"][0];
}

function dataEdge(source: string, target: string, id: string): WorkflowCanvasDocument["edges"][0] {
  return {
    id,
    source,
    target,
    kind: "data",
  } as WorkflowCanvasDocument["edges"][0];
}

describe("syncWorkflowScopeFromCanvas — aliasing config matches canvas", () => {
  it("drops transform rules removed from the canvas (composition order)", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [alNode("a", "keep_a"), alNode("b", "keep_b")],
      edges: [seqEdge("a", "b")],
    };
    const scope: Record<string, unknown> = {
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              { name: "keep_a", handler: "regex_substitution", enabled: true, priority: 1 },
              { name: "keep_b", handler: "leading_zero_normalization", enabled: true, priority: 2 },
              { name: "orphan", handler: "semantic_expansion", enabled: true, priority: 3 },
            ],
          },
        },
      },
    };
    const out = syncWorkflowScopeFromCanvas(canvas, scope);
    const rules = (out.aliasing as { config: { data: { aliasing_rules: { name: string }[] } } })
      .config.data.aliasing_rules;
    expect(rules.map((r) => r.name)).toEqual(["keep_a", "keep_b"]);
  });

  it("filters rules by aliasing nodes when there are no composition edges", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [alNode("x", "only_one")],
      edges: [],
    };
    const scope: Record<string, unknown> = {
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              { name: "only_one", handler: "document_aliases", enabled: true, priority: 1 },
              { name: "gone", handler: "semantic_expansion", enabled: true, priority: 2 },
            ],
          },
        },
      },
    };
    const out = syncWorkflowScopeFromCanvas(canvas, scope);
    const rules = (out.aliasing as { config: { data: { aliasing_rules: { name: string }[] } } })
      .config.data.aliasing_rules;
    expect(rules.map((r) => r.name)).toEqual(["only_one"]);
  });

  it("prunes root aliasing_rule_definitions when a rule is removed from data", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [alNode("a", "stay")],
      edges: [],
    };
    const scope: Record<string, unknown> = {
      aliasing_rule_definitions: {
        stay: { name: "stay", handler: "regex_substitution" },
        drop_me: { name: "drop_me", handler: "semantic_expansion" },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              { name: "stay", handler: "regex_substitution", enabled: true, priority: 1 },
              { name: "drop_me", handler: "semantic_expansion", enabled: true, priority: 2 },
            ],
          },
        },
      },
    };
    const out = syncWorkflowScopeFromCanvas(canvas, scope);
    const defs = out.aliasing_rule_definitions as Record<string, unknown> | undefined;
    expect(defs && Object.keys(defs).sort()).toEqual(["stay"]);
  });

  it("preserves extraction→aliasing data edge order for concurrent heads (not alphabetical)", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [
        {
          id: "ext_er1",
          kind: "extraction",
          position: { x: 0, y: 0 },
          data: { ref: { extraction_rule_name: "er1" } },
        } as WorkflowCanvasDocument["nodes"][0],
        { ...alNode("al_zebra", "zebra_rule"), position: { x: 1, y: 0 } },
        { ...alNode("al_ant", "ant_rule"), position: { x: 2, y: 0 } },
      ],
      edges: [
        dataEdge("ext_er1", "al_zebra", "e-zebra-first"),
        dataEdge("ext_er1", "al_ant", "e-ant-second"),
      ],
    };
    const scope: Record<string, unknown> = {
      key_extraction: {
        config: {
          data: {
            extraction_rules: [
              {
                name: "er1",
                handler: "regex_handler",
                enabled: true,
                priority: 1,
                aliasing_pipeline: [],
              },
            ],
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [
              { name: "ant_rule", handler: "character_substitution", enabled: true, priority: 1 },
              { name: "zebra_rule", handler: "character_substitution", enabled: true, priority: 2 },
            ],
          },
        },
      },
    };
    const out = syncWorkflowScopeFromCanvas(canvas, scope);
    const rules = (out.key_extraction as { config: { data: { extraction_rules: unknown[] } } }).config.data
      .extraction_rules as { name: string; aliasing_pipeline: unknown[] }[];
    const row = rules.find((r) => r.name === "er1")!;
    expect(Array.isArray(row.aliasing_pipeline)).toBe(true);
    expect(row.aliasing_pipeline.length).toBe(1);
    const h = row.aliasing_pipeline[0] as { hierarchy?: { mode?: string; children?: unknown[] } };
    expect(h.hierarchy?.mode).toBe("concurrent");
    expect(h.hierarchy?.children).toEqual(["zebra_rule", "ant_rule"]);
  });
});
