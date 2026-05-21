import { describe, expect, it } from "vitest";
import { syncWorkflowScopeFromCanvas } from "./canvasScopeSync";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";

describe("syncWorkflowScopeFromCanvas", () => {
  it("strips legacy rule lists and associations", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [],
      edges: [],
    };
    const scope: Record<string, unknown> = {
      associations: [
        { kind: "source_view_to_extraction", source_view_index: 0, extraction_rule_name: "r1" },
      ],
      aliasing_rule_definitions: { x: { name: "x" } },
      key_extraction: {
        config: {
          data: {
            extraction_rules: [{ name: "r1", handler: "regex_handler" }],
            validation: { validation_rules: [] },
          },
        },
      },
      aliasing: {
        config: {
          data: {
            aliasing_rules: [{ name: "a", handler: "prefix_suffix" }],
            pathways: { steps: [] },
          },
        },
      },
    };
    const out = syncWorkflowScopeFromCanvas(canvas, scope);
    expect(out.associations).toBeUndefined();
    expect(out.aliasing_rule_definitions).toBeUndefined();
    const keData = (out.key_extraction as { config: { data: Record<string, unknown> } }).config.data;
    expect(keData.extraction_rules).toBeUndefined();
    const alData = (out.aliasing as { config: { data: Record<string, unknown> } }).config.data;
    expect(alData.aliasing_rules).toBeUndefined();
    expect(alData.pathways).toBeUndefined();
  });

  it("writes global key_extraction validation from match_validation_extraction nodes", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [
        {
          id: "sv",
          kind: "source_view",
          position: { x: 0, y: 0 },
          data: { ref: { source_view_index: 0 } },
        },
        {
          id: "mv",
          kind: "match_validation_extraction",
          position: { x: 1, y: 0 },
          data: {
            ref: { extraction_global_validation: true },
            validation_rule_name: "check_alias",
          },
        },
      ],
      edges: [
        {
          id: "e1",
          source: "sv",
          target: "mv",
          kind: "data",
        },
      ],
    };
    const scope: Record<string, unknown> = {
      key_extraction: {
        config: {
          data: {
            validation: { validation_rules: [], min_score: 0.5 },
          },
        },
      },
    };
    const out = syncWorkflowScopeFromCanvas(canvas, scope);
    const validation = (out.key_extraction as { config: { data: { validation: Record<string, unknown> } } })
      .config.data.validation;
    expect(validation.min_score).toBe(0.5);
    expect(validation.validation_rules).toEqual(["check_alias"]);
  });
});
