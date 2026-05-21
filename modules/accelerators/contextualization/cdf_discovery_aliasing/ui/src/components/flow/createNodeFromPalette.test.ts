import { describe, expect, it } from "vitest";
import { emptyWorkflowCanvasDocument, type WorkflowCanvasNode } from "../../types/workflowCanvas";
import { createNodeFromPalette } from "./createNodeFromPalette";
import { canvasToFlowNodes } from "./flowDocumentBridge";
import { nodeFlowSize } from "./flowNodeGeometry";

describe("createNodeFromPalette", () => {
  it("creates a discovery transform with default name field and append output mode", () => {
    const n = createNodeFromPalette({ kind: "discovery", stage: "transform" }, { x: 0, y: 0 });
    expect(n.type).toBe("keaTransform");
    expect((n.data as Record<string, unknown>).handler_id).toBe("regex_substitution");
    const cfg = (n.data as Record<string, unknown>).config as Record<string, unknown>;
    expect(cfg.output_mode).toBe("append");
    expect(cfg.fields).toEqual([{ field_name: "name" }]);
    expect(cfg.output_template).toBe("{name}");
    expect(cfg.output_field).toBe("indexKey");
  });

  it("chains discovery transform input from previous transform output_field when provided", () => {
    const n = createNodeFromPalette(
      { kind: "discovery", stage: "transform" },
      { x: 0, y: 0 },
      { previousTransformOutputField: "assetTag" }
    );
    const cfg = (n.data as Record<string, unknown>).config as Record<string, unknown>;
    expect(cfg.fields).toEqual([{ field_name: "assetTag" }]);
    expect(cfg.output_template).toBe("{assetTag}");
    expect(cfg.output_field).toBe("assetTag");
  });

  it("creates a discrete transform node for the selected handler id", () => {
    const n = createNodeFromPalette(
      { kind: "discovery", stage: "transform", transformHandlerId: "trim_whitespace" },
      { x: 0, y: 0 }
    );
    const data = n.data as Record<string, unknown>;
    expect(data.handler_id).toBe("trim_whitespace");
    expect(data.label).toBe("Transform · trim_whitespace");
  });

  it("creates a structural source view node", () => {
    const n = createNodeFromPalette({ kind: "structural", nodeKind: "source_view" }, { x: 0, y: 0 });
    expect(n.type).toBe("keaSourceView");
    expect((n.data as Record<string, unknown>).label).toBe("Source view");
  });

  it("creates a card-sized keaSubgraph with default ports and empty inner canvas", () => {
    const n = createNodeFromPalette(
      { kind: "structural", nodeKind: "subgraph" },
      { x: 10, y: 20 }
    );
    expect(n.type).toBe("keaSubgraph");
    expect(n.style).toBeUndefined();
    const data = n.data as Record<string, unknown>;
    expect(data.label).toBe("Subgraph");
    expect(data.subflow_ports).toEqual({
      inputs: [{ id: "in", label: "in" }],
      outputs: [{ id: "out", label: "out" }],
    });
    expect(data.inner_canvas).toEqual(emptyWorkflowCanvasDocument());
    expect(nodeFlowSize(n)).toEqual({ w: 192, h: 112 });
  });

  it("canvasToFlowNodes does not apply subflow-sized style to persisted subgraph nodes", () => {
    const canvasNode: WorkflowCanvasNode = {
      id: "sg1",
      kind: "subgraph",
      position: { x: 0, y: 0 },
      data: { label: "S" },
      size: { width: 400, height: 300 },
    };
    const [rf] = canvasToFlowNodes([canvasNode]);
    expect(rf.style).toBeUndefined();
  });
});
