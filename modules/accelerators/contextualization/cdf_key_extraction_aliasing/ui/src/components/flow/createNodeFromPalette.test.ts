import { describe, expect, it } from "vitest";
import { emptyWorkflowCanvasDocument, type WorkflowCanvasNode } from "../../types/workflowCanvas";
import { createNodeFromPalette } from "./createNodeFromPalette";
import { canvasToFlowNodes } from "./flowDocumentBridge";
import { nodeFlowSize } from "./flowNodeGeometry";

describe("createNodeFromPalette", () => {
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
