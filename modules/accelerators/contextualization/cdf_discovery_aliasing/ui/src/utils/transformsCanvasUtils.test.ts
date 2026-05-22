import { describe, expect, it } from "vitest";
import type { WorkflowCanvasNode } from "../types/workflowCanvas";
import { isHandlerTypedTransformNode } from "./transformsCanvasUtils";

function transformNode(data: WorkflowCanvasNode["data"]): WorkflowCanvasNode {
  return { id: "t1", kind: "transform", position: { x: 0, y: 0 }, data };
}

describe("isHandlerTypedTransformNode", () => {
  it("is true when preset_from_palette is set", () => {
    expect(
      isHandlerTypedTransformNode(
        transformNode({
          preset_from_palette: true,
          handler_id: "trim_whitespace",
          label: "Transform · trim_whitespace",
        })
      )
    ).toBe(true);
  });

  it("is true when handler_id is a registered discovery handler", () => {
    expect(
      isHandlerTypedTransformNode(
        transformNode({
          handler_id: "regex_substitution",
          label: "Regex substitution",
          config: { handler_id: "regex_substitution" },
        })
      )
    ).toBe(true);
  });

  it("is false without preset_from_palette or registered handler_id", () => {
    expect(
      isHandlerTypedTransformNode(
        transformNode({
          label: "Transform",
          config: { handler_id: "not_a_real_handler" },
        })
      )
    ).toBe(false);
  });
});
