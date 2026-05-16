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

  it("is true when label matches Transform · {handler}", () => {
    expect(
      isHandlerTypedTransformNode(
        transformNode({
          handler_id: "regex_substitution",
          label: "Transform · regex_substitution",
          config: { handler_id: "regex_substitution" },
        })
      )
    ).toBe(true);
  });

  it("is false for legacy generic transform label", () => {
    expect(
      isHandlerTypedTransformNode(
        transformNode({
          label: "Transform",
          config: { handler_id: "trim_whitespace" },
        })
      )
    ).toBe(false);
  });
});
