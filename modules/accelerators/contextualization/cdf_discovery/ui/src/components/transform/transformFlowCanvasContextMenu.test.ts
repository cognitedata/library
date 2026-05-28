import { describe, expect, it, vi } from "vitest";
import {
  buildTransformFlowPaneContextMenuItems,
  flowLayoutContextMenuItems,
} from "./transformFlowCanvasContextMenu";

describe("transformFlowCanvasContextMenu", () => {
  it("includes layout method and edge style options on the pane menu", () => {
    const items = buildTransformFlowPaneContextMenuItems({
      t: (key) => key,
      onCopy: vi.fn(),
      onPaste: vi.fn(),
      onAddNode: vi.fn(),
      alignDisabled: true,
      onAlignSelection: vi.fn(),
      handleOrientation: "lr",
      onHandleOrientationChange: vi.fn(),
      layoutMethod: "layered",
      onLayoutMethodChange: vi.fn(),
      edgePathStyle: "smoothstep",
      onEdgePathStyleChange: vi.fn(),
      onFitView: vi.fn(),
      onAutoLayout: vi.fn(),
    });
    const ids = items.map((i) => i.id);
    expect(ids).toContain("copy");
    expect(ids).toContain("layout-method-dagre");
    expect(ids).toContain("edge-default");
    expect(ids.some((id) => id.startsWith("align-"))).toBe(true);
  });

  it("omits layout method when not configured", () => {
    const items = flowLayoutContextMenuItems({
      t: (key) => key,
      handleOrientation: "tb",
      onHandleOrientationChange: vi.fn(),
      edgePathStyle: "straight",
      onEdgePathStyleChange: vi.fn(),
      onFitView: vi.fn(),
      onAutoLayout: vi.fn(),
    });
    expect(items.some((i) => i.id.startsWith("layout-method-"))).toBe(false);
    expect(items.some((i) => i.id === "handle-tb" && i.label.endsWith("✓"))).toBe(true);
  });
});
