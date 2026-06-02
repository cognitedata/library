import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { autoResizeFlowNodesToContent } from "./autoResizeFlowNodesToContent";
import { maxEtlNodeWidth } from "./etlFlowNodeSizing";

describe("autoResizeFlowNodesToContent", () => {
  it("clears fixed width/height and measured size while preserving other style props", () => {
    const nodes = [
      {
        id: "n1",
        type: "etlTransform",
        position: { x: 10, y: 20 },
        data: { label: "Transform" },
        width: 192,
        height: 180,
        measured: { width: 192, height: 176 },
        style: { width: 192, height: 180, borderLeftColor: "#00a", backgroundColor: "#fff" },
      } satisfies Node,
    ];

    const resized = autoResizeFlowNodesToContent(nodes);
    const resizedNode = resized[0]!;
    const cappedWidth = `${maxEtlNodeWidth("transform")}px`;

    expect(resizedNode.width).toBeUndefined();
    expect(resizedNode.height).toBeUndefined();
    expect(resizedNode.measured).toBeUndefined();
    expect(resizedNode.style).toEqual({
      borderLeftColor: "#00a",
      backgroundColor: "#fff",
      maxWidth: cappedWidth,
      "--etl-label-max-width": cappedWidth,
    });
  });

  it("does not cap label width for manually widened nodes", () => {
    const nodes = [
      {
        id: "n2",
        type: "etlTransform",
        position: { x: 0, y: 0 },
        data: { label: "Wide Transform" },
        width: 420,
        height: 160,
        measured: { width: 420, height: 160 },
        style: { width: 420, height: 160, borderLeftColor: "#0a0", "--etl-label-max-width": "288px" },
      } satisfies Node,
    ];

    const resized = autoResizeFlowNodesToContent(nodes);
    expect(resized[0]!.style).toEqual({ borderLeftColor: "#0a0", maxWidth: `${maxEtlNodeWidth("transform")}px` });
  });
});
