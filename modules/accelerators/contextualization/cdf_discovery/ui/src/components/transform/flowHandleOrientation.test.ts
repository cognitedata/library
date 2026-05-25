import { Position } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import {
  applyFlowHandleOrientationToNode,
  flowHandlePositionsForOrientation,
} from "./flowHandleOrientation";

describe("flowHandlePositionsForOrientation", () => {
  it("uses left/right for lr", () => {
    expect(flowHandlePositionsForOrientation("lr")).toEqual({
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    });
  });

  it("uses top/bottom for tb", () => {
    expect(flowHandlePositionsForOrientation("tb")).toEqual({
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
    });
  });
});

describe("applyFlowHandleOrientationToNode", () => {
  it("stamps orientation on node data", () => {
    const next = applyFlowHandleOrientationToNode(
      { id: "a", position: { x: 0, y: 0 }, data: { label: "x" } },
      "tb"
    );
    expect(next.sourcePosition).toBe(Position.Bottom);
    expect(next.targetPosition).toBe(Position.Top);
    expect((next.data as { flowHandleOrientation?: string }).flowHandleOrientation).toBe("tb");
  });
});
