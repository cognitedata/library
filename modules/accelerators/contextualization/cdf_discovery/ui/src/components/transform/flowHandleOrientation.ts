import { Position, type Node } from "@xyflow/react";
import type { TransformCanvasHandleOrientation } from "../../types/transformCanvas";

export type FlowHandleOrientationNodeData = {
  flowHandleOrientation?: TransformCanvasHandleOrientation;
};

/** React Flow edge routing uses these when computing smooth-step paths. */
export function flowHandlePositionsForOrientation(
  orientation: TransformCanvasHandleOrientation
): Pick<Node, "sourcePosition" | "targetPosition"> {
  if (orientation === "tb") {
    return { sourcePosition: Position.Bottom, targetPosition: Position.Top };
  }
  return { sourcePosition: Position.Right, targetPosition: Position.Left };
}

export function applyFlowHandleOrientationToNode(
  node: Node,
  orientation: TransformCanvasHandleOrientation
): Node {
  const positions = flowHandlePositionsForOrientation(orientation);
  return {
    ...node,
    ...positions,
    data: {
      ...(node.data as Record<string, unknown>),
      flowHandleOrientation: orientation,
    },
  };
}
