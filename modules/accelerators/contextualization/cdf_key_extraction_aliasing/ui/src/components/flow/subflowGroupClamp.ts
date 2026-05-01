import type { Node } from "@xyflow/react";

/** Kept for callers that still import the constant; subflow frames are removed. */
export const SUBFLOW_FRAME_HEADER_PX = 40;

/** No-op: legacy subflow frame clamping is no longer applicable. */
export function clampNodeInsideParentSubflowFrame(nodes: Node[], _nodeId: string): Node[] {
  void _nodeId;
  return nodes;
}
