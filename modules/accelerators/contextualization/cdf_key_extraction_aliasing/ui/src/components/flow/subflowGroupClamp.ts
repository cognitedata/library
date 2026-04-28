import type { Node } from "@xyflow/react";
import { absoluteNodeRect, nodeFlowSize } from "./flowNodeGeometry";
import { absoluteNodePosition } from "./flowParentGeometry";

/** Title strip height inside ``keaSubflow`` frames (matches wrap layout). */
export const SUBFLOW_FRAME_HEADER_PX = 40;
const FRAME_PAD = 12;

/**
 * If ``nodeId`` is parented under ``keaSubflow`` and its center lies outside the inner content
 * rectangle of the frame, nudge ``position`` so the center lies inside (minimal clamp).
 */
export function clampNodeInsideParentSubflowFrame(nodes: Node[], nodeId: string): Node[] {
  const node = nodes.find((n) => n.id === nodeId);
  if (!node?.parentId) return nodes;
  const pid = String(node.parentId).trim();
  if (!pid) return nodes;
  const parent = nodes.find((n) => n.id === pid);
  if (!parent || parent.type !== "keaSubflow") return nodes;

  const parentAbs = absoluteNodeRect(nodes, parent);
  const inner = {
    x: parentAbs.x + FRAME_PAD,
    y: parentAbs.y + SUBFLOW_FRAME_HEADER_PX + FRAME_PAD,
    w: Math.max(0, parentAbs.w - 2 * FRAME_PAD),
    h: Math.max(0, parentAbs.h - SUBFLOW_FRAME_HEADER_PX - 2 * FRAME_PAD),
  };
  if (inner.w <= 0 || inner.h <= 0) return nodes;

  const { w: cw, h: ch } = nodeFlowSize(node);
  const childAbs = absoluteNodeRect(nodes, node);
  let cx = childAbs.x + cw / 2;
  let cy = childAbs.y + ch / 2;

  const minCx = inner.x + Math.min(cw / 2, inner.w / 2);
  const maxCx = inner.x + inner.w - Math.min(cw / 2, inner.w / 2);
  const minCy = inner.y + Math.min(ch / 2, inner.h / 2);
  const maxCy = inner.y + inner.h - Math.min(ch / 2, inner.h / 2);
  if (minCx <= maxCx) cx = Math.min(maxCx, Math.max(minCx, cx));
  if (minCy <= maxCy) cy = Math.min(maxCy, Math.max(minCy, cy));

  const parentPos = absoluteNodePosition(nodes, pid);
  const nextRelX = cx - cw / 2 - parentPos.x;
  const nextRelY = cy - ch / 2 - parentPos.y;

  if (Math.abs(nextRelX - node.position.x) < 0.5 && Math.abs(nextRelY - node.position.y) < 0.5) {
    return nodes;
  }

  return nodes.map((n) => (n.id === nodeId ? { ...n, position: { x: nextRelX, y: nextRelY } } : n));
}
