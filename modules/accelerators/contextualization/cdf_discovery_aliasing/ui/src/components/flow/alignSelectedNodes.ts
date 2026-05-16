import type { Node } from "@xyflow/react";
import { absoluteNodeRect } from "./flowNodeGeometry";
import { absoluteNodePosition } from "./flowParentGeometry";

export type AlignFlowSelectionMode =
  | "left"
  | "centerHorizontal"
  | "right"
  | "top"
  | "centerVertical"
  | "bottom";

const FIXED_LAYOUT_TYPES = new Set(["keaStart", "keaEnd"]);

function canAlignInFlow(n: Node): boolean {
  return !FIXED_LAYOUT_TYPES.has(String(n.type ?? ""));
}

/** Convert desired absolute top-left into stored ``position`` (relative when ``parentId`` is set). */
function flowTopLeftToStoredPosition(nodes: Node[], n: Node, absX: number, absY: number): { x: number; y: number } {
  const pid = n.parentId != null && String(n.parentId).trim() ? String(n.parentId).trim() : "";
  if (!pid) return { x: absX, y: absY };
  const pAbs = absoluteNodePosition(nodes, pid);
  return { x: absX - pAbs.x, y: absY - pAbs.y };
}

/**
 * Align selected nodes in flow space to a common edge or axis (bounding box of movable selection).
 * Horizontal/vertical center aligns each node’s center to the selection bbox center on that axis.
 * Start/end anchors are ignored for movement; if fewer than two movable nodes are selected,
 * returns ``null``.
 */
export function alignSelectedFlowNodes(
  nodes: Node[],
  selected: readonly Node[],
  mode: AlignFlowSelectionMode
): Node[] | null {
  const movable = selected.filter(canAlignInFlow);
  if (movable.length < 2) return null;

  const rects = movable.map((n) => ({ id: n.id, r: absoluteNodeRect(nodes, n) }));
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (const { r } of rects) {
    minX = Math.min(minX, r.x);
    minY = Math.min(minY, r.y);
    maxX = Math.max(maxX, r.x + r.w);
    maxY = Math.max(maxY, r.y + r.h);
  }

  const midX = (minX + maxX) / 2;
  const midY = (minY + maxY) / 2;

  const targets = new Map<string, { x: number; y: number }>();
  for (const { id, r } of rects) {
    let ax = r.x;
    let ay = r.y;
    if (mode === "left") ax = minX;
    else if (mode === "centerHorizontal") ax = midX - r.w / 2;
    else if (mode === "right") ax = maxX - r.w;
    else if (mode === "top") ay = minY;
    else if (mode === "centerVertical") ay = midY - r.h / 2;
    else if (mode === "bottom") ay = maxY - r.h;
    targets.set(id, { x: ax, y: ay });
  }

  let changed = false;
  const next = nodes.map((n) => {
    const t = targets.get(n.id);
    if (!t) return n;
    const rel = flowTopLeftToStoredPosition(nodes, n, t.x, t.y);
    if (Math.abs(rel.x - n.position.x) < 1e-6 && Math.abs(rel.y - n.position.y) < 1e-6) return n;
    changed = true;
    return { ...n, position: rel };
  });
  return changed ? next : null;
}
