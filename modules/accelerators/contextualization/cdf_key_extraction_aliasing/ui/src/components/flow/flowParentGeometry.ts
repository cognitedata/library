import type { Node } from "@xyflow/react";

/** Absolute flow position (sum of relative positions along parent chain). */
export function absoluteNodePosition(nodes: Node[], nodeId: string): { x: number; y: number } {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  let x = 0;
  let y = 0;
  let cur: string | undefined = nodeId;
  const guard = new Set<string>();
  while (cur && !guard.has(cur)) {
    guard.add(cur);
    const n = byId.get(cur);
    if (!n) break;
    x += n.position.x;
    y += n.position.y;
    cur = n.parentId;
  }
  return { x, y };
}

/**
 * When assigning ``parentId``, convert a node's current absolute position into
 * coordinates relative to the new parent so it does not jump on the canvas.
 */
export function relativePositionForNewParent(
  nodes: Node[],
  nodeId: string,
  newParentId: string
): { x: number; y: number } {
  const childAbs = absoluteNodePosition(nodes, nodeId);
  const parentAbs = absoluteNodePosition(nodes, newParentId);
  return { x: childAbs.x - parentAbs.x, y: childAbs.y - parentAbs.y };
}

export function isDescendantInParentTree(nodes: Node[], ancestorId: string, maybeDescendantId: string): boolean {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  let cur: string | undefined = maybeDescendantId;
  const guard = new Set<string>();
  while (cur && !guard.has(cur)) {
    guard.add(cur);
    if (cur === ancestorId) return true;
    cur = byId.get(cur)?.parentId;
  }
  return false;
}

/** Ids to remove when deleting ``rootId`` (node plus every descendant in the parent tree). */
export function collectSubtreeNodeIds(nodes: Node[], rootId: string): Set<string> {
  const out = new Set<string>([rootId]);
  let prev = -1;
  while (out.size > prev) {
    prev = out.size;
    for (const n of nodes) {
      if (n.parentId && out.has(n.parentId)) out.add(n.id);
    }
  }
  return out;
}

/** Apply or clear ``parentId`` while keeping the node visually fixed in flow space. */
export function applyNodeParentChange(nodes: Node[], nodeId: string, newParentId: string | null): Node[] {
  const next = newParentId != null && String(newParentId).trim() ? String(newParentId).trim() : null;
  if (!next) {
    const abs = absoluteNodePosition(nodes, nodeId);
    return nodes.map((n) => {
      if (n.id !== nodeId) return n;
      const { parentId: _p, extent: _e, expandParent: _x, ...rest } = n as Node & {
        parentId?: string;
        extent?: "parent" | null;
        expandParent?: boolean;
      };
      return { ...(rest as Node), position: abs };
    });
  }
  const rel = relativePositionForNewParent(nodes, nodeId, next);
  return nodes.map((n) => {
    if (n.id !== nodeId) return n;
    return { ...n, parentId: next, extent: "parent" as const, expandParent: true, position: rel };
  });
}
