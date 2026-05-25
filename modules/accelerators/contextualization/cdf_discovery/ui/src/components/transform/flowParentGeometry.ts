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
