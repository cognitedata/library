import type { Node } from "@xyflow/react";
import { orderFlowNodesForReactFlow } from "./flowDocumentBridge";
import { absoluteNodeRect, type FlowRect } from "./flowNodeGeometry";
import { applyNodeParentChange, isDescendantInParentTree } from "./flowParentGeometry";
import { canChangeSubflowParent } from "./subflowMembership";

function parentDepth(nodes: Node[], id: string): number {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  let d = 0;
  let cur: string | undefined = id;
  const guard = new Set<string>();
  while (cur && !guard.has(cur)) {
    guard.add(cur);
    const p: string | undefined = byId.get(cur)?.parentId;
    if (!p || !String(p).trim()) break;
    d++;
    cur = p;
  }
  return d;
}

/**
 * Single entry point for subflow ``parentId`` changes: same validation and
 * ``applyNodeParentChange`` position conversion as drag-drop reparenting and
 * palette-drop resolution.
 */
export function assignFlowNodeSubflowParent(
  nodes: Node[],
  nodeId: string,
  parentSubflowId: string | null
): Node[] {
  const next =
    parentSubflowId != null && String(parentSubflowId).trim() ? String(parentSubflowId).trim() : null;
  const cur = nodes.find((n) => n.id === nodeId);
  if (!cur) return nodes;
  if (!canChangeSubflowParent(cur.type)) return nodes;
  if (next != null) {
    const p = nodes.find((n) => n.id === next);
    if (!p || p.type !== "keaSubflow") return nodes;
    if (isDescendantInParentTree(nodes, nodeId, next)) return nodes;
  }
  return orderFlowNodesForReactFlow(applyNodeParentChange(nodes, nodeId, next));
}

function rectCenter(r: FlowRect): { x: number; y: number } {
  return { x: r.x + r.w / 2, y: r.y + r.h / 2 };
}

function pointInRect(p: { x: number; y: number }, r: FlowRect): boolean {
  return p.x >= r.x && p.x <= r.x + r.w && p.y >= r.y && p.y <= r.y + r.h;
}

function rectArea(r: FlowRect): number {
  return Math.max(0, r.w) * Math.max(0, r.h);
}

/**
 * After a node drag ends, assign or clear ``parentId`` when the node center lies inside a
 * subflow frame (innermost/smallest containing subflow wins). Dragging out of the current
 * subflow clears the association.
 *
 * @returns Updated node list, or ``null`` if no change.
 */
export function resolveSubflowParentAfterDrag(nodes: Node[], dragged: Node): Node[] | null {
  if (!canChangeSubflowParent(dragged.type)) return null;

  const center = rectCenter(absoluteNodeRect(nodes, dragged));
  const currentParentId =
    dragged.parentId != null && String(dragged.parentId).trim() ? String(dragged.parentId).trim() : "";

  const subflowCandidates = nodes.filter(
    (n) =>
      n.type === "keaSubflow" &&
      n.id !== dragged.id &&
      !isDescendantInParentTree(nodes, dragged.id, n.id)
  );

  const containing = subflowCandidates
    .map((sf) => ({ sf, rect: absoluteNodeRect(nodes, sf) }))
    .filter(({ rect }) => pointInRect(center, rect))
    .sort((a, b) => rectArea(a.rect) - rectArea(b.rect));

  const nextParentId = containing[0]?.sf.id ?? "";

  if (nextParentId) {
    if (nextParentId === currentParentId) return null;
    return assignFlowNodeSubflowParent(nodes, dragged.id, nextParentId);
  }

  if (currentParentId) {
    const parent = nodes.find((n) => n.id === currentParentId);
    if (parent && parent.type === "keaSubflow") {
      const pb = absoluteNodeRect(nodes, parent);
      if (!pointInRect(center, pb)) {
        return assignFlowNodeSubflowParent(nodes, dragged.id, null);
      }
    }
  }

  return null;
}

/**
 * After a **multi-select** drag, run ``resolveSubflowParentAfterDrag`` for every selected node
 * that may change subflow membership, iterating shallow-to-deep until stable.
 *
 * @returns Updated node list, or ``null`` if nothing changed.
 */
export function resolveSubflowParentsAfterGroupDrag(nodes: Node[], primaryId: string): Node[] | null {
  const want = new Set<string>();
  for (const n of nodes) {
    if (n.selected && canChangeSubflowParent(n.type)) want.add(n.id);
  }
  want.add(primaryId);

  let acc = nodes;
  let any = false;
  for (let pass = 0; pass < want.size + 4; pass++) {
    let passChanged = false;
    const ids = [...want].sort((a, b) => parentDepth(acc, a) - parentDepth(acc, b) || a.localeCompare(b));
    for (const id of ids) {
      const d = acc.find((n) => n.id === id);
      if (!d) continue;
      const u = resolveSubflowParentAfterDrag(acc, d);
      if (u) {
        acc = u;
        passChanged = true;
        any = true;
      }
    }
    if (!passChanged) break;
  }
  return any ? orderFlowNodesForReactFlow(acc) : null;
}

/** Append a palette-created node, then parent it under the innermost subflow frame that contains its center. */
export function appendNodeAndResolveSubflowParent(nds: Node[], node: Node): Node[] {
  const extended = nds.concat(node);
  const placed = extended.find((n) => n.id === node.id);
  if (!placed) return orderFlowNodesForReactFlow(extended);
  const updated = resolveSubflowParentAfterDrag(extended, placed);
  return orderFlowNodesForReactFlow(updated ?? extended);
}
