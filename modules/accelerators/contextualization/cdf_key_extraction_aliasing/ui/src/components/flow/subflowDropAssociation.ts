import type { Node } from "@xyflow/react";
import { orderFlowNodesForReactFlow } from "./flowDocumentBridge";
import { applyNodeParentChange } from "./flowParentGeometry";
import { canChangeSubflowParent } from "./subflowMembership";

/**
 * Clear or keep root placement for nodes that used to reparent under a subflow frame (removed).
 * Assigning a non-null parent is ignored (root canvas only; nesting uses subgraph ``inner_canvas``).
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
  if (next != null) return nodes;
  return orderFlowNodesForReactFlow(applyNodeParentChange(nodes, nodeId, null));
}

/** Legacy hook: subflow frames are gone; drag no longer changes ``parentId``. */
export function resolveSubflowParentAfterDrag(_nodes: Node[], _dragged: Node): Node[] | null {
  return null;
}

/** Legacy hook: subflow group drag is a no-op. */
export function resolveSubflowParentsAfterGroupDrag(_nodes: Node[], _primaryId: string): Node[] | null {
  return null;
}

/** Append a palette-created node at the root canvas (no subflow containment). */
export function appendNodeAndResolveSubflowParent(nds: Node[], node: Node): Node[] {
  return orderFlowNodesForReactFlow(nds.concat(node));
}
