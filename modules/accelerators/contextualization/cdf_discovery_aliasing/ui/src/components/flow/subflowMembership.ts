import type { Node } from "@xyflow/react";
import { isSubflowGraphHubRfType } from "../../types/workflowCanvas";

/** Nodes that may have ``parentId`` cleared when lifting from a subgraph. */
export function canChangeSubflowParent(nodeType: string | undefined): boolean {
  if (!nodeType) return false;
  if (nodeType === "keaStart" || nodeType === "keaEnd") return false;
  if (isSubflowGraphHubRfType(nodeType)) return false;
  return true;
}

/** Nodes that may be grouped into a new ``keaSubgraph`` via collapse selection. */
export function isWrapGroupableNodeType(nodeType: string | undefined): boolean {
  if (!canChangeSubflowParent(nodeType)) return false;
  if (nodeType === "keaSubgraph") return false;
  return true;
}

export function isWrapGroupableNode(n: Node): boolean {
  return Boolean(n.selected && isWrapGroupableNodeType(n.type));
}

/**
 * Nodes that should participate in wrap/collapse actions: union of React Flow's selection
 * (including shift+drag box selection via ``useOnSelectionChange``), ``node.selected`` flags,
 * and the context-menu target node when it is groupable.
 */
export function resolveGroupableSelectionNodes(
  nodes: Node[],
  contextNode: Node,
  rfSelectionSnapshot: readonly Node[]
): Node[] {
  const selIds = new Set<string>();
  for (const n of nodes) {
    if (n.selected) selIds.add(n.id);
  }
  for (const n of rfSelectionSnapshot) {
    selIds.add(n.id);
  }
  if (isWrapGroupableNodeType(contextNode.type)) {
    selIds.add(contextNode.id);
  }
  return nodes.filter((n) => selIds.has(n.id) && isWrapGroupableNodeType(n.type));
}
