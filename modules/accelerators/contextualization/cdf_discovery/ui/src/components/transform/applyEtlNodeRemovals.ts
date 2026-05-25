import type { Edge, Node } from "@xyflow/react";
import { edgesAfterRemovingNodes } from "./bridgeEdgesOnNodeRemoval";

export type EtlNodeRemovalResult = {
  nodes: Node[];
  edges: Edge[];
  clearedSelectionNodeIds: Set<string>;
};

const NON_REMOVABLE_RF_TYPES = new Set(["etlStart", "etlEnd"]);

/** Remove nodes and splice bypass edges so upstream/downstream data flow stays connected. */
export function applyEtlNodeRemovals(
  nodes: Node[],
  edges: Edge[],
  rootNodeIds: string[]
): EtlNodeRemovalResult | null {
  if (rootNodeIds.length === 0) return null;

  const toRemove = new Set<string>();
  for (const nodeId of rootNodeIds) {
    const root = nodes.find((n) => n.id === nodeId);
    if (!root) continue;
    if (root.type && NON_REMOVABLE_RF_TYPES.has(root.type)) continue;
    const kind =
      root.data && typeof root.data === "object" ? (root.data as { kind?: string }).kind : undefined;
    if (kind === "start" || kind === "end") continue;
    toRemove.add(nodeId);
  }

  if (toRemove.size === 0) return null;

  const getNode = (id: string) => nodes.find((n) => n.id === id);
  return {
    nodes: nodes.filter((n) => !toRemove.has(n.id)),
    edges: edgesAfterRemovingNodes(edges, toRemove, getNode),
    clearedSelectionNodeIds: toRemove,
  };
}
