import type { Edge, Node } from "@xyflow/react";
import type { WorkflowCanvasHandleOrientation } from "../../types/workflowCanvas";
import { edgesAfterRemovingNodes } from "./bridgeEdgesOnNodeRemoval";
import { collectSubtreeNodeIds } from "./flowParentGeometry";
import { liftSubgraphInnerToParentWorkflow, subgraphHasLiftableInnerContent } from "./liftSubgraphInnerToParent";

export type FlowNodeRemovalResult = {
  nodes: Node[];
  edges: Edge[];
  clearedSelectionNodeIds: Set<string>;
};

type TConfirmLift = (message: string) => boolean;

/**
 * Remove one or more nodes (and their child subtrees) from the flow graph, optionally lifting
 * subgraph contents, and splice bypass edges so upstream/downstream data flow stays connected.
 */
export function applyFlowNodeRemovals(
  nodes: Node[],
  edges: Edge[],
  rootNodeIds: string[],
  handleOrientation: WorkflowCanvasHandleOrientation,
  confirmLift: TConfirmLift,
  liftConfirmMessage: string
): FlowNodeRemovalResult | null {
  if (rootNodeIds.length === 0) return null;

  let curNodes = nodes;
  let curEdges = edges;
  const liftedAway = new Set<string>();

  for (const nodeId of rootNodeIds) {
    const root = curNodes.find((n) => n.id === nodeId);
    if (!root || liftedAway.has(nodeId)) continue;
    if (root.type === "discoverySubgraph") {
      if (
        subgraphHasLiftableInnerContent(curNodes, nodeId) &&
        confirmLift(liftConfirmMessage)
      ) {
        const lifted = liftSubgraphInnerToParentWorkflow(curNodes, curEdges, nodeId, handleOrientation);
        if (lifted) {
          curNodes = lifted.nodes;
          curEdges = lifted.edges;
          liftedAway.add(nodeId);
        }
      }
    }
  }

  const toRemove = new Set<string>();
  for (const nodeId of rootNodeIds) {
    if (liftedAway.has(nodeId)) continue;
    if (!curNodes.some((n) => n.id === nodeId)) continue;
    for (const id of collectSubtreeNodeIds(curNodes, nodeId)) {
      toRemove.add(id);
    }
  }

  if (toRemove.size === 0 && liftedAway.size === 0) return null;

  const getNode = (id: string) => curNodes.find((n) => n.id === id);
  const nextNodes = curNodes.filter((n) => !toRemove.has(n.id));
  const nextEdges = toRemove.size > 0 ? edgesAfterRemovingNodes(curEdges, toRemove, getNode) : curEdges;

  return {
    nodes: nextNodes,
    edges: nextEdges,
    clearedSelectionNodeIds: new Set([...toRemove, ...liftedAway]),
  };
}
