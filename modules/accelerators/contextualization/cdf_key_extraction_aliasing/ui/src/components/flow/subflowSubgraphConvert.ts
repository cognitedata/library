import type { Edge, Node } from "@xyflow/react";
import { type WorkflowCanvasHandleOrientation, type WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import { orderFlowNodesForReactFlow } from "./flowDocumentBridge";
import { collapseSelectionToSubgraph } from "./collapseSelectionToSubgraph";
import { collectSubflowFrameAndHubIds } from "./subflowDeleteLift";
import { liftSubgraphInnerToParentWorkflow, subgraphHasLiftableInnerContent } from "./liftSubgraphInnerToParent";
import { isWrapGroupableNodeType } from "./subflowMembership";
import { wrapSelectionInNewSubflow } from "./wrapSelectionInSubflow";
import { dedupeEdgesByHandles } from "./flowEdgeHelpers";

function directChildIdsOfSubflow(nodes: Node[], subflowId: string): Node[] {
  return nodes.filter((n) => {
    const pid = n.parentId != null && String(n.parentId).trim() ? String(n.parentId).trim() : "";
    return pid === subflowId;
  });
}

/**
 * True when the subflow has at least one groupable pipeline child and no other direct children
 * (subgraph, nested subflow, etc.) besides optional graph hub nodes.
 */
export function subflowCanConvertToSubgraph(nodes: Node[], subflowId: string): boolean {
  const sf = nodes.find((n) => n.id === subflowId && n.type === "keaSubflow");
  if (!sf) return false;
  const data = (sf.data ?? {}) as WorkflowCanvasNodeData;
  const explicitHub = new Set(
    [String(data.subflow_hub_input_id ?? "").trim(), String(data.subflow_hub_output_id ?? "").trim()].filter(Boolean)
  );
  const children = directChildIdsOfSubflow(nodes, subflowId);
  let groupable = 0;
  for (const n of children) {
    if (n.type === "keaSubflowGraphIn" || n.type === "keaSubflowGraphOut") continue;
    if (explicitHub.has(n.id)) continue;
    if (!isWrapGroupableNodeType(n.type)) return false;
    groupable++;
  }
  return groupable >= 1;
}

/**
 * Replace a ``keaSubflow`` frame (and its hub children) with a ``keaSubgraph`` whose inner canvas
 * holds the former groupable children, preserving crossing edges like ``collapseSelectionToSubgraph``.
 */
export function convertSubflowToSubgraph(
  nodes: Node[],
  edges: Edge[],
  subflowId: string,
  handleOrientation: WorkflowCanvasHandleOrientation
): { nodes: Node[]; edges: Edge[] } | null {
  if (!subflowCanConvertToSubgraph(nodes, subflowId)) return null;
  const childMembers = directChildIdsOfSubflow(nodes, subflowId).filter((n) => isWrapGroupableNodeType(n.type));
  if (childMembers.length < 1) return null;

  const collapsed = collapseSelectionToSubgraph(nodes, edges, childMembers, handleOrientation);
  if (!collapsed) return null;

  const stripIds = collectSubflowFrameAndHubIds(nodes, subflowId);
  const nextNodes = collapsed.nodes.filter((n) => !stripIds.has(n.id));
  const alive = new Set(nextNodes.map((n) => n.id));
  const nextEdges = dedupeEdgesByHandles(
    collapsed.edges.filter((e) => alive.has(e.source) && alive.has(e.target))
  );
  return { nodes: orderFlowNodesForReactFlow(nextNodes), edges: nextEdges };
}

/**
 * Replace a ``keaSubgraph`` with a ``keaSubflow`` frame containing the former inner pipeline nodes
 * (re-laid out by ``wrapSelectionInNewSubflow``). Requires liftable inner content.
 */
export function convertSubgraphToSubflow(
  nodes: Node[],
  edges: Edge[],
  subgraphId: string,
  handleOrientation: WorkflowCanvasHandleOrientation
): { nodes: Node[]; edges: Edge[] } | null {
  if (!subgraphHasLiftableInnerContent(nodes, subgraphId)) return null;
  const beforeIds = new Set(nodes.map((n) => n.id));
  const lifted = liftSubgraphInnerToParentWorkflow(nodes, edges, subgraphId, handleOrientation);
  if (!lifted) return null;
  const newMemberNodes = lifted.nodes.filter((n) => !beforeIds.has(n.id));
  if (newMemberNodes.length < 1) return null;
  const wrapped = wrapSelectionInNewSubflow(lifted.nodes, newMemberNodes);
  if (!wrapped) return null;
  return { nodes: wrapped, edges: lifted.edges };
}
