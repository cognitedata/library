import type { Edge, Node } from "@xyflow/react";
import { dedupeEdgesByHandles } from "./transformFlowEdgeHelpers";
import {
  buildTransformFlowClipboardPayload,
  pasteTransformFlowClipboard,
} from "./transformFlowClipboard";

const DUPLICATE_OFFSET = { x: 40, y: 40 };

/** Duplicate the current React Flow selection in place (new ids). */
export function duplicateTransformFlowSelection(
  nodes: Node[],
  edges: Edge[],
  selected: Node[]
): { nodes: Node[]; edges: Edge[]; newNodeIds: string[] } | null {
  const payload = buildTransformFlowClipboardPayload(nodes, edges, selected);
  if (!payload) return null;
  const existingNodeIds = new Set(nodes.map((n) => n.id));
  const result = pasteTransformFlowClipboard(nodes, edges, payload, {
    offset: DUPLICATE_OFFSET,
    existingNodeIds,
  });
  if (!result) return null;
  return {
    nodes: result.nodes,
    edges: dedupeEdgesByHandles(result.edges),
    newNodeIds: result.newNodeIds,
  };
}
