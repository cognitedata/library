import { reconnectEdge, type Connection, type Edge, type Node } from "@xyflow/react";
import { isValidEtlFlowConnection, wouldCreateCycle } from "./etlFlowConnections";
import { dedupeEdgesByHandles } from "./transformFlowEdgeHelpers";

type GetNode = (id: string) => Node | undefined;

/** Apply reconnect when validation passes; otherwise return the previous edge list. */
export function reconnectTransformFlowEdge(
  oldEdge: Edge,
  newConnection: Connection,
  edges: Edge[],
  getNode: GetNode
): Edge[] {
  if (!isValidEtlFlowConnection(newConnection, getNode)) return edges;
  const withoutOld = edges.filter((e) => e.id !== oldEdge.id);
  if (wouldCreateCycle(withoutOld, newConnection.source, newConnection.target)) return edges;
  const next = reconnectEdge(oldEdge, newConnection, edges);
  return dedupeEdgesByHandles(next);
}
