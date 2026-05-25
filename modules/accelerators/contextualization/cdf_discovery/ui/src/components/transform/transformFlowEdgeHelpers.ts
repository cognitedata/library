import { addEdge, type Connection, type Edge, type Node } from "@xyflow/react";
import { transformFlowEdgeVisualDefaults, type FlowEdgeData } from "./flowDocumentBridge";
import { etlPersistenceOutboundToEndOnlyRfTypes } from "./transformFlowConstants";

/** First ``etlEnd`` node id in document order, if any. */
export function findFirstEtlEndNodeId(nodes: readonly Pick<Node, "id" | "type">[]): string | null {
  for (const n of nodes) {
    if (n.type === "etlEnd") return n.id;
  }
  return null;
}

/** Primary data edge from a persistence node ``out`` to ``etlEnd`` ``in``. */
export function buildPersistenceOutboundToEndDataEdge(persistenceNodeId: string, endNodeId: string): Edge {
  return {
    ...transformFlowEdgeVisualDefaults,
    id: `e_${persistenceNodeId}_${endNodeId}_persistenceEnd_${Date.now()}`,
    source: persistenceNodeId,
    sourceHandle: "out",
    target: endNodeId,
    targetHandle: "in",
    data: { kind: "data" } satisfies FlowEdgeData,
  };
}

/** Auto-wire save nodes to the canvas end when present. */
export function persistenceOutboundEdgesToEnd(
  rfType: string,
  persistenceNodeId: string,
  nodes: readonly Pick<Node, "id" | "type">[]
): Edge[] {
  if (!etlPersistenceOutboundToEndOnlyRfTypes.has(rfType)) return [];
  const endId = findFirstEtlEndNodeId(nodes);
  if (!endId) return [];
  return [buildPersistenceOutboundToEndDataEdge(persistenceNodeId, endId)];
}

export function dedupeEdgesByHandles(edges: Edge[]): Edge[] {
  const seen = new Set<string>();
  const out: Edge[] = [];
  for (const e of edges) {
    const k = `${e.source}\0${e.sourceHandle ?? ""}\0${e.target}\0${e.targetHandle ?? ""}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(e);
  }
  return out;
}

type GetNode = (id: string) => Node | undefined;

export function appendEtlConnectionEdge(_getNode: GetNode, edges: Edge[], params: Connection): Edge[] {
  return addEdge(
    {
      ...params,
      sourceHandle: params.sourceHandle ?? "out",
      targetHandle: params.targetHandle ?? "in",
      ...transformFlowEdgeVisualDefaults,
      data: { kind: "data" } satisfies FlowEdgeData,
    },
    edges
  );
}
