import type { Connection, Edge, Node } from "@xyflow/react";
import { isBridgeableFlowEdge } from "./bridgeEdgesOnNodeRemoval";
import { appendEtlConnectionEdge, dedupeEdgesByHandles } from "./transformFlowEdgeHelpers";

function connectionFromEdge(e: Edge, targetId: string): Connection {
  return {
    source: e.source,
    sourceHandle: e.sourceHandle ?? "out",
    target: targetId,
    targetHandle: e.targetHandle ?? "in",
  };
}

function connectionToEdge(e: Edge, sourceId: string): Connection {
  return {
    source: sourceId,
    sourceHandle: e.sourceHandle ?? "out",
    target: e.target,
    targetHandle: e.targetHandle ?? "in",
  };
}

function chainConnection(sourceId: string, targetId: string): Connection {
  return {
    source: sourceId,
    sourceHandle: "out",
    target: targetId,
    targetHandle: "in",
  };
}

export function wireEdgesForExplodedNodes(
  edges: Edge[],
  sourceNodeId: string,
  newNodeIds: string[],
  wiring: "ordered" | "parallel",
  getNode: (id: string) => Node | undefined
): Edge[] {
  const incoming = edges.filter(
    (e) => e.target === sourceNodeId && isBridgeableFlowEdge(e, getNode)
  );
  const outgoing = edges.filter(
    (e) => e.source === sourceNodeId && isBridgeableFlowEdge(e, getNode)
  );

  let next = edges.filter((e) => e.source !== sourceNodeId && e.target !== sourceNodeId);

  if (wiring === "ordered") {
    const first = newNodeIds[0]!;
    const last = newNodeIds[newNodeIds.length - 1]!;
    for (const inE of incoming) {
      next = appendEtlConnectionEdge(getNode, next, connectionFromEdge(inE, first));
    }
    for (let i = 0; i < newNodeIds.length - 1; i++) {
      next = appendEtlConnectionEdge(getNode, next, chainConnection(newNodeIds[i]!, newNodeIds[i + 1]!));
    }
    for (const outE of outgoing) {
      next = appendEtlConnectionEdge(getNode, next, connectionToEdge(outE, last));
    }
    return dedupeEdgesByHandles(next);
  }

  for (const inE of incoming) {
    for (const nid of newNodeIds) {
      next = appendEtlConnectionEdge(getNode, next, connectionFromEdge(inE, nid));
    }
  }
  for (const outE of outgoing) {
    for (const nid of newNodeIds) {
      next = appendEtlConnectionEdge(getNode, next, connectionToEdge(outE, nid));
    }
  }
  return dedupeEdgesByHandles(next);
}
