import type { Connection, Edge, Node } from "@xyflow/react";
import type { FlowEdgeData } from "./flowDocumentBridge";
import { appendDiscoveryConnectionEdge, dedupeEdgesByHandles } from "./flowEdgeHelpers";

type GetNode = (id: string) => Node | undefined;

function edgeKind(e: Edge): FlowEdgeData["kind"] {
  return (e.data as FlowEdgeData | undefined)?.kind ?? "data";
}

/** Primary / composition edges that may be spliced when a node is removed. */
export function isBridgeableFlowEdge(e: Edge, getNode: GetNode): boolean {
  const sh = e.sourceHandle ?? "out";
  if (sh === "validation") return false;
  const kind = edgeKind(e);
  if (kind === "data" || kind === "sequence" || kind === "parallel_group") {
    const srcType = getNode(e.source)?.type;
    const tgtType = getNode(e.target)?.type;
    if (srcType === "discoverySubgraph" || tgtType === "discoverySubgraph") return false;
    return true;
  }
  return false;
}

function buildRemovedSubgraphAdjacency(edges: Edge[], toRemove: Set<string>): Map<string, string[]> {
  const adj = new Map<string, string[]>();
  const add = (from: string, to: string) => {
    const list = adj.get(from) ?? [];
    list.push(to);
    adj.set(from, list);
  };
  for (const e of edges) {
    if (!toRemove.has(e.source) || !toRemove.has(e.target)) continue;
    add(e.source, e.target);
  }
  return adj;
}

function reachableWithinRemoved(
  from: string,
  to: string,
  toRemove: Set<string>,
  adj: Map<string, string[]>
): boolean {
  if (from === to) return true;
  if (!toRemove.has(from) || !toRemove.has(to)) return false;
  const seen = new Set<string>([from]);
  const queue = [from];
  while (queue.length) {
    const cur = queue.shift()!;
    for (const next of adj.get(cur) ?? []) {
      if (next === to) return true;
      if (seen.has(next)) continue;
      seen.add(next);
      queue.push(next);
    }
  }
  return false;
}

/**
 * After removing ``toRemove`` nodes, drop incident edges and add bypass edges from each
 * external predecessor to each external successor connected through the removed subgraph.
 */
export function bridgeEdgesOnNodeRemoval(edges: Edge[], toRemove: Set<string>, getNode: GetNode): Edge[] {
  if (toRemove.size === 0) return edges;

  const adj = buildRemovedSubgraphAdjacency(edges, toRemove);
  const incoming = edges.filter((e) => !toRemove.has(e.source) && toRemove.has(e.target));
  const outgoing = edges.filter((e) => !toRemove.has(e.target) && toRemove.has(e.source));

  const bridges: Connection[] = [];
  for (const inE of incoming) {
    if (!isBridgeableFlowEdge(inE, getNode)) continue;
    for (const outE of outgoing) {
      if (!isBridgeableFlowEdge(outE, getNode)) continue;
      if (!reachableWithinRemoved(inE.target, outE.source, toRemove, adj)) continue;
      bridges.push({
        source: inE.source,
        sourceHandle: inE.sourceHandle ?? "out",
        target: outE.target,
        targetHandle: outE.targetHandle ?? "in",
      });
    }
  }

  let next = edges.filter((e) => !toRemove.has(e.source) && !toRemove.has(e.target));
  for (const conn of bridges) {
    next = appendDiscoveryConnectionEdge(getNode, next, conn);
  }
  return dedupeEdgesByHandles(next);
}

/** Edges to persist after deleting nodes (filter + bypass wiring). */
export function edgesAfterRemovingNodes(edges: Edge[], toRemove: Set<string>, getNode: GetNode): Edge[] {
  return bridgeEdgesOnNodeRemoval(edges, toRemove, getNode);
}
