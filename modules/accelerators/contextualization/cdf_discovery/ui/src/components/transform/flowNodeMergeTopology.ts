import type { Edge, Node } from "@xyflow/react";
import type { FlowEdgeData } from "./flowDocumentBridge";

export function isMergeFlowEdge(e: Edge): boolean {
  const kind = ((e.data ?? {}) as FlowEdgeData).kind ?? "data";
  return kind === "data" || kind === "sequence" || kind === "parallel_group";
}

export function mergeTopologyEdges(edges: Edge[], selectedIds: Set<string>): Edge[] {
  return edges.filter((e) => {
    if (!selectedIds.has(e.source) || !selectedIds.has(e.target)) return false;
    const kind = ((e.data ?? {}) as FlowEdgeData).kind ?? "data";
    return kind === "data" || kind === "sequence";
  });
}

function neighborSetKey(ids: Set<string>): string {
  return [...ids].sort().join("\0");
}

export function externalNodePredecessors(nodeId: string, selectedIds: Set<string>, edges: Edge[]): Set<string> {
  const out = new Set<string>();
  for (const e of edges) {
    if (!isMergeFlowEdge(e)) continue;
    if (e.target !== nodeId || !selectedIds.has(e.target)) continue;
    if (selectedIds.has(e.source)) continue;
    out.add(e.source);
  }
  return out;
}

export function externalNodeSuccessors(nodeId: string, selectedIds: Set<string>, edges: Edge[]): Set<string> {
  const out = new Set<string>();
  for (const e of edges) {
    if (!isMergeFlowEdge(e)) continue;
    if (e.source !== nodeId || !selectedIds.has(e.source)) continue;
    if (selectedIds.has(e.target)) continue;
    out.add(e.target);
  }
  return out;
}

export function hasSharedNodeBoundaries(nodes: Node[], edges: Edge[]): boolean {
  if (nodes.length < 2) return false;
  const ids = new Set(nodes.map((n) => n.id));
  if (mergeTopologyEdges(edges, ids).length > 0) return false;

  let predKey: string | null = null;
  let succKey: string | null = null;
  let preds: Set<string> | null = null;
  let succs: Set<string> | null = null;
  for (const n of nodes) {
    const p = externalNodePredecessors(n.id, ids, edges);
    const s = externalNodeSuccessors(n.id, ids, edges);
    const pk = neighborSetKey(p);
    const sk = neighborSetKey(s);
    if (predKey === null) {
      predKey = pk;
      succKey = sk;
      preds = p;
      succs = s;
      continue;
    }
    if (predKey !== pk || succKey !== sk) return false;
  }
  if (!preds || !succs) return false;
  return preds.size > 0 || succs.size > 0;
}

export function isSequentialNodeSelection(nodes: Node[], edges: Edge[]): boolean {
  if (nodes.length < 2) return false;
  const ids = new Set(nodes.map((n) => n.id));
  const internal = mergeTopologyEdges(edges, ids);
  if (internal.length !== nodes.length - 1) return false;

  const inDeg = new Map<string, number>();
  const outDeg = new Map<string, number>();
  for (const id of ids) {
    inDeg.set(id, 0);
    outDeg.set(id, 0);
  }
  for (const e of internal) {
    outDeg.set(e.source, (outDeg.get(e.source) ?? 0) + 1);
    inDeg.set(e.target, (inDeg.get(e.target) ?? 0) + 1);
  }
  for (const id of ids) {
    if ((inDeg.get(id) ?? 0) > 1 || (outDeg.get(id) ?? 0) > 1) return false;
  }
  return true;
}

export function findMaximalNodeChains(nodes: Node[], edges: Edge[]): string[][] {
  const ids = new Set(nodes.map((n) => n.id));
  const internal = mergeTopologyEdges(edges, ids);
  const inNeighbors = new Map<string, string[]>();
  const outNeighbors = new Map<string, string[]>();
  for (const id of ids) {
    inNeighbors.set(id, []);
    outNeighbors.set(id, []);
  }
  for (const e of internal) {
    outNeighbors.get(e.source)!.push(e.target);
    inNeighbors.get(e.target)!.push(e.source);
  }

  const chains: string[][] = [];
  for (const head of [...ids].sort()) {
    if ((inNeighbors.get(head)?.length ?? 0) !== 0) continue;
    const chain = [head];
    let cur = head;
    while (true) {
      const outs = outNeighbors.get(cur) ?? [];
      if (outs.length !== 1) break;
      const next = outs[0]!;
      if ((inNeighbors.get(next)?.length ?? 0) !== 1) break;
      chain.push(next);
      cur = next;
    }
    if (chain.length >= 2) chains.push(chain);
  }
  return chains;
}

export function topologicalNodeOrder(nodes: Node[], edges: Edge[]): Node[] {
  const ids = new Set(nodes.map((n) => n.id));
  const internal = mergeTopologyEdges(edges, ids);
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const inDeg = new Map<string, number>();
  const adj = new Map<string, string[]>();
  for (const id of ids) inDeg.set(id, 0);
  for (const e of internal) {
    inDeg.set(e.target, (inDeg.get(e.target) ?? 0) + 1);
    const list = adj.get(e.source) ?? [];
    list.push(e.target);
    adj.set(e.source, list);
  }
  const queue = [...ids].filter((id) => (inDeg.get(id) ?? 0) === 0).sort();
  const ordered: Node[] = [];
  while (queue.length) {
    const id = queue.shift()!;
    const node = byId.get(id);
    if (node) ordered.push(node);
    for (const next of adj.get(id) ?? []) {
      const d = (inDeg.get(next) ?? 1) - 1;
      inDeg.set(next, d);
      if (d === 0) {
        queue.push(next);
        queue.sort();
      }
    }
  }
  if (ordered.length === nodes.length) return ordered;
  return [...nodes].sort(
    (a, b) => a.position.x - b.position.x || a.position.y - b.position.y || a.id.localeCompare(b.id)
  );
}

export function canvasPositionOrder(nodes: Node[]): Node[] {
  return [...nodes].sort(
    (a, b) => a.position.x - b.position.x || a.position.y - b.position.y || a.id.localeCompare(b.id)
  );
}

export function orderNodesForMerge(nodes: Node[], edges: Edge[]): Node[] {
  if (isSequentialNodeSelection(nodes, edges)) {
    return topologicalNodeOrder(nodes, edges);
  }
  return canvasPositionOrder(nodes);
}

export function findParallelNodeGroups(
  nodes: Node[],
  edges: Edge[],
  allPeerIds: Set<string>
): string[][] {
  const buckets = new Map<string, Node[]>();
  for (const n of nodes) {
    const preds = new Set<string>();
    const succs = new Set<string>();
    for (const e of edges) {
      if (!isMergeFlowEdge(e)) continue;
      if (e.target === n.id && !allPeerIds.has(e.source)) preds.add(e.source);
      if (e.source === n.id && !allPeerIds.has(e.target)) succs.add(e.target);
    }
    const key = `${[...preds].sort().join("\0")}\x01${[...succs].sort().join("\0")}`;
    const list = buckets.get(key) ?? [];
    list.push(n);
    buckets.set(key, list);
  }
  return [...buckets.values()].filter((g) => g.length >= 2).map((g) => g.map((n) => n.id));
}
