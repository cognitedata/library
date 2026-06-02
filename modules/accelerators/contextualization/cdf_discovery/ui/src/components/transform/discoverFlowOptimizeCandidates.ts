import type { Edge, Node } from "@xyflow/react";
import {
  canMergeScoreSelection,
  isMergeableScoreFlowNode,
  mergeSelectedScoreFlowNodes,
} from "./mergeSelectedScoreNodes";
import {
  canMergeTransformSelection,
  isMergeableTransformFlowNode,
  mergeSelectedTransformFlowNodes,
} from "./mergeSelectedTransformNodes";
import {
  findMaximalNodeChains,
  findParallelNodeGroups,
  orderNodesForMerge,
} from "./flowNodeMergeTopology";

export type FlowOptimizeNodeKind = "transform" | "score";

export type FlowOptimizeCandidateKind = "ordered_chain" | "parallel_siblings";

export type FlowOptimizeCandidate = {
  id: string;
  nodeKind: FlowOptimizeNodeKind;
  kind: FlowOptimizeCandidateKind;
  nodeIds: string[];
  anchorNodeId: string;
};

function candidateId(nodeKind: FlowOptimizeNodeKind, nodeIds: string[]): string {
  return `${nodeKind}:${[...nodeIds].sort().join(",")}`;
}

function discoverMergeCandidatesForKind(
  nodes: Node[],
  edges: Edge[],
  nodeKind: FlowOptimizeNodeKind,
  isMergeable: (node: Node) => boolean,
  canMerge: (group: Node[], edges: Edge[]) => boolean
): FlowOptimizeCandidate[] {
  const peers = nodes.filter(isMergeable);
  const peersById = new Map(peers.map((node) => [node.id, node]));
  const peerIds = new Set(peers.map((n) => n.id));
  const raw: FlowOptimizeCandidate[] = [];

  for (const nodeIds of findMaximalNodeChains(peers, edges)) {
    const group = nodeIds.map((id) => peersById.get(id)).filter((n): n is Node => Boolean(n));
    if (!canMerge(group, edges)) continue;
    raw.push({
      id: candidateId(nodeKind, nodeIds),
      nodeKind,
      kind: "ordered_chain",
      nodeIds,
      anchorNodeId: nodeIds[0]!,
    });
  }

  for (const nodeIds of findParallelNodeGroups(peers, edges, peerIds)) {
    const group = nodeIds.map((id) => peersById.get(id)).filter((n): n is Node => Boolean(n));
    if (!canMerge(group, edges)) continue;
    const ordered = orderNodesForMerge(group, edges).map((n) => n.id);
    raw.push({
      id: candidateId(nodeKind, ordered),
      nodeKind,
      kind: "parallel_siblings",
      nodeIds: ordered,
      anchorNodeId: ordered[0]!,
    });
  }

  const seen = new Set<string>();
  const out: FlowOptimizeCandidate[] = [];
  for (const c of raw) {
    if (seen.has(c.id)) continue;
    seen.add(c.id);
    out.push(c);
  }
  return out;
}

export function discoverTransformMergeCandidates(nodes: Node[], edges: Edge[]): FlowOptimizeCandidate[] {
  return discoverMergeCandidatesForKind(
    nodes,
    edges,
    "transform",
    isMergeableTransformFlowNode,
    canMergeTransformSelection
  );
}

export function discoverScoreMergeCandidates(nodes: Node[], edges: Edge[]): FlowOptimizeCandidate[] {
  return discoverMergeCandidatesForKind(nodes, edges, "score", isMergeableScoreFlowNode, canMergeScoreSelection);
}

export function discoverFlowOptimizeCandidates(nodes: Node[], edges: Edge[]): FlowOptimizeCandidate[] {
  return [
    ...discoverTransformMergeCandidates(nodes, edges),
    ...discoverScoreMergeCandidates(nodes, edges),
  ].sort((a, b) => b.nodeIds.length - a.nodeIds.length || a.id.localeCompare(b.id));
}

export type FlowOptimizeCandidateRow = FlowOptimizeCandidate & {
  approved: boolean;
  conflicts: boolean;
};

export function buildFlowOptimizeCandidateRows(candidates: FlowOptimizeCandidate[]): FlowOptimizeCandidateRow[] {
  const used = new Set<string>();
  return candidates.map((c) => {
    const conflicts = c.nodeIds.some((id) => used.has(id));
    if (!conflicts) {
      for (const id of c.nodeIds) used.add(id);
    }
    return { ...c, approved: !conflicts, conflicts };
  });
}

export function flowCanvasNodeLabel(node: Node | undefined, nodeId: string): string {
  if (!node) return nodeId;
  const data = (node.data ?? {}) as Record<string, unknown>;
  const label = String(data.label ?? "").trim();
  return label || nodeId;
}

export function applyFlowOptimizeCandidates(
  nodes: Node[],
  edges: Edge[],
  approved: FlowOptimizeCandidate[]
): { nodes: Node[]; edges: Edge[]; appliedCount: number } | null {
  if (approved.length === 0) return null;

  let curNodes = nodes;
  let curEdges = edges;
  const consumed = new Set<string>();
  let appliedCount = 0;

  const sorted = [...approved].sort((a, b) => b.nodeIds.length - a.nodeIds.length);
  for (const c of sorted) {
    if (c.nodeIds.some((id) => consumed.has(id))) continue;
    const liveIds = c.nodeIds.filter((id) => curNodes.some((n) => n.id === id));
    if (liveIds.length < 2) continue;
    const anchorId = curNodes.some((n) => n.id === c.anchorNodeId) ? c.anchorNodeId : liveIds[0]!;

    const result =
      c.nodeKind === "score"
        ? mergeSelectedScoreFlowNodes(curNodes, curEdges, liveIds, anchorId)
        : mergeSelectedTransformFlowNodes(curNodes, curEdges, liveIds, anchorId);
    if (!result) continue;
    curNodes = result.nodes;
    curEdges = result.edges;
    for (const id of c.nodeIds) consumed.add(id);
    appliedCount += 1;
  }

  if (appliedCount === 0) return null;
  return { nodes: curNodes, edges: curEdges, appliedCount };
}

