import type { Edge, Node } from "@xyflow/react";
import type { JsonObject } from "../../types/jsonConfig";
import { rfTypeToKind } from "../../types/transformCanvas";
import { readScoreFields, readScoringRules } from "../../utils/scoreNodeConfigModel";
import { edgesAfterRemovingNodes } from "./bridgeEdgesOnNodeRemoval";
import {
  hasSharedNodeBoundaries,
  isSequentialNodeSelection,
  orderNodesForMerge,
} from "./flowNodeMergeTopology";

function readScoreNodeConfig(node: Node): JsonObject | null {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const cfg = data.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) return null;
  return cfg as JsonObject;
}

export function isMergeableScoreFlowNode(node: Node): boolean {
  if (rfTypeToKind(node.type) !== "score") return false;
  const cfg = readScoreNodeConfig(node);
  if (!cfg) return false;
  return readScoreFields(cfg).length > 0 && readScoringRules(cfg).length > 0;
}

export function canMergeScoreSelection(nodes: Node[], edges: Edge[]): boolean {
  if (nodes.length < 2) return false;
  if (!nodes.every(isMergeableScoreFlowNode)) return false;
  if (isSequentialNodeSelection(nodes, edges)) return true;
  return hasSharedNodeBoundaries(nodes, edges);
}

function copyScoreThresholdFields(target: JsonObject, source: JsonObject): void {
  if (source.min_threshold_filter_enabled === true) {
    target.min_threshold_filter_enabled = true;
    if (source.min_threshold != null) target.min_threshold = source.min_threshold;
  }
}

export function buildMergedScoreConfig(orderedNodes: Node[], anchorNodeId: string): JsonObject | null {
  const anchor = orderedNodes.find((n) => n.id === anchorNodeId) ?? orderedNodes[0];
  if (!anchor) return null;
  const anchorCfg = readScoreNodeConfig(anchor);
  if (!anchorCfg) return null;

  const fields = new Set<string>();
  const rules: unknown[] = [];
  for (const n of orderedNodes) {
    const cfg = readScoreNodeConfig(n);
    if (!cfg) return null;
    for (const f of readScoreFields(cfg)) fields.add(f);
    rules.push(...readScoringRules(cfg));
  }
  if (fields.size === 0 || rules.length === 0) return null;

  const out: JsonObject = {
    description: String(anchorCfg.description ?? "").trim(),
    score_fields: [...fields],
    scoring_rules: rules,
    initial_score: anchorCfg.initial_score ?? 1.0,
    min_score: anchorCfg.min_score ?? 0.0,
  };
  copyScoreThresholdFields(out, anchorCfg);
  return out;
}

export type MergeSelectedScoreResult = {
  nodes: Node[];
  edges: Edge[];
  anchorNodeId: string;
};

export function mergeSelectedScoreFlowNodes(
  nodes: Node[],
  edges: Edge[],
  selectedIds: string[],
  anchorNodeId: string
): MergeSelectedScoreResult | null {
  const selectedSet = new Set(selectedIds);
  const scoreNodes = nodes.filter((n) => selectedSet.has(n.id) && isMergeableScoreFlowNode(n));
  if (scoreNodes.length < 2) return null;
  if (!scoreNodes.some((n) => n.id === anchorNodeId)) return null;
  if (!canMergeScoreSelection(scoreNodes, edges)) return null;

  const ordered = orderNodesForMerge(scoreNodes, edges);
  const mergedConfig = buildMergedScoreConfig(ordered, anchorNodeId);
  if (!mergedConfig) return null;

  const withMergedAnchor = nodes.map((n) => {
    if (n.id !== anchorNodeId) return n;
    const data = { ...(n.data as Record<string, unknown>), config: mergedConfig };
    return { ...n, data };
  });

  const toRemove = new Set(scoreNodes.filter((n) => n.id !== anchorNodeId).map((n) => n.id));
  const nextNodes = withMergedAnchor.filter((n) => !toRemove.has(n.id));
  const getNode = (id: string) => withMergedAnchor.find((n) => n.id === id);
  const sequential = isSequentialNodeSelection(scoreNodes, edges);
  const nextEdges = edgesAfterRemovingNodes(edges, toRemove, getNode, { bridge: sequential });

  return { nodes: nextNodes, edges: nextEdges, anchorNodeId };
}
