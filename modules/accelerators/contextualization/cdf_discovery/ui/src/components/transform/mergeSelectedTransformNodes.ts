import type { Edge, Node } from "@xyflow/react";
import type { JsonObject } from "../../types/jsonConfig";
import type { ExecutionMode } from "../../utils/etlPipelineStepsModel";
import {
  materializeTransformSteps,
  parseTransformNodeConfig,
  serializeTransformNodeConfig,
} from "../../utils/etlTransformNodeConfigModel";
import { readTransformFields } from "../../utils/etlTransformHandlerTemplates";
import type { FlowEdgeData } from "./flowDocumentBridge";
import { edgesAfterRemovingNodes } from "./bridgeEdgesOnNodeRemoval";
import { rfTypeToKind } from "../../types/transformCanvas";

export function isMergeableTransformFlowNode(node: Node): boolean {
  if (rfTypeToKind(node.type) !== "transform") return false;
  const cfg = readTransformNodeConfig(node);
  if (!cfg) return false;
  return materializeTransformSteps(cfg).length > 0;
}

function readTransformNodeConfig(node: Node): JsonObject | null {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const cfg = data.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) return null;
  return cfg as JsonObject;
}

function isMergeFlowEdge(e: Edge): boolean {
  const kind = ((e.data ?? {}) as FlowEdgeData).kind ?? "data";
  return kind === "data" || kind === "sequence" || kind === "parallel_group";
}

function mergeTopologyEdges(edges: Edge[], selectedIds: Set<string>): Edge[] {
  return edges.filter((e) => {
    if (!selectedIds.has(e.source) || !selectedIds.has(e.target)) return false;
    const kind = ((e.data ?? {}) as FlowEdgeData).kind ?? "data";
    return kind === "data" || kind === "sequence";
  });
}

function neighborSetKey(ids: Set<string>): string {
  return [...ids].sort().join("\0");
}

/** Predecessors outside the selection that connect into ``nodeId``. */
export function externalTransformPredecessors(
  nodeId: string,
  selectedIds: Set<string>,
  edges: Edge[]
): Set<string> {
  const out = new Set<string>();
  for (const e of edges) {
    if (!isMergeFlowEdge(e)) continue;
    if (e.target !== nodeId || !selectedIds.has(e.target)) continue;
    if (selectedIds.has(e.source)) continue;
    out.add(e.source);
  }
  return out;
}

/** Successors outside the selection that ``nodeId`` connects into. */
export function externalTransformSuccessors(
  nodeId: string,
  selectedIds: Set<string>,
  edges: Edge[]
): Set<string> {
  const out = new Set<string>();
  for (const e of edges) {
    if (!isMergeFlowEdge(e)) continue;
    if (e.source !== nodeId || !selectedIds.has(e.source)) continue;
    if (selectedIds.has(e.target)) continue;
    out.add(e.target);
  }
  return out;
}

/** Parallel siblings: identical external predecessors and successors, no internal chain edges. */
export function hasSharedTransformBoundaries(nodes: Node[], edges: Edge[]): boolean {
  if (nodes.length < 2) return false;
  const ids = new Set(nodes.map((n) => n.id));
  if (mergeTopologyEdges(edges, ids).length > 0) return false;

  let predKey: string | null = null;
  let succKey: string | null = null;
  let preds: Set<string> | null = null;
  let succs: Set<string> | null = null;
  for (const n of nodes) {
    const p = externalTransformPredecessors(n.id, ids, edges);
    const s = externalTransformSuccessors(n.id, ids, edges);
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

/**
 * Valid merge: every node is a transform, and either a continuous internal sequence
 * or parallel siblings with the same external predecessors and successors.
 */
export function canMergeTransformSelection(nodes: Node[], edges: Edge[]): boolean {
  if (nodes.length < 2) return false;
  if (!nodes.every(isMergeableTransformFlowNode)) return false;
  if (isSequentialTransformSelection(nodes, edges)) return true;
  return hasSharedTransformBoundaries(nodes, edges);
}

/** Selected transforms form one directed path (n nodes, n−1 edges, degree ≤ 1). */
export function isSequentialTransformSelection(nodes: Node[], edges: Edge[]): boolean {
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

function topologicalTransformOrder(nodes: Node[], edges: Edge[]): Node[] {
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

function canvasPositionOrder(nodes: Node[]): Node[] {
  return [...nodes].sort(
    (a, b) => a.position.x - b.position.x || a.position.y - b.position.y || a.id.localeCompare(b.id)
  );
}

export function orderTransformNodesForMerge(nodes: Node[], edges: Edge[]): Node[] {
  if (isSequentialTransformSelection(nodes, edges)) {
    return topologicalTransformOrder(nodes, edges);
  }
  return canvasPositionOrder(nodes);
}

function stepsFromTransformNode(node: Node): JsonObject[] {
  const cfg = readTransformNodeConfig(node);
  if (!cfg) return [];
  return materializeTransformSteps(cfg);
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function rewireStepFieldsForOrderedSuccessor(
  step: JsonObject,
  prevOriginalOutput: string,
  prevAlias: string
): JsonObject {
  const prev = prevOriginalOutput.trim();
  const alias = prevAlias.trim();
  if (!alias) return step;

  let fields = readTransformFields(step as Record<string, unknown>);
  if (fields.length === 0) {
    fields = [{ field_name: alias }];
  } else {
    let wired = false;
    fields = fields.map((row) => {
      const name = String(row.field_name ?? row.name ?? "").trim();
      if (prev && name === prev) {
        wired = true;
        return { ...row, field_name: alias };
      }
      return row;
    });
    if (prev && !wired) {
      fields = [{ ...fields[0], field_name: alias }, ...fields.slice(1)];
    } else if (!prev) {
      fields = [{ ...fields[0], field_name: alias }, ...fields.slice(1)];
    }
  }

  let output_template = String(step.output_template ?? "");
  if (prev && output_template.includes(`{${prev}}`)) {
    output_template = output_template.replace(new RegExp(`\\{${escapeRegExp(prev)}\\}`, "g"), `{${alias}}`);
  } else if (!output_template.trim()) {
    output_template = `{${alias}}`;
  }

  return { ...step, fields, output_template };
}

export function wireOrderedMergeSteps(steps: JsonObject[]): JsonObject[] {
  if (steps.length <= 1) return steps.map((s) => ({ ...s }));

  const result: JsonObject[] = [];
  let prevOriginalOutput = "";

  for (let i = 0; i < steps.length; i++) {
    let step = { ...steps[i]! };
    const originalOutput = String(step.output_field ?? "").trim();

    if (i > 0) {
      step = rewireStepFieldsForOrderedSuccessor(step, prevOriginalOutput, `_mergeStep${i - 1}`);
    }

    if (i < steps.length - 1) {
      step = { ...step, output_field: `_mergeStep${i}` };
    } else if (originalOutput) {
      step = { ...step, output_field: originalOutput };
    }

    prevOriginalOutput = originalOutput;
    result.push(step);
  }
  return result;
}

export function buildDefaultFieldPoliciesForParallelSteps(steps: JsonObject[]): unknown[] | undefined {
  const counts = new Map<string, number>();
  for (const s of steps) {
    const out = String(s.output_field ?? "").trim();
    if (out) counts.set(out, (counts.get(out) ?? 0) + 1);
  }
  const dupes = [...counts.entries()].filter(([, c]) => c > 1).map(([p]) => p);
  if (dupes.length === 0) return undefined;
  return dupes.map((property) => ({
    property,
    strategy: "merge_list",
    merge_list: { unique: true, branch_order: "by_score" },
  }));
}

export function buildMergedTransformConfigFromNodes(
  orderedNodes: Node[],
  edges: Edge[],
  anchorNodeId: string
): JsonObject | null {
  const rawSteps: JsonObject[] = [];
  for (const n of orderedNodes) {
    rawSteps.push(...stepsFromTransformNode(n));
  }
  if (rawSteps.length < 2) return null;

  const sequential = isSequentialTransformSelection(orderedNodes, edges);
  const executionMode: ExecutionMode = sequential ? "ordered" : "parallel";
  const steps = sequential ? wireOrderedMergeSteps(rawSteps) : rawSteps.map((s) => ({ ...s }));

  const anchor = orderedNodes.find((n) => n.id === anchorNodeId) ?? orderedNodes[0]!;
  const anchorCfg = readTransformNodeConfig(anchor) ?? {};
  const parsed = parseTransformNodeConfig(anchorCfg);

  return serializeTransformNodeConfig({
    description: parsed.description,
    executionMode,
    steps,
    fieldPolicies:
      executionMode === "parallel"
        ? (buildDefaultFieldPoliciesForParallelSteps(steps) ?? parsed.fieldPolicies)
        : parsed.fieldPolicies,
    multiStep: true,
    extras: parsed.extras,
  });
}

export type MergeSelectedTransformResult = {
  nodes: Node[];
  edges: Edge[];
  anchorNodeId: string;
};

/**
 * Merge multiple canvas transform nodes into one multi-step transform on ``anchorNodeId``.
 * Removes the other selected transform nodes. Sequential merges splice bypass edges;
 * parallel merges keep only the anchor's external connections.
 */
export function mergeSelectedTransformFlowNodes(
  nodes: Node[],
  edges: Edge[],
  selectedIds: string[],
  anchorNodeId: string
): MergeSelectedTransformResult | null {
  const selectedSet = new Set(selectedIds);
  const transformNodes = nodes.filter((n) => selectedSet.has(n.id) && isMergeableTransformFlowNode(n));
  if (transformNodes.length < 2) return null;
  if (!transformNodes.some((n) => n.id === anchorNodeId)) return null;
  if (!canMergeTransformSelection(transformNodes, edges)) return null;

  const ordered = orderTransformNodesForMerge(transformNodes, edges);
  const mergedConfig = buildMergedTransformConfigFromNodes(ordered, edges, anchorNodeId);
  if (!mergedConfig) return null;

  const anchor = nodes.find((n) => n.id === anchorNodeId);
  if (!anchor) return null;

  const withMergedAnchor = nodes.map((n) => {
    if (n.id !== anchorNodeId) return n;
    const data = { ...(n.data as Record<string, unknown>), config: mergedConfig };
    return { ...n, data };
  });

  const toRemove = new Set(transformNodes.filter((n) => n.id !== anchorNodeId).map((n) => n.id));
  const nextNodes = withMergedAnchor.filter((n) => !toRemove.has(n.id));
  const getNode = (id: string) => withMergedAnchor.find((n) => n.id === id);
  const sequential = isSequentialTransformSelection(transformNodes, edges);
  const nextEdges = edgesAfterRemovingNodes(edges, toRemove, getNode, { bridge: sequential });

  return { nodes: nextNodes, edges: nextEdges, anchorNodeId };
}
