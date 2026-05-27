/**
 * List upstream canvas tasks connected by data edges (for json_mapping reference picker).
 */
import type { Edge, Node } from "@xyflow/react";
import { rfTypeToKind } from "../types/transformCanvas";

export type CanvasPredecessorTask = {
  taskId: string;
  label: string;
};

export type CanvasPredecessorContext = {
  nodes: readonly Node[];
  edges: readonly Edge[];
  nodeId: string;
};

function edgeKind(edge: Edge): string {
  const data = edge.data as { kind?: string } | undefined;
  return data?.kind ?? "data";
}

function isDataEdge(edge: Edge): boolean {
  return edgeKind(edge) === "data";
}

function nodeLabel(node: Node): string {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const label = String(data.label ?? "").trim();
  if (label) return label;
  const kind = typeof data.kind === "string" ? data.kind : rfTypeToKind(node.type);
  return kind.replace(/_/g, " ");
}

export function listDataPredecessorTasks(ctx: CanvasPredecessorContext): CanvasPredecessorTask[] {
  const byId = new Map(ctx.nodes.map((n) => [n.id, n]));
  const seen = new Set<string>();
  const out: CanvasPredecessorTask[] = [];
  for (const edge of ctx.edges) {
    if (!isDataEdge(edge) || edge.target !== ctx.nodeId) continue;
    const src = edge.source?.trim();
    if (!src || seen.has(src)) continue;
    const node = byId.get(src);
    if (!node) continue;
    seen.add(src);
    out.push({ taskId: src, label: nodeLabel(node) });
  }
  out.sort((a, b) => a.taskId.localeCompare(b.taskId));
  return out;
}

export function workflowOutputRef(taskId: string): string {
  return `\${${taskId}.output}`;
}
