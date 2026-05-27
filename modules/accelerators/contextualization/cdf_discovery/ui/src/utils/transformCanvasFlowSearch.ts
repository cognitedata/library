import type { TransformCanvasNode } from "../types/transformCanvas";
import {
  canvasNodeDisplayLabel,
  canvasNodeKindLabel,
  type CanvasNodeTranslate,
} from "./canvasNodeKindLabel";

function configSearchBits(config: unknown): string[] {
  if (!config || typeof config !== "object" || Array.isArray(config)) return [];
  const o = config as Record<string, unknown>;
  const bits: string[] = [];
  for (const key of [
    "description",
    "sql_query",
    "view_external_id",
    "view_version",
    "view_space",
    "raw_table",
    "raw_db",
    "source_raw_table",
    "source_raw_db",
    "transformation_external_id",
    "function_external_id",
    "workflow_external_id",
    "workflow_base",
    "subworkflow_external_id",
    "cdf_task_external_id",
  ]) {
    const v = o[key];
    if (v != null && String(v).trim()) bits.push(String(v).trim());
  }
  return bits;
}

export function transformCanvasNodeKindLabel(
  node: TransformCanvasNode,
  t: CanvasNodeTranslate
): string {
  return canvasNodeKindLabel(node.kind, t);
}

export function transformCanvasNodeDisplayLabel(
  node: TransformCanvasNode,
  t: CanvasNodeTranslate
): string {
  const label = node.data?.label != null ? String(node.data.label).trim() : "";
  if (label) return label;
  const notes = node.data?.notes != null ? String(node.data.notes).trim() : "";
  if (notes) return notes;
  const desc = configSearchBits(node.data?.config)[0];
  if (desc) return desc;
  return canvasNodeDisplayLabel(node.data, node.kind, t);
}

/** Canvas node title for run results: ``Label (node_id)`` when label differs from id. */
export function formatTransformCanvasNodeLabelWithId(
  node: TransformCanvasNode,
  t: CanvasNodeTranslate
): string {
  const label = transformCanvasNodeDisplayLabel(node, t);
  return label !== node.id ? `${label} (${node.id})` : node.id;
}

export function resolveTransformCanvasNodeForTask(
  canvas: { nodes: TransformCanvasNode[] },
  taskId: string,
  summary?: Record<string, unknown> | null
): TransformCanvasNode | null {
  const fromSummary = summary?.canvas_node_id;
  if (fromSummary != null && String(fromSummary).trim()) {
    const id = String(fromSummary).trim();
    return canvas.nodes.find((n) => n.id === id) ?? null;
  }
  return canvas.nodes.find((n) => n.id === taskId) ?? null;
}

export function transformCanvasNodeMatchesSearch(
  node: TransformCanvasNode,
  query: string,
  t: CanvasNodeTranslate
): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const hay = [
    node.id,
    transformCanvasNodeDisplayLabel(node, t),
    transformCanvasNodeKindLabel(node, t),
    node.kind,
    node.data?.notes != null ? String(node.data.notes) : "",
    ...configSearchBits(node.data?.config),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return hay.includes(q);
}

export function filterTransformCanvasNodesBySearch(
  nodes: TransformCanvasNode[],
  query: string,
  t: CanvasNodeTranslate
): TransformCanvasNode[] {
  const q = query.trim();
  if (!q) return nodes;
  return nodes.filter((n) => transformCanvasNodeMatchesSearch(n, query, t));
}
