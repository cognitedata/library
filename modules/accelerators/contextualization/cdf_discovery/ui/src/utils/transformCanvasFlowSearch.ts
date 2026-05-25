import type { TransformCanvasNode, TransformCanvasNodeKind } from "../types/transformCanvas";

const KIND_LABEL: Record<TransformCanvasNodeKind, string> = {
  start: "Workflow trigger",
  end: "End",
  query_view: "View query",
  query_raw: "RAW query",
  query_classic: "Classic query",
  query_sql: "SQL query",
  score: "Score",
  transform: "Transform",
  field_map: "Field map",
  filter: "Filter",
  join: "Join",
  merge: "Merge",
  build_index: "Build index",
  save_view: "View save",
  save_raw: "RAW save",
  save_classic: "Classic save",
  raw_cleanup: "RAW cleanup",
  spark_transform: "Spark transform",
  transformation_ref: "Transformation ref",
  function_ref: "Function ref",
  dynamic_fanout: "Dynamic fan-out",
  subworkflow: "Sub-workflow",
  simulation: "Simulation",
  cdf_task: "CDF task",
  subgraph: "Subgraph",
};

export function transformCanvasNodeKindLabel(node: TransformCanvasNode): string {
  return KIND_LABEL[node.kind] ?? node.kind.replace(/_/g, " ");
}

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

export function transformCanvasNodeDisplayLabel(node: TransformCanvasNode): string {
  const label = node.data?.label != null ? String(node.data.label).trim() : "";
  if (label) return label;
  const notes = node.data?.notes != null ? String(node.data.notes).trim() : "";
  if (notes) return notes;
  const desc = configSearchBits(node.data?.config)[0];
  if (desc) return desc;
  return node.id;
}

/** Canvas node title for run results: ``Label (node_id)`` when label differs from id. */
export function formatTransformCanvasNodeLabelWithId(node: TransformCanvasNode): string {
  const label = transformCanvasNodeDisplayLabel(node);
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

export function transformCanvasNodeMatchesSearch(node: TransformCanvasNode, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const hay = [
    node.id,
    transformCanvasNodeDisplayLabel(node),
    transformCanvasNodeKindLabel(node),
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
  query: string
): TransformCanvasNode[] {
  const q = query.trim();
  if (!q) return nodes;
  return nodes.filter((n) => transformCanvasNodeMatchesSearch(n, query));
}
