import { kindToRfType, type WorkflowCanvasNode } from "../types/workflowCanvas";

/** Human-readable stage kind for flow search results (matches node card ``__kind`` lines). */
const RF_TYPE_KIND_LABEL: Record<string, string> = {
  discoveryStart: "Start",
  discoveryEnd: "End",
  discoverySourceView: "Source view",
  discoveryViewQuery: "View query",
  discoveryRawQuery: "RAW query",
  discoveryClassicQuery: "Classic query",
  discoverySqlQuery: "SQL query",
  discoveryTransform: "Transform",
  discoveryMerge: "Merge",
  discoveryJoin: "Join",
  discoveryValidate: "Validation",
  discoveryInstanceFilter: "Instance filter",
  discoveryConfidenceFilter: "Confidence filter",
  discoveryViewSave: "View save",
  discoveryRawSave: "RAW save",
  discoveryClassicSave: "Classic save",
  discoveryAliasPersistence: "Alias write-back",
  discoveryInvertedIndex: "Inverted index",
  discoveryMatchValidationRuleSourceView: "Match validation",
  discoveryMatchValidationRuleExtraction: "Match validation",
  discoveryMatchValidationRuleAliasing: "Match validation",
  discoverySubgraph: "Subgraph",
  discoverySubflowGraphIn: "Graph inputs",
  discoverySubflowGraphOut: "Graph outputs",
};

export function canvasNodeKindLabel(node: WorkflowCanvasNode): string {
  const rf = kindToRfType(node.kind);
  return RF_TYPE_KIND_LABEL[rf] ?? node.kind;
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
    "source_raw_table",
    "source_raw_db",
    "handler_id",
    "validation_rule_name",
  ]) {
    const v = o[key];
    if (v != null && String(v).trim()) bits.push(String(v).trim());
  }
  return bits;
}

export function canvasNodeDisplayLabel(node: WorkflowCanvasNode): string {
  const label = node.data?.label != null ? String(node.data.label).trim() : "";
  if (label) return label;
  const cfg = node.data?.config;
  const desc = configSearchBits(cfg)[0];
  if (desc) return desc;
  return node.id;
}

export function canvasNodeMatchesSearch(node: WorkflowCanvasNode, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const hay = [
    node.id,
    canvasNodeDisplayLabel(node),
    canvasNodeKindLabel(node),
    node.kind,
    ...configSearchBits(node.data?.config),
    node.data?.handler_id != null ? String(node.data.handler_id) : "",
    node.data?.validation_rule_name != null ? String(node.data.validation_rule_name) : "",
    node.data?.validation_rule_context != null ? String(node.data.validation_rule_context) : "",
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return hay.includes(q);
}

export function filterCanvasNodesBySearch(
  nodes: WorkflowCanvasNode[],
  query: string
): WorkflowCanvasNode[] {
  const q = query.trim();
  if (!q) return nodes;
  return nodes.filter((n) => canvasNodeMatchesSearch(n, query));
}
