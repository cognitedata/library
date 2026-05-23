import type { JsonObject } from "../types/scopeConfig";
import type {
  CanvasNodeKind,
  WorkflowCanvasDocument,
  WorkflowCanvasNode,
  WorkflowCanvasNodeData,
} from "../types/workflowCanvas";
import { newNodeId } from "../components/flow/flowDocumentBridge";

export const QUERY_NODE_KINDS = ["query_view", "query_raw", "query_classic", "query_sql"] as const;
export type QueryNodeKind = (typeof QUERY_NODE_KINDS)[number];

export function isQueryNodeKind(k: string | undefined): k is QueryNodeKind {
  return QUERY_NODE_KINDS.includes(k as QueryNodeKind);
}

export function listQueryNodes(canvas: WorkflowCanvasDocument): WorkflowCanvasNode[] {
  return canvas.nodes.filter((n) => isQueryNodeKind(n.kind));
}

export const SAVE_NODE_KINDS = ["save_view", "save_raw", "save_classic"] as const;
export type SaveNodeKind = (typeof SAVE_NODE_KINDS)[number];

export function isSaveNodeKind(k: string | undefined): k is SaveNodeKind {
  return SAVE_NODE_KINDS.includes(k as SaveNodeKind);
}

export function listSaveNodes(canvas: WorkflowCanvasDocument): WorkflowCanvasNode[] {
  return canvas.nodes.filter((n) => isSaveNodeKind(n.kind));
}

export function saveNodeListLabel(node: WorkflowCanvasNode): string {
  const cfg = readNodeConfig(node);
  if (node.kind === "save_view") {
    const ext = String(cfg.view_external_id ?? "").trim();
    if (ext) return ext;
  }
  const desc = String(cfg.description ?? "").trim();
  if (desc) return desc;
  const label = String(node.data?.label ?? "").trim();
  if (label) return label;
  return node.id;
}

export function defaultQueryConfig(kind: QueryNodeKind, schema_space = ""): JsonObject {
  const space = schema_space.trim() || "cdf_cdm";
  switch (kind) {
    case "query_view":
      return {
        description: "View query",
        view_space: space,
        view_external_id: "",
        view_version: "v1",
        filters: [],
        include_properties: [],
      };
    case "query_raw":
      return {
        description: "RAW query",
        source_raw_db: "",
        source_raw_table_key: "",
        read_limit: 100,
      };
    case "query_classic":
      return { description: "Classic query", resource_type: "assets", limit: 1000 };
    case "query_sql":
      return {
        description: "SQL query",
        sql_query: "",
        limit: 100,
        convert_to_string: true,
      };
  }
}

function defaultQueryLabel(kind: QueryNodeKind): string {
  switch (kind) {
    case "query_view":
      return "View query";
    case "query_raw":
      return "RAW query";
    case "query_classic":
      return "Classic query";
    case "query_sql":
      return "SQL query";
  }
}

export function readNodeConfig(node: WorkflowCanvasNode): JsonObject {
  const data = node.data ?? {};
  const cfg = data.config;
  if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
    return { ...(cfg as JsonObject) };
  }
  return {};
}

export function patchNodeConfig(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  config: JsonObject
): WorkflowCanvasDocument {
  return {
    ...canvas,
    nodes: canvas.nodes.map((n) => {
      if (n.id !== nodeId) return n;
      const data: WorkflowCanvasNodeData = { ...(n.data ?? {}), config };
      return { ...n, data };
    }),
  };
}

export function addQueryNode(
  canvas: WorkflowCanvasDocument,
  kind: QueryNodeKind,
  schema_space = ""
): { canvas: WorkflowCanvasDocument; nodeId: string } {
  const id = newNodeId();
  const config = defaultQueryConfig(kind, schema_space);
  const node: WorkflowCanvasNode = {
    id,
    kind,
    position: { x: 0, y: 0 },
    data: {
      label: defaultQueryLabel(kind),
      handler_family: "discovery",
      config,
    },
  };
  return { canvas: { ...canvas, nodes: [...canvas.nodes, node] }, nodeId: id };
}

export function removeQueryNode(canvas: WorkflowCanvasDocument, nodeId: string): WorkflowCanvasDocument {
  return {
    ...canvas,
    nodes: canvas.nodes.filter((n) => n.id !== nodeId),
    edges: canvas.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
  };
}

export function queryNodeListLabel(node: WorkflowCanvasNode): string {
  const cfg = readNodeConfig(node);
  if (node.kind === "query_sql") {
    const sq = String(cfg.sql_query ?? cfg.query ?? "").trim();
    if (sq) {
      const oneLine = sq.split(/\r?\n/)[0]?.trim() ?? sq;
      return oneLine.length > 72 ? `${oneLine.slice(0, 72)}…` : oneLine;
    }
  }
  const ext = String(cfg.view_external_id ?? "").trim();
  if (ext) return ext;
  const desc = String(cfg.description ?? "").trim();
  if (desc) return desc;
  const rawDb = String(cfg.raw_db ?? "").trim();
  if (rawDb) return rawDb;
  const label = String(node.data?.label ?? "").trim();
  if (label) return label;
  return node.id;
}

export function kindLabelKey(kind: CanvasNodeKind | undefined): string {
  switch (kind) {
    case "query_view":
      return "flow.discoveryViewQuery";
    case "query_raw":
      return "flow.discoveryRawQuery";
    case "query_classic":
      return "flow.discoveryClassicQuery";
    case "query_sql":
      return "flow.discoverySqlQuery";
    default:
      return "queries.unnamedQuery";
  }
}
