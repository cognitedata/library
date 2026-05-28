/**
 * Serialized flow graph under `canvas` in a v1 pipeline YAML document.
 */

import type { TransformCanvasViewport } from "./transformCanvasViewport";

export const TRANSFORM_CANVAS_SCHEMA_VERSION = 1;
export type { TransformCanvasViewport } from "./transformCanvasViewport";
export { normalizeTransformCanvasViewport } from "./transformCanvasViewport";

export type TransformCanvasHandleOrientation = "lr" | "tb";

export function normalizeTransformCanvasHandleOrientation(raw: unknown): TransformCanvasHandleOrientation {
  if (raw === "tb") return "tb";
  return "lr";
}

/** Auto-layout engine for the transform pipeline canvas. */
export type TransformCanvasLayoutMethod = "layered" | "dagre";

export function normalizeTransformCanvasLayoutMethod(raw: unknown): TransformCanvasLayoutMethod {
  if (raw === "dagre") return "dagre";
  return "layered";
}

/** React Flow edge path style persisted on the canvas document. */
export type TransformCanvasEdgePathStyle =
  | "smoothstep"
  | "straight"
  | "step"
  | "default"
  | "simplebezier";

export function normalizeTransformCanvasEdgePathStyle(raw: unknown): TransformCanvasEdgePathStyle {
  if (raw === "straight" || raw === "step" || raw === "default" || raw === "simplebezier") {
    return raw;
  }
  return "smoothstep";
}

export type TransformCanvasEdgeKind = "data" | "sequence" | "parallel_group";

/** Logical kind stored in canvas file (maps to React Flow custom type). */
export type TransformCanvasNodeKind =
  | "start"
  | "end"
  | "query_view"
  | "query_raw"
  | "query_classic"
  | "query_sql"
  | "query_records"
  | "score"
  | "transform"
  | "filter"
  | "json_mapping"
  | "join"
  | "merge"
  | "build_index"
  | "save_view"
  | "save_raw"
  | "save_classic"
  | "save_records"
  | "save_stream"
  | "raw_cleanup"
  | "spark_transform"
  | "transformation_ref"
  | "function_ref"
  | "dynamic_fanout"
  | "file_annotation"
  | "workflow_fanout_plan"
  | "subworkflow"
  | "simulation"
  | "cdf_task"
  | "subgraph"
  | "node_preview";

export type TransformCanvasNodeRfType =
  | "etlStart"
  | "etlEnd"
  | "etlQueryView"
  | "etlQueryRaw"
  | "etlQueryClassic"
  | "etlQuerySql"
  | "etlQueryRecords"
  | "etlScore"
  | "etlTransform"
  | "etlFilter"
  | "etlJsonMapping"
  | "etlJoin"
  | "etlMerge"
  | "etlBuildIndex"
  | "etlSaveView"
  | "etlSaveRaw"
  | "etlSaveClassic"
  | "etlSaveRecords"
  | "etlSaveStream"
  | "etlRawCleanup"
  | "etlSparkTransform"
  | "etlTransformationRef"
  | "etlFunctionRef"
  | "etlDynamicFanout"
  | "etlFileAnnotation"
  | "etlWorkflowFanoutPlan"
  | "etlSubworkflow"
  | "etlSimulation"
  | "etlCdfTask"
  | "etlSubgraph"
  | "etlNodePreview";

const KIND_TO_RF: Record<TransformCanvasNodeKind, TransformCanvasNodeRfType> = {
  start: "etlStart",
  end: "etlEnd",
  query_view: "etlQueryView",
  query_raw: "etlQueryRaw",
  query_classic: "etlQueryClassic",
  query_sql: "etlQuerySql",
  query_records: "etlQueryRecords",
  score: "etlScore",
  transform: "etlTransform",
  filter: "etlFilter",
  json_mapping: "etlJsonMapping",
  join: "etlJoin",
  merge: "etlMerge",
  build_index: "etlBuildIndex",
  save_view: "etlSaveView",
  save_raw: "etlSaveRaw",
  save_classic: "etlSaveClassic",
  save_records: "etlSaveRecords",
  save_stream: "etlSaveStream",
  raw_cleanup: "etlRawCleanup",
  spark_transform: "etlSparkTransform",
  transformation_ref: "etlTransformationRef",
  function_ref: "etlFunctionRef",
  dynamic_fanout: "etlDynamicFanout",
  file_annotation: "etlFileAnnotation",
  workflow_fanout_plan: "etlWorkflowFanoutPlan",
  subworkflow: "etlSubworkflow",
  simulation: "etlSimulation",
  cdf_task: "etlCdfTask",
  subgraph: "etlSubgraph",
  node_preview: "etlNodePreview",
};

const RF_TO_KIND: Record<TransformCanvasNodeRfType, TransformCanvasNodeKind> = Object.fromEntries(
  Object.entries(KIND_TO_RF).map(([k, v]) => [v, k])
) as Record<TransformCanvasNodeRfType, TransformCanvasNodeKind>;

export function kindToRfType(kind: TransformCanvasNodeKind): TransformCanvasNodeRfType {
  return KIND_TO_RF[kind] ?? "etlTransform";
}

export function rfTypeToKind(rfType: string | undefined): TransformCanvasNodeKind {
  if (rfType && rfType in RF_TO_KIND) return RF_TO_KIND[rfType as TransformCanvasNodeRfType];
  return "transform";
}

export type TransformCanvasNodeData = {
  label?: string;
  notes?: string;
  config?: Record<string, unknown>;
  node_color?: string;
  node_bg_color?: string;
};

export type TransformCanvasNode = {
  id: string;
  kind: TransformCanvasNodeKind;
  position: { x: number; y: number };
  data: TransformCanvasNodeData;
  enabled?: boolean;
  parent_id?: string | null;
  /** Flow node width in pixels (persisted when user resizes on canvas). */
  width?: number;
  /** Flow node height in pixels (persisted when user resizes on canvas). */
  height?: number;
};

export type TransformCanvasEdge = {
  id: string;
  source: string;
  target: string;
  source_handle?: string | null;
  target_handle?: string | null;
  kind?: TransformCanvasEdgeKind;
};

export type TransformCanvasDocument = {
  schemaVersion: typeof TRANSFORM_CANVAS_SCHEMA_VERSION;
  handle_orientation?: TransformCanvasHandleOrientation;
  /** Auto-layout algorithm (layered pipeline layout vs Dagre hierarchical). */
  layout_method?: TransformCanvasLayoutMethod;
  /** Visual edge routing for all connections on the canvas (React Flow edge type). */
  edge_path_style?: TransformCanvasEdgePathStyle;
  /** Last pan/zoom when the pipeline canvas was saved (optional). */
  viewport?: TransformCanvasViewport | null;
  nodes: TransformCanvasNode[];
  edges: TransformCanvasEdge[];
};

export type TransformPipelineParameters = {
  incremental?: boolean;
  incremental_change_processing?: boolean;
  incremental_skip_unchanged?: boolean;
  raw_db?: string;
  raw_table_key?: string;
  /** Stable RAW table for canvas preview node snapshots (excluded from cohort cleanup). */
  preview_raw_table_key?: string;
  instance_space?: string;
  etl_state_instance_space?: string | null;
  [key: string]: unknown;
};

export type TransformPipelineDocument = {
  schemaVersion: number;
  id: string;
  label: string;
  template_id?: string | null;
  parameters?: TransformPipelineParameters;
  scope?: Record<string, unknown> | null;
  sources?: unknown[];
  canvas: TransformCanvasDocument;
};

export function defaultStartNodeData(): TransformCanvasNodeData {
  return {
    label: "",
    config: {
      description: "",
      trigger_type: "schedule",
      cron_expression: "0 2 * * *",
      workflow_version: "1",
      workflow_base: "",
      workflow_external_id: "",
      trigger_external_id: "",
      incremental_change_processing: true,
      run_id: "",
    },
  };
}

export function defaultEndNodeData(): TransformCanvasNodeData {
  return {
    label: "End",
    config: {
      description: "",
    },
  };
}

export function emptyTransformCanvasDocument(): TransformCanvasDocument {
  return {
    schemaVersion: TRANSFORM_CANVAS_SCHEMA_VERSION,
    handle_orientation: "lr",
    layout_method: "layered",
    edge_path_style: "smoothstep",
    nodes: [
      { id: "start", kind: "start", position: { x: 80, y: 200 }, data: defaultStartNodeData() },
      { id: "end", kind: "end", position: { x: 480, y: 200 }, data: defaultEndNodeData() },
    ],
    edges: [{ id: "e_start_end", source: "start", target: "end", kind: "data" }],
  };
}

export function isTransformCanvasNodeEnabled(node: TransformCanvasNode): boolean {
  return node.enabled !== false;
}

export const TRANSFORM_PALETTE_STAGES: TransformCanvasNodeKind[] = [
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "query_records",
  "score",
  "filter",
  "join",
  "merge",
  "build_index",
  "save_view",
  "save_raw",
  "save_classic",
  "save_records",
  "save_stream",
  "file_annotation",
  "workflow_fanout_plan",
  "spark_transform",
  "transformation_ref",
  "function_ref",
  "json_mapping",
  "dynamic_fanout",
  "subworkflow",
  "simulation",
  "cdf_task",
];

export function paletteMessageKeyForKind(kind: TransformCanvasNodeKind): string {
  return `transform.palette.${kind}`;
}
