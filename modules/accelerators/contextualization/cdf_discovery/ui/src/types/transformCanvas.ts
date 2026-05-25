/**
 * Serialized flow graph under `canvas` in a v1 pipeline YAML document.
 */

export const TRANSFORM_CANVAS_SCHEMA_VERSION = 1;

export type TransformCanvasHandleOrientation = "lr" | "tb";

export function normalizeTransformCanvasHandleOrientation(raw: unknown): TransformCanvasHandleOrientation {
  if (raw === "tb") return "tb";
  return "lr";
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
  | "score"
  | "transform"
  | "filter"
  | "field_map"
  | "join"
  | "merge"
  | "build_index"
  | "save_view"
  | "save_raw"
  | "save_classic"
  | "raw_cleanup"
  | "spark_transform"
  | "transformation_ref"
  | "function_ref"
  | "dynamic_fanout"
  | "subworkflow"
  | "simulation"
  | "cdf_task"
  | "subgraph";

export type TransformCanvasNodeRfType =
  | "etlStart"
  | "etlEnd"
  | "etlQueryView"
  | "etlQueryRaw"
  | "etlQueryClassic"
  | "etlQuerySql"
  | "etlScore"
  | "etlTransform"
  | "etlFilter"
  | "etlFieldMap"
  | "etlJoin"
  | "etlMerge"
  | "etlBuildIndex"
  | "etlSaveView"
  | "etlSaveRaw"
  | "etlSaveClassic"
  | "etlRawCleanup"
  | "etlSparkTransform"
  | "etlTransformationRef"
  | "etlFunctionRef"
  | "etlDynamicFanout"
  | "etlSubworkflow"
  | "etlSimulation"
  | "etlCdfTask"
  | "etlSubgraph";

const KIND_TO_RF: Record<TransformCanvasNodeKind, TransformCanvasNodeRfType> = {
  start: "etlStart",
  end: "etlEnd",
  query_view: "etlQueryView",
  query_raw: "etlQueryRaw",
  query_classic: "etlQueryClassic",
  query_sql: "etlQuerySql",
  score: "etlScore",
  transform: "etlTransform",
  filter: "etlFilter",
  field_map: "etlFieldMap",
  join: "etlJoin",
  merge: "etlMerge",
  build_index: "etlBuildIndex",
  save_view: "etlSaveView",
  save_raw: "etlSaveRaw",
  save_classic: "etlSaveClassic",
  raw_cleanup: "etlRawCleanup",
  spark_transform: "etlSparkTransform",
  transformation_ref: "etlTransformationRef",
  function_ref: "etlFunctionRef",
  dynamic_fanout: "etlDynamicFanout",
  subworkflow: "etlSubworkflow",
  simulation: "etlSimulation",
  cdf_task: "etlCdfTask",
  subgraph: "etlSubgraph",
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
  /** Visual edge routing for all connections on the canvas (React Flow edge type). */
  edge_path_style?: TransformCanvasEdgePathStyle;
  nodes: TransformCanvasNode[];
  edges: TransformCanvasEdge[];
};

export type TransformPipelineParameters = {
  incremental?: boolean;
  incremental_change_processing?: boolean;
  incremental_skip_unchanged?: boolean;
  raw_db?: string;
  raw_table_key?: string;
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
    label: "Workflow trigger",
    config: {
      description: "Workflow trigger",
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
      description: "Post-run cohort RAW cleanup",
    },
  };
}

export function emptyTransformCanvasDocument(): TransformCanvasDocument {
  return {
    schemaVersion: TRANSFORM_CANVAS_SCHEMA_VERSION,
    handle_orientation: "lr",
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
  "transform",
  "score",
  "filter",
  "field_map",
  "join",
  "merge",
  "build_index",
  "save_view",
  "save_raw",
  "save_classic",
  "spark_transform",
  "function_ref",
  "transformation_ref",
  "subworkflow",
  "dynamic_fanout",
  "simulation",
  "cdf_task",
];

export function paletteMessageKeyForKind(kind: TransformCanvasNodeKind): string {
  return `transform.palette.${kind}`;
}
