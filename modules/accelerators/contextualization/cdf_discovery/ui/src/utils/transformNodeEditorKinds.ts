import type { TransformCanvasNodeKind } from "../types/transformCanvas";

/** Filter / join / merge stages in the Orchestration palette group. */
export const PIPELINE_ORCHESTRATION_NODE_KINDS: readonly TransformCanvasNodeKind[] = [
  "filter",
  "join",
  "merge",
] as const;

/** Native CDF Data Workflow task types (Fusion * palette labels). */
export const FUSION_NODE_KINDS: readonly TransformCanvasNodeKind[] = [
  "spark_transform",
  "transformation_ref",
  "function_ref",
  "json_mapping",
  "dynamic_fanout",
  "subworkflow",
  "simulation",
  "cdf_task",
] as const;

const FUSION_KIND_SET = new Set<string>(FUSION_NODE_KINDS);

export function isOrchestrationNodeKind(
  kind: TransformCanvasNodeKind | null | undefined
): kind is TransformCanvasNodeKind {
  return kind != null && FUSION_KIND_SET.has(kind);
}

export const MODAL_EDITOR_NODE_KINDS = new Set<TransformCanvasNodeKind>([
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "query_records",
  "filter",
  "json_mapping",
  "join",
  "merge",
  "build_index",
  "transform",
  "score",
  "save_view",
  "save_raw",
  "save_classic",
  "save_records",
  "save_stream",
  "spark_transform",
  "transformation_ref",
  "function_ref",
  "subworkflow",
  "simulation",
  "cdf_task",
  "file_annotation",
  "workflow_fanout_plan",
  "dynamic_fanout",
  "start",
  "end",
  "raw_cleanup",
  "subgraph",
]);

export function isNodePreviewKind(kind: TransformCanvasNodeKind | null | undefined): boolean {
  return kind === "node_preview";
}

export function shouldOpenPreviewQueryOnActivate(
  kind: TransformCanvasNodeKind | null | undefined
): boolean {
  return isNodePreviewKind(kind);
}

export function shouldOpenNodeEditorOnDoubleClick(
  kind: TransformCanvasNodeKind | null | undefined,
  readOnly: boolean
): boolean {
  if (!kind) return false;
  if (shouldOpenPreviewQueryOnActivate(kind)) return false;
  if (readOnly) return isOrchestrationNodeKind(kind);
  return MODAL_EDITOR_NODE_KINDS.has(kind);
}
