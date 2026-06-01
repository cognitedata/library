import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import { kindToRfType } from "../../types/transformCanvas";
import { PIPELINE_ORCHESTRATION_NODE_KINDS } from "../../utils/transformNodeEditorKinds";

/** Canvas stages that materialize or pass cohort rows for downstream transform / score / build_index. */
export const COHORT_SOURCE_KINDS: readonly TransformCanvasNodeKind[] = [
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "query_records",
  "transform",
  "score",
  "build_index",
  ...PIPELINE_ORCHESTRATION_NODE_KINDS,
  "file_annotation",
  "workflow_fanout_plan",
  "json_mapping",
] as const;

export const COHORT_SOURCE_RF_TYPES = new Set(
  COHORT_SOURCE_KINDS.map((kind) => kindToRfType(kind))
);

/** Stages that require an upstream cohort-producing node. */
export const COHORT_CONSUMER_RF_TYPES = new Set<string>([
  kindToRfType("transform"),
  kindToRfType("score"),
  kindToRfType("build_index"),
]);

export function isCohortSourceRfType(rfType: string | undefined): boolean {
  return rfType != null && COHORT_SOURCE_RF_TYPES.has(rfType);
}

export function isCohortConsumerRfType(rfType: string | undefined): boolean {
  return rfType != null && COHORT_CONSUMER_RF_TYPES.has(rfType);
}
