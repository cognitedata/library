import type { MessageKey } from "../i18n/types";
import type { TransformCanvasNodeKind } from "../types/transformCanvas";
import { buildIndexHandlerDescription, transformHandlerDescription } from "./transformHandlerCatalog";
import type { PaletteDragPayload } from "../components/transform/transformFlowDrag";

type TFn = (key: MessageKey) => string;

/** Verbose palette hover text for canvas stages (not per-handler transform/build_index picks). */
const PALETTE_STAGE_DOC_KEY: Partial<Record<TransformCanvasNodeKind, MessageKey>> = {
  query_view: "transform.paletteDoc.query_view",
  query_raw: "transform.paletteDoc.query_raw",
  query_classic: "transform.paletteDoc.query_classic",
  query_sql: "transform.paletteDoc.query_sql",
  query_records: "transform.paletteDoc.query_records",
  filter: "transform.paletteDoc.filter",
  join: "transform.paletteDoc.join",
  merge: "transform.paletteDoc.merge",
  score: "transform.paletteDoc.score",
  build_index: "transform.paletteDoc.build_index",
  file_annotation: "transform.paletteDoc.file_annotation",
  workflow_fanout_plan: "transform.paletteDoc.workflow_fanout_plan",
  save_view: "transform.paletteDoc.save_view",
  save_raw: "transform.paletteDoc.save_raw",
  save_classic: "transform.paletteDoc.save_classic",
  save_records: "transform.paletteDoc.save_records",
  save_stream: "transform.paletteDoc.save_stream",
  spark_transform: "transform.paletteDoc.spark_transform",
  transformation_ref: "transform.paletteDoc.transformation_ref",
  function_ref: "transform.paletteDoc.function_ref",
  json_mapping: "transform.paletteDoc.json_mapping",
  dynamic_fanout: "transform.paletteDoc.dynamic_fanout",
  subworkflow: "transform.paletteDoc.subworkflow",
  simulation: "transform.paletteDoc.simulation",
  cdf_task: "transform.paletteDoc.cdf_task",
  node_preview: "transform.paletteDoc.node_preview",
};

export function paletteStageDocKey(stage: TransformCanvasNodeKind): MessageKey {
  return (
    PALETTE_STAGE_DOC_KEY[stage] ?? (`transform.paletteDoc.${stage}` as MessageKey)
  );
}

export function paletteStageTooltip(stage: TransformCanvasNodeKind, t: TFn): string {
  const key = paletteStageDocKey(stage);
  const text = t(key).trim();
  if (text && text !== key) return text;
  return t(`transform.palette.${stage}` as MessageKey);
}

/** Tooltip for palette drag payloads (handlers use exported Python descriptions). */
export function palettePayloadTooltip(payload: PaletteDragPayload, t: TFn): string {
  if (payload.kind !== "etl_stage") return "";
  const handlerId = String(payload.handlerId ?? "").trim();
  if (handlerId) {
    if (payload.stage === "transform") return transformHandlerDescription(handlerId);
    if (payload.stage === "build_index") return buildIndexHandlerDescription(handlerId);
  }
  return paletteStageTooltip(payload.stage, t);
}
