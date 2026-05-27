import type { MessageKey } from "../../i18n";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import type { PaletteDragPayload } from "./transformFlowDrag";
import { etlPersistenceOutboundToEndOnlyRfTypes } from "./transformFlowConstants";

const QUERY_STAGES: readonly TransformCanvasNodeKind[] = [
  "query_view",
  "query_raw",
  "query_classic",
  "query_sql",
  "query_records",
];

const MID_PIPELINE_STAGES: readonly TransformCanvasNodeKind[] = [
  "transform",
  "json_mapping",
  "score",
  "filter",
  "join",
  "merge",
  "build_index",
];

const LOAD_STAGES: readonly TransformCanvasNodeKind[] = [
  "save_view",
  "save_raw",
  "save_classic",
  "save_records",
  "save_stream",
];

const CONTEXTUALIZATION_STAGES: readonly TransformCanvasNodeKind[] = [
  "file_annotation",
  "workflow_fanout_plan",
  "dynamic_fanout",
];

const ORCHESTRATION_STAGES: readonly TransformCanvasNodeKind[] = [
  "spark_transform",
  "transformation_ref",
  "function_ref",
  "subworkflow",
  "simulation",
  "cdf_task",
];

export type ConnectEndMenuOption = {
  id: string;
  payload: PaletteDragPayload;
  labelKey: MessageKey;
};

export type ConnectEndMenuGroup = {
  id: "query" | "transform" | "contextualization" | "load" | "orchestration";
  labelKey: MessageKey;
  options: ConnectEndMenuOption[];
};

const STAGE_LABEL: Partial<Record<TransformCanvasNodeKind, MessageKey>> = {
  start: "transform.palette.query_view",
  end: "transform.palette.query_view",
  query_view: "transform.palette.query_view",
  query_raw: "transform.palette.query_raw",
  query_classic: "transform.palette.query_classic",
  query_sql: "transform.palette.query_sql",
  query_records: "transform.palette.query_records",
  score: "transform.palette.score",
  transform: "transform.palette.transform",
  json_mapping: "transform.palette.json_mapping",
  filter: "transform.palette.filter",
  join: "transform.palette.join",
  merge: "transform.palette.merge",
  build_index: "transform.palette.build_index",
  save_view: "transform.palette.save_view",
  save_raw: "transform.palette.save_raw",
  save_classic: "transform.palette.save_classic",
  save_records: "transform.palette.save_records",
  save_stream: "transform.palette.save_stream",
  raw_cleanup: "transform.palette.transform",
  spark_transform: "transform.palette.spark_transform",
  transformation_ref: "transform.palette.transformation_ref",
  function_ref: "transform.palette.function_ref",
  subworkflow: "transform.palette.subworkflow",
  dynamic_fanout: "transform.palette.dynamic_fanout",
  file_annotation: "transform.palette.file_annotation",
  workflow_fanout_plan: "transform.palette.workflow_fanout_plan",
  simulation: "transform.palette.simulation",
  cdf_task: "transform.palette.cdf_task",
  subgraph: "transform.palette.transform",
};

function stageOptions(prefix: string, stages: readonly TransformCanvasNodeKind[]): ConnectEndMenuOption[] {
  return stages.map((stage) => ({
    id: `${prefix}-${stage}`,
    payload: { kind: "etl_stage", stage },
    labelKey: STAGE_LABEL[stage] ?? (`transform.palette.${stage}` as MessageKey),
  }));
}

export function connectEndMenuOptionsForSourceType(sourceType: string | undefined): ConnectEndMenuOption[] {
  if (!sourceType) return [];

  /** Save nodes wire only to end; palette has no End drop — offer no connect-end targets. */
  if (etlPersistenceOutboundToEndOnlyRfTypes.has(sourceType)) {
    return [];
  }

  if (sourceType === "etlStart") {
    return stageOptions("from-start", QUERY_STAGES);
  }

  const queryTypes = new Set([
    "etlQueryView",
    "etlQueryRaw",
    "etlQueryClassic",
    "etlQuerySql",
    "etlQueryRecords",
  ]);
  if (queryTypes.has(sourceType)) {
    return [
      ...stageOptions(`from-${sourceType}`, MID_PIPELINE_STAGES),
      ...stageOptions(`from-${sourceType}-ctx`, CONTEXTUALIZATION_STAGES),
      ...stageOptions(`from-${sourceType}-load`, LOAD_STAGES),
      ...stageOptions(`from-${sourceType}-orch`, ORCHESTRATION_STAGES),
    ];
  }

  const midTypes = new Set([
    "etlTransform",
    "etlScore",
    "etlFilter",
    "etlJoin",
    "etlMerge",
    "etlBuildIndex",
    ...LOAD_STAGES.map((s) => {
      const map: Partial<Record<TransformCanvasNodeKind, string>> = {
        save_view: "etlSaveView",
        save_raw: "etlSaveRaw",
        save_classic: "etlSaveClassic",
        save_records: "etlSaveRecords",
        save_stream: "etlSaveStream",
        raw_cleanup: "etlRawCleanup",
      };
      return map[s] ?? "";
    }),
  ]);
  if (midTypes.has(sourceType)) {
    return [
      ...stageOptions(`from-${sourceType}`, MID_PIPELINE_STAGES.filter((s) => s !== "filter" || sourceType !== "etlFilter")),
      ...stageOptions(`from-${sourceType}-ctx`, CONTEXTUALIZATION_STAGES),
      ...stageOptions(`from-${sourceType}-load`, LOAD_STAGES),
      ...stageOptions(`from-${sourceType}-orch`, ORCHESTRATION_STAGES),
    ];
  }

  return [];
}

export function connectEndMenuGroupedOptionsForSourceType(
  sourceType: string | undefined
): ConnectEndMenuGroup[] {
  const flat = connectEndMenuOptionsForSourceType(sourceType);
  const groups: ConnectEndMenuGroup[] = [];
  const query = flat.filter((o) => QUERY_STAGES.includes(o.payload.stage));
  const transform = flat.filter((o) => MID_PIPELINE_STAGES.includes(o.payload.stage));
  const load = flat.filter((o) => LOAD_STAGES.includes(o.payload.stage));
  const ctx = flat.filter((o) => CONTEXTUALIZATION_STAGES.includes(o.payload.stage));
  const orch = flat.filter((o) => ORCHESTRATION_STAGES.includes(o.payload.stage));
  if (query.length) groups.push({ id: "query", labelKey: "transform.connectEnd.group.query", options: query });
  if (transform.length) groups.push({ id: "transform", labelKey: "transform.connectEnd.group.transform", options: transform });
  if (ctx.length) groups.push({ id: "contextualization", labelKey: "transform.connectEnd.group.contextualization", options: ctx });
  if (load.length) groups.push({ id: "load", labelKey: "transform.connectEnd.group.load", options: load });
  if (orch.length) groups.push({ id: "orchestration", labelKey: "transform.connectEnd.group.orchestration", options: orch });
  return groups;
}

/** All palette stage groups for empty-canvas right-click (add node without connecting). */
export function connectEndMenuGroupedOptionsForPane(): ConnectEndMenuGroup[] {
  return [
    { id: "query", labelKey: "transform.connectEnd.group.query", options: stageOptions("pane", QUERY_STAGES) },
    { id: "transform", labelKey: "transform.connectEnd.group.transform", options: stageOptions("pane", MID_PIPELINE_STAGES) },
    { id: "contextualization", labelKey: "transform.connectEnd.group.contextualization", options: stageOptions("pane", CONTEXTUALIZATION_STAGES) },
    { id: "load", labelKey: "transform.connectEnd.group.load", options: stageOptions("pane", LOAD_STAGES) },
    { id: "orchestration", labelKey: "transform.connectEnd.group.orchestration", options: stageOptions("pane", ORCHESTRATION_STAGES) },
  ];
}
