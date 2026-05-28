import type { MessageKey } from "../i18n/types";
import type { WorkflowGraphTask } from "../types/discoveryNodes";

const FN_KIND_KEYS: Record<string, MessageKey> = {
  fn_dm_view_query: "wfViewer.fnKind.fn_dm_view_query",
  fn_dm_raw_query: "wfViewer.fnKind.fn_dm_raw_query",
  fn_dm_classic_query: "wfViewer.fnKind.fn_dm_classic_query",
  fn_dm_transform: "wfViewer.fnKind.fn_dm_transform",
  fn_dm_merge: "wfViewer.fnKind.fn_dm_merge",
  fn_dm_join: "wfViewer.fnKind.fn_dm_join",
  fn_dm_validate: "wfViewer.fnKind.fn_dm_validate",
  fn_dm_filter: "wfViewer.fnKind.fn_dm_filter",
  fn_dm_confidence_filter: "wfViewer.fnKind.fn_dm_confidence_filter",
  fn_dm_inverted_index: "wfViewer.fnKind.fn_dm_inverted_index",
  fn_etl_build_index: "wfViewer.fnKind.fn_etl_build_index",
  fn_dm_view_save: "wfViewer.fnKind.fn_dm_view_save",
  fn_dm_raw_save: "wfViewer.fnKind.fn_dm_raw_save",
  fn_dm_classic_save: "wfViewer.fnKind.fn_dm_classic_save",
  fn_dm_discovery_raw_cleanup: "wfViewer.fnKind.fn_dm_discovery_raw_cleanup",
};

type Translate = (key: MessageKey, vars?: Record<string, string | number>) => string;

/** CDF Data Workflow native task types (not discovery ETL functions). */
const FUSION_NATIVE_TASK_TYPE_KEYS: Record<string, MessageKey> = {
  jsonmapping: "transform.palette.json_mapping",
  transformation: "transform.palette.transformation_ref",
  function: "transform.palette.function_ref",
  dynamic: "transform.palette.dynamic_fanout",
  subworkflow: "transform.palette.subworkflow",
  simulation: "transform.palette.simulation",
  cdf: "transform.palette.cdf_task",
};

function functionExternalIdFromParameters(parameters: Record<string, unknown> | undefined): string {
  if (!parameters || typeof parameters !== "object") return "";
  const fn = parameters.function;
  if (!fn || typeof fn !== "object") return "";
  const ext = (fn as Record<string, unknown>).externalId ?? (fn as Record<string, unknown>).external_id;
  return ext != null ? String(ext).trim() : "";
}

/** Human-readable stage kind for workflow viewer nodes (e.g. Transform, View query). */
export function workflowTaskKindLabel(task: WorkflowGraphTask, t: Translate): string {
  const fn = functionExternalIdFromParameters(task.parameters);
  const fnKey = fn ? FN_KIND_KEYS[fn] : undefined;
  if (fnKey) return t(fnKey);

  const type = (task.type ?? "task").trim();
  const fusionKey = FUSION_NATIVE_TASK_TYPE_KEYS[type.toLowerCase().replace(/[^a-z]/g, "")];
  if (fusionKey) return t(fusionKey);
  if (type === "task") return t("wfViewer.taskType.task");
  if (!type) return t("wfViewer.taskType.task");
  return t("wfViewer.taskType.generic", {
    type: type.charAt(0).toUpperCase() + type.slice(1),
  });
}
