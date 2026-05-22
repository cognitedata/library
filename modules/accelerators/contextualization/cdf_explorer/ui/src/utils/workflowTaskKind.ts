import type { WorkflowGraphTask } from "../types/explorerNodes";

const DISCOVERY_FUNCTION_KIND: Record<string, string> = {
  fn_dm_view_query: "View query",
  fn_dm_raw_query: "RAW query",
  fn_dm_classic_query: "Classic query",
  fn_dm_transform: "Transform",
  fn_dm_merge: "Merge",
  fn_dm_join: "Join",
  fn_dm_validate: "Validate",
  fn_dm_filter: "Instance filter",
  fn_dm_confidence_filter: "Confidence filter",
  fn_dm_inverted_index: "Inverted index",
  fn_dm_view_save: "View save",
  fn_dm_raw_save: "RAW save",
  fn_dm_classic_save: "Classic save",
  fn_dm_discovery_raw_cleanup: "RAW cleanup",
};

function functionExternalIdFromParameters(parameters: Record<string, unknown> | undefined): string {
  if (!parameters || typeof parameters !== "object") return "";
  const fn = parameters.function;
  if (!fn || typeof fn !== "object") return "";
  const ext = (fn as Record<string, unknown>).externalId ?? (fn as Record<string, unknown>).external_id;
  return ext != null ? String(ext).trim() : "";
}

function titleCaseTaskType(type: string): string {
  const t = type.trim();
  if (!t) return "Task";
  if (t === "function") return "Function";
  return t.charAt(0).toUpperCase() + t.slice(1);
}

/** Human-readable stage kind for workflow viewer nodes (e.g. Transform, View query). */
export function workflowTaskKindLabel(task: WorkflowGraphTask): string {
  const fn = functionExternalIdFromParameters(task.parameters);
  if (fn && DISCOVERY_FUNCTION_KIND[fn]) {
    return DISCOVERY_FUNCTION_KIND[fn];
  }
  if (task.type && task.type !== "function") {
    return titleCaseTaskType(task.type);
  }
  return titleCaseTaskType(task.type || "task");
}
