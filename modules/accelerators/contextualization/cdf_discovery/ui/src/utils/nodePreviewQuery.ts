import type { OpenTarget } from "../types/discoveryNodes";
import type { TransformPipelineParameters } from "../types/transformCanvas";
import { sqlQueryForNodePreview } from "./sqlQuerySeed";

export const DEFAULT_PREVIEW_RAW_DB = "etl_staging";
export const DEFAULT_PREVIEW_RAW_TABLE = "etl_preview";

export function resolvePreviewRawSink(
  parameters?: TransformPipelineParameters | null,
  nodeConfig?: Record<string, unknown> | null
): { rawDb: string; previewTable: string } {
  const rawDb = String(parameters?.raw_db ?? DEFAULT_PREVIEW_RAW_DB).trim() || DEFAULT_PREVIEW_RAW_DB;
  const nodeTableOverride = String(nodeConfig?.preview_raw_table_key ?? "").trim();
  const previewTable =
    nodeTableOverride ||
    String(parameters?.preview_raw_table_key ?? DEFAULT_PREVIEW_RAW_TABLE).trim() ||
    DEFAULT_PREVIEW_RAW_TABLE;
  return { rawDb, previewTable };
}

export function nodePreviewOpenTarget(opts: {
  pipelineId: string;
  previewNodeId: string;
  rawDb: string;
  previewTable: string;
  runId: string;
}): OpenTarget {
  return {
    type: "node_preview",
    pipeline_id: opts.pipelineId,
    preview_node_id: opts.previewNodeId,
    raw_db: opts.rawDb,
    preview_table: opts.previewTable,
    run_id: opts.runId,
  };
}

export function sqlQueryForPreviewNode(opts: {
  rawDb: string;
  previewTable: string;
  runId: string;
  previewNodeId: string;
  noRunComment: string;
}): string {
  if (!opts.runId.trim()) {
    return opts.noRunComment;
  }
  return sqlQueryForNodePreview({
    rawDb: opts.rawDb,
    previewTable: opts.previewTable,
    runId: opts.runId,
    previewNodeId: opts.previewNodeId,
  });
}

export function nodePreviewSqlTabId(pipelineId: string, previewNodeId: string): string {
  return `sql:preview:${encodeURIComponent(pipelineId)}:${encodeURIComponent(previewNodeId)}`;
}
