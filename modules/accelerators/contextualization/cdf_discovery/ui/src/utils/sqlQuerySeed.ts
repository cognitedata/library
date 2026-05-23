import type { DataModelGraphView, OpenTarget, TreeNode } from "../types/discoveryNodes";

/** Classic resource_type → ``_cdf.*`` transformation table (Cognite SQL docs). */
const CLASSIC_CDF_TABLE: Record<string, string> = {
  assets: "_cdf.assets",
  timeseries: "_cdf.timeseries",
  files: "_cdf.files",
  events: "_cdf.events",
  sequences: "_cdf.sequences",
  data_sets: "_cdf.datasets",
  relationships: "_cdf.relationships",
  labels: "_cdf.labels",
};

/** Fusion ``_cdf.*`` tables (not Classic metadata). */
const FUSION_CDF_TABLE: Record<string, string> = {
  datapoints: "_cdf.datapoints",
  stringdatapoints: "_cdf.stringdatapoints",
};

function sqlStringLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function quoteIdent(segment: string): string {
  if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(segment)) {
    return segment;
  }
  return `\`${segment.replace(/`/g, "``")}\``;
}

/** Base SQL without ``LIMIT`` — limits are applied at run time from the query tab fields. */
export function sqlQueryForOpenTarget(target: OpenTarget): string | null {
  if (target.type === "classic_list") {
    const table = CLASSIC_CDF_TABLE[target.resource_type];
    if (!table) return null;
    return `SELECT * FROM ${table}`;
  }
  if (target.type === "raw_rows") {
    const db = quoteIdent(target.database);
    const table = quoteIdent(target.table);
    return `SELECT * FROM ${db}.${table}`;
  }
  if (target.type === "dm_instances") {
    const { view_space, view_external_id, view_version } = target;
    return `SELECT * FROM cdf_nodes(${sqlStringLiteral(view_space)}, ${sqlStringLiteral(
      view_external_id
    )}, ${sqlStringLiteral(view_version)})`;
  }
  if (target.type === "fusion_cdf") {
    const table = FUSION_CDF_TABLE[target.resource];
    if (!table) return null;
    return `SELECT * FROM ${table}`;
  }
  if (target.type === "fusion_sequence") {
    const seq = quoteIdent(target.sequence_external_id);
    return `SELECT * FROM _cdf_sequences.${seq}`;
  }
  if (target.type === "fusion_dm_all") {
    if (target.entity === "nodes") return "SELECT * FROM cdf_nodes()";
    if (target.entity === "edges") return "SELECT * FROM cdf_edges()";
    return null;
  }
  if (target.type === "fusion_data_model") {
    const { model_space, model_external_id, model_version, type_external_id } = target;
    return `SELECT * FROM cdf_data_models(${sqlStringLiteral(model_space)}, ${sqlStringLiteral(
      model_external_id
    )}, ${sqlStringLiteral(model_version)}, ${sqlStringLiteral(type_external_id)})`;
  }
  return null;
}

export function openTargetForDmView(view: DataModelGraphView): OpenTarget {
  return {
    type: "dm_instances",
    view_space: view.space,
    view_external_id: view.external_id,
    view_version: view.version,
  };
}

export function labelForDmView(view: DataModelGraphView): string {
  const title = view.name?.trim() || view.external_id;
  return `${title} (${view.version})`;
}

/** Tree kinds that support **Query** in the object tree context menu. */
const QUERYABLE_KINDS = new Set([
  "classic_resource",
  "raw_table",
  "dm_view",
  "fusion_resource",
  "fusion_sequence",
  "fusion_dm_all",
  "fusion_dm_view",
  "fusion_dm_container",
]);

export function canQueryTreeNode(node: TreeNode): boolean {
  if (!QUERYABLE_KINDS.has(node.kind) || !node.open_target) {
    return false;
  }
  return sqlQueryForOpenTarget(node.open_target) != null;
}

export function sqlQueryForTreeNode(node: TreeNode): string | null {
  if (!canQueryTreeNode(node) || !node.open_target) {
    return null;
  }
  return sqlQueryForOpenTarget(node.open_target);
}
