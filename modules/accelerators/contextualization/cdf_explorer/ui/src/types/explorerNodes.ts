export type OpenTarget =
  | { type: "classic_list"; resource_type: string }
  | {
      type: "dm_instances";
      view_space: string;
      view_external_id: string;
      view_version: string;
    }
  | { type: "raw_rows"; database: string; table: string };

export type TreeNode = {
  id: string;
  label: string;
  kind: string;
  has_children: boolean;
  starred?: boolean;
  open_target?: OpenTarget;
  meta?: Record<string, unknown>;
};

export type GridRow = Record<string, unknown>;

export type DataModelRef = {
  space: string;
  external_id: string;
  version: string;
  name?: string;
};

export type DataModelGraphView = {
  id: string;
  space: string;
  external_id: string;
  version: string;
  name: string;
  property_count: number;
};

export type DataModelGraphEdge = {
  id: string;
  from: { space: string; external_id: string; version: string };
  to: { space: string; external_id: string; version: string };
  label: string;
  kind: string;
};

export type DataModelGraph = {
  data_model: DataModelRef & { name?: string };
  views: DataModelGraphView[];
  edges: DataModelGraphEdge[];
};

export type DataModelDocumentTab = {
  kind: "data_model";
  id: string;
  label: string;
  dataModel: DataModelRef;
  graph: DataModelGraph | null;
  loading: boolean;
  error: string | null;
};

export type SqlQueryResult = {
  columns: string[];
  items: GridRow[];
  schema: { name?: string | null; type?: string | null }[];
  row_count: number;
};

export type SavedQuery = {
  id: string;
  name: string;
  query: string;
  limit: number;
  convert_to_string: boolean;
  source_limit?: number | null;
  timeout?: number | null;
};

export type FileContentFormat = "parquet" | "csv" | "json";

export type FileContentRef = {
  file_id?: number;
  external_id?: string;
  name?: string;
  format: FileContentFormat;
};

export type SqlDocumentTab = {
  kind: "sql";
  id: string;
  label: string;
  query: string;
  limit: number;
  convertToString: boolean;
  sourceLimit?: number | null;
  timeoutSec?: number | null;
  lastRunMs?: number | null;
  /** When set, **Save** updates this entry in ``saved_queries`` config. */
  savedQueryId?: string;
  /** ``cdf`` uses Transformations preview; ``file_content`` uses local DuckDB. */
  engine?: "cdf" | "file_content";
  fileContent?: FileContentRef;
  result: SqlQueryResult | null;
  loading: boolean;
  error: string | null;
  pageSize: number;
  pageIndex: number;
  selectedRowIndex: number | null;
};

export type TransformationDetail = {
  id: number;
  external_id?: string | null;
  name?: string | null;
  query: string;
  created_time?: string | null;
  last_updated_time?: string | null;
  data_set_id?: number | null;
  is_public?: boolean;
  has_destination?: boolean;
  conflict_mode?: string | null;
  definition?: Record<string, unknown>;
};

export type TransformationDocumentTab = {
  kind: "transformation";
  id: string;
  label: string;
  transformationId: number;
  detail: TransformationDetail | null;
  loading: boolean;
  error: string | null;
};

export type FunctionDetail = {
  id: string | number;
  external_id?: string | null;
  name?: string | null;
  description?: string | null;
  status?: string | null;
  file_id?: number | null;
  owner?: string | null;
  created_time?: string | null;
  definition?: Record<string, unknown>;
};

export type FunctionDocumentTab = {
  kind: "function";
  id: string;
  label: string;
  functionId: string;
  detail: FunctionDetail | null;
  loading: boolean;
  error: string | null;
};

export type WorkflowRef = {
  external_id: string;
  version?: string;
  name?: string;
};

export type WorkflowGraphTask = {
  id: string;
  external_id: string;
  name: string;
  type: string;
  label: string;
  description: string;
  retries?: number | null;
  timeout?: number | null;
  on_failure?: string | null;
  parameters?: Record<string, unknown>;
};

export type WorkflowGraphEdge = {
  id: string;
  from: string;
  to: string;
  label: string;
};

export type WorkflowGraph = {
  workflow: WorkflowRef & { description?: string; task_count?: number };
  tasks: WorkflowGraphTask[];
  edges: WorkflowGraphEdge[];
};

export type WorkflowDocumentTab = {
  kind: "workflow";
  id: string;
  label: string;
  workflow: WorkflowRef;
  graph: WorkflowGraph | null;
  loading: boolean;
  error: string | null;
};

export type DocumentTab =
  | DataModelDocumentTab
  | SqlDocumentTab
  | TransformationDocumentTab
  | FunctionDocumentTab
  | WorkflowDocumentTab;

/** Serializable document tab for ``explorer.local.config.yaml`` workspace persistence. */
export type SavedWorkspaceSqlTab = {
  kind: "sql";
  id: string;
  label?: string;
  query: string;
  limit?: number;
  source_limit?: number;
  timeout?: number;
  convert_to_string?: boolean;
  saved_query_id?: string;
  engine?: "cdf" | "file_content";
  file_content?: FileContentRef;
};

export type SavedWorkspaceDataModelTab = {
  kind: "data_model";
  id: string;
  label?: string;
  space: string;
  external_id: string;
  version: string;
  name?: string;
};

export type SavedWorkspaceTransformationTab = {
  kind: "transformation";
  id: string;
  label?: string;
  transformation_id: number;
};

export type SavedWorkspaceFunctionTab = {
  kind: "function";
  id: string;
  label?: string;
  function_id: string;
};

export type SavedWorkspaceWorkflowTab = {
  kind: "workflow";
  id: string;
  label?: string;
  external_id: string;
  version?: string;
  name?: string;
};

export type SavedWorkspaceTab =
  | SavedWorkspaceSqlTab
  | SavedWorkspaceDataModelTab
  | SavedWorkspaceTransformationTab
  | SavedWorkspaceFunctionTab
  | SavedWorkspaceWorkflowTab;

export type SavedWorkspace = {
  active_tab_id: string | null;
  tabs: SavedWorkspaceTab[];
};

export function isDataModelTab(tab: DocumentTab): tab is DataModelDocumentTab {
  return tab.kind === "data_model";
}

export function isSqlTab(tab: DocumentTab): tab is SqlDocumentTab {
  return tab.kind === "sql";
}

export function isTransformationTab(tab: DocumentTab): tab is TransformationDocumentTab {
  return tab.kind === "transformation";
}

export function isFunctionTab(tab: DocumentTab): tab is FunctionDocumentTab {
  return tab.kind === "function";
}

export function isWorkflowTab(tab: DocumentTab): tab is WorkflowDocumentTab {
  return tab.kind === "workflow";
}

export function openTargetKey(target: OpenTarget): string {
  if (target.type === "classic_list") return `classic:${target.resource_type}`;
  if (target.type === "dm_instances") {
    return `dm:${target.view_space}:${target.view_external_id}:${target.view_version}`;
  }
  return `raw:${target.database}:${target.table}`;
}

export function tabLabelForTarget(target: OpenTarget): string {
  if (target.type === "classic_list") {
    const names: Record<string, string> = {
      assets: "Assets",
      timeseries: "Time Series",
      events: "Events",
      files: "Files",
      sequences: "Sequences",
      data_sets: "Data Sets",
      relationships: "Relationships",
      labels: "Labels",
    };
    return names[target.resource_type] ?? target.resource_type;
  }
  if (target.type === "dm_instances") {
    return `Instances: ${target.view_external_id} (${target.view_version})`;
  }
  return `RAW ${target.database}.${target.table}`;
}
