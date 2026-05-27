export type DmInstanceKind = "node" | "edge";

export type OpenTarget =
  | { type: "classic_list"; resource_type: string }
  | {
      type: "dm_instances";
      view_space: string;
      view_external_id: string;
      view_version: string;
      instance_kind: DmInstanceKind;
    }
  | { type: "raw_rows"; database: string; table: string }
  | { type: "record_stream"; stream_external_id: string }
  | { type: "fusion_cdf"; resource: string }
  | { type: "fusion_sequence"; sequence_external_id: string }
  | { type: "fusion_dm_all"; entity: "nodes" | "edges" }
  | {
      type: "fusion_data_model";
      model_space: string;
      model_external_id: string;
      model_version: string;
      type_external_id: string;
    };

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
  instance_kind: DmInstanceKind;
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
  instance_space?: string;
  name?: string;
  format: FileContentFormat;
};

export type RecordsStreamDocumentTab = {
  kind: "records_stream";
  id: string;
  label: string;
  streamExternalId: string;
  streamDetail: Record<string, unknown> | null;
  items: GridRow[];
  columns: string[];
  cursor: string | null;
  loading: boolean;
  error: string | null;
  pageSize: number;
  pageIndex: number;
  selectedRowIndex: number | null;
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
  destination?: Record<string, unknown> | null;
  definition?: Record<string, unknown>;
};

export type TransformationListItem = {
  id: number;
  external_id?: string | null;
  name?: string | null;
  label: string;
  created_time?: string | null;
  data_set_id?: number | null;
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

export type GovernanceSubTab = "configure" | "build" | "artifacts";

export type GovernanceScopeDocumentTab = {
  kind: "governance_scope";
  id: "gov:scope";
  label: string;
};

export type GovernanceSpacesDocumentTab = {
  kind: "governance_spaces";
  id: "gov:spaces";
  label: string;
  activeSubTab: GovernanceSubTab;
  artifactRel?: string | null;
};

export type GovernanceGroupsDocumentTab = {
  kind: "governance_groups";
  id: "gov:groups";
  label: string;
  activeSubTab: GovernanceSubTab;
  artifactRel?: string | null;
};

export type GovernanceCdfSpaceDocumentTab = {
  kind: "governance_cdf_space";
  id: string;
  label: string;
  space: string;
  detail: Record<string, unknown> | null;
  loading: boolean;
  error: string | null;
};

export type GovernanceCdfGroupDocumentTab = {
  kind: "governance_cdf_group";
  id: string;
  label: string;
  groupId: number;
  detail: Record<string, unknown> | null;
  loading: boolean;
  error: string | null;
};

export type EtlPipelineDocumentTab = {
  kind: "etl_pipeline";
  id: string;
  label: string;
  pipelineId: string;
  /** Build scope folder (``transform/workflows/{scopeSuffix}/``). */
  scopeSuffix: string;
  document: import("./transformCanvas").TransformPipelineDocument | null;
  canvas: import("./transformCanvas").TransformCanvasDocument | null;
  loading: boolean;
  error: string | null;
  dirty: boolean;
  /** Console log, results, and editor subtab for local runs (session-only). */
  runSession?: import("./transformTabRun").TransformTabRunSession | null;
};

export type EtlTemplateDocumentTab = {
  kind: "etl_template";
  id: string;
  label: string;
  templateId: string;
  document: Record<string, unknown> | null;
  canvas: import("./transformCanvas").TransformCanvasDocument | null;
  loading: boolean;
  error: string | null;
  dirty: boolean;
  runSession?: import("./transformTabRun").TransformTabRunSession | null;
};

export type EtlScopeDocumentTab = {
  kind: "etl_scope";
  id: "transform:scope";
  label: string;
};

export type EtlWorkflowYamlDocumentTab = {
  kind: "etl_workflow_yaml";
  id: string;
  label: string;
  /** Module-relative path under ``transform/workflows/``. */
  relPath: string;
  loading: boolean;
  error: string | null;
  dirty: boolean;
};

export type ExtractDocumentTab = {
  kind: "extract";
  id: "extract";
  label: string;
};

export type MonitorDocumentTab = {
  kind: "monitor";
  id: "monitor";
  label: string;
};

export type DocumentTab =
  | DataModelDocumentTab
  | RecordsStreamDocumentTab
  | SqlDocumentTab
  | TransformationDocumentTab
  | FunctionDocumentTab
  | WorkflowDocumentTab
  | GovernanceScopeDocumentTab
  | GovernanceSpacesDocumentTab
  | GovernanceGroupsDocumentTab
  | GovernanceCdfSpaceDocumentTab
  | GovernanceCdfGroupDocumentTab
  | EtlPipelineDocumentTab
  | EtlTemplateDocumentTab
  | EtlScopeDocumentTab
  | EtlWorkflowYamlDocumentTab
  | ExtractDocumentTab
  | MonitorDocumentTab;

/** Serializable document tab for ``discovery.local.config.yaml`` workspace persistence. */
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

export type SavedWorkspaceGovernanceScopeTab = {
  kind: "governance_scope";
  id: string;
  label?: string;
};

export type SavedWorkspaceGovernanceSpacesTab = {
  kind: "governance_spaces";
  id: string;
  label?: string;
  active_sub_tab?: GovernanceSubTab;
  artifact_rel?: string | null;
};

export type SavedWorkspaceGovernanceGroupsTab = {
  kind: "governance_groups";
  id: string;
  label?: string;
  active_sub_tab?: GovernanceSubTab;
  artifact_rel?: string | null;
};

export type SavedWorkspaceGovernanceCdfSpaceTab = {
  kind: "governance_cdf_space";
  id: string;
  label?: string;
  space: string;
};

export type SavedWorkspaceGovernanceCdfGroupTab = {
  kind: "governance_cdf_group";
  id: string;
  label?: string;
  group_id: number;
};

export type SavedWorkspaceEtlPipelineTab = {
  kind: "etl_pipeline";
  id: string;
  label?: string;
  pipeline_id: string;
  scope_suffix?: string;
};

export type SavedWorkspaceEtlTemplateTab = {
  kind: "etl_template";
  id: string;
  label?: string;
  template_id: string;
};

export type SavedWorkspaceEtlScopeTab = {
  kind: "etl_scope";
  id: string;
  label?: string;
};

export type SavedWorkspaceExtractTab = {
  kind: "extract";
  id: string;
  label?: string;
};

export type SavedWorkspaceMonitorTab = {
  kind: "monitor";
  id: string;
  label?: string;
};

export type SavedWorkspaceTab =
  | SavedWorkspaceSqlTab
  | SavedWorkspaceDataModelTab
  | SavedWorkspaceTransformationTab
  | SavedWorkspaceFunctionTab
  | SavedWorkspaceWorkflowTab
  | SavedWorkspaceGovernanceScopeTab
  | SavedWorkspaceGovernanceSpacesTab
  | SavedWorkspaceGovernanceGroupsTab
  | SavedWorkspaceGovernanceCdfSpaceTab
  | SavedWorkspaceGovernanceCdfGroupTab
  | SavedWorkspaceEtlPipelineTab
  | SavedWorkspaceEtlTemplateTab
  | SavedWorkspaceEtlScopeTab
  | SavedWorkspaceExtractTab
  | SavedWorkspaceMonitorTab;

export type SavedWorkspace = {
  active_tab_id: string | null;
  tabs: SavedWorkspaceTab[];
};

export function isDataModelTab(tab: DocumentTab): tab is DataModelDocumentTab {
  return tab.kind === "data_model";
}

export function isRecordsStreamTab(tab: DocumentTab): tab is RecordsStreamDocumentTab {
  return tab.kind === "records_stream";
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

export function isGovernanceScopeTab(tab: DocumentTab): tab is GovernanceScopeDocumentTab {
  return tab.kind === "governance_scope";
}

export function isGovernanceSpacesTab(tab: DocumentTab): tab is GovernanceSpacesDocumentTab {
  return tab.kind === "governance_spaces";
}

export function isGovernanceGroupsTab(tab: DocumentTab): tab is GovernanceGroupsDocumentTab {
  return tab.kind === "governance_groups";
}

export function isGovernanceCdfSpaceTab(tab: DocumentTab): tab is GovernanceCdfSpaceDocumentTab {
  return tab.kind === "governance_cdf_space";
}

export function isGovernanceCdfGroupTab(tab: DocumentTab): tab is GovernanceCdfGroupDocumentTab {
  return tab.kind === "governance_cdf_group";
}

export function isEtlPipelineTab(tab: DocumentTab): tab is EtlPipelineDocumentTab {
  return tab.kind === "etl_pipeline";
}

export function isEtlTemplateTab(tab: DocumentTab): tab is EtlTemplateDocumentTab {
  return tab.kind === "etl_template";
}

export function isEtlScopeTab(tab: DocumentTab): tab is EtlScopeDocumentTab {
  return tab.kind === "etl_scope";
}

export function isEtlWorkflowYamlTab(tab: DocumentTab): tab is EtlWorkflowYamlDocumentTab {
  return tab.kind === "etl_workflow_yaml";
}

export function isExtractTab(tab: DocumentTab): tab is ExtractDocumentTab {
  return tab.kind === "extract";
}

export function isMonitorTab(tab: DocumentTab): tab is MonitorDocumentTab {
  return tab.kind === "monitor";
}

export function openTargetKey(target: OpenTarget): string {
  if (target.type === "classic_list") return `classic:${target.resource_type}`;
  if (target.type === "dm_instances") {
    return `dm:${target.view_space}:${target.view_external_id}:${target.view_version}`;
  }
  if (target.type === "fusion_cdf") return `fusion:${target.resource}`;
  if (target.type === "fusion_sequence") {
    return `fusion:seq:${encodeURIComponent(target.sequence_external_id)}`;
  }
  if (target.type === "fusion_dm_all") return `fusion:dm:${target.entity}`;
  if (target.type === "fusion_data_model") {
    const { model_space, model_external_id, model_version, type_external_id } = target;
    return `fusion:type:${encodeURIComponent(model_space)}:${encodeURIComponent(
      model_external_id
    )}:${encodeURIComponent(model_version)}:${encodeURIComponent(type_external_id)}`;
  }
  if (target.type === "record_stream") {
    return `record_stream:${encodeURIComponent(target.stream_external_id)}`;
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
  if (target.type === "fusion_cdf") {
    const names: Record<string, string> = {
      datapoints: "Datapoints",
      stringdatapoints: "String Datapoints",
    };
    return names[target.resource] ?? target.resource;
  }
  if (target.type === "fusion_sequence") {
    return `Sequence rows: ${target.sequence_external_id}`;
  }
  if (target.type === "fusion_dm_all") {
    return target.entity === "nodes" ? "All nodes" : "All edges";
  }
  if (target.type === "fusion_data_model") {
    return `Type: ${target.type_external_id}`;
  }
  if (target.type === "record_stream") {
    return `Records: ${target.stream_external_id}`;
  }
  return `RAW ${target.database}.${target.table}`;
}
