export type IndexTabKind =
  | "dashboard"
  | "configuration"
  | "build-metadata"
  | "build-annotations"
  | "query"
  | "file-context"
  | "target-driven"
  | "tag-reuse"
  | "settings";

export type IndexDocumentTab = {
  id: string;
  kind: IndexTabKind;
  label: string;
  navNodeId: string;
};

export type IndexNavNode = {
  id: string;
  labelKey: string;
  kind?: IndexTabKind;
  children?: IndexNavNode[];
};

export type WorkspaceState = {
  active_tab_id: string | null;
  tabs: IndexDocumentTab[];
};

export type ConnectionInfo = {
  project: string;
  base_url: string;
  cluster?: string;
  auth_mode?: string;
};

export type RuntimeConfigSummary = {
  storage_backend?: string;
  raw_database?: string;
  term_partition_enabled?: boolean;
  term_partition_threshold?: number;
  scope_enabled?: boolean;
  scope_fallback?: string;
  subscription_enabled?: boolean;
  watch_property?: string;
  index_field_count?: number;
  instance_spaces?: string[] | null;
  direct_relation_config?: Record<string, unknown>;
};

export type DashboardScopeRow = {
  match_scope_key: string;
  partition_strategy?: string;
  partition_table?: string;
  row_count: number;
  row_count_estimate?: number | null;
  bucket_tables_with_data?: number;
  reshard_recommended?: boolean;
  reshard_in_progress?: boolean;
  last_build_at?: string;
  row_status: "ok" | "warn" | "critical";
};

export type DashboardSummary = {
  scope_count: number;
  total_row_count: number;
  reshard_recommended_count: number;
  scopes_over_warn_threshold: number;
  scopes_over_critical_threshold: number;
  term_partition_enabled?: boolean;
  activate_above_rows?: number;
  reshard_recommended: string[];
  scopes: DashboardScopeRow[];
};

export type DashboardFileDeltaRow = {
  file_external_id: string;
  missing_tags_count: number;
  pattern_feedback_count: number;
  missing_tags: unknown[];
  pattern_feedback: unknown[];
};

export type DashboardBatchDeltasResult = {
  files_scanned: number;
  total_missing_tags: number;
  total_pattern_feedback: number;
  by_file: DashboardFileDeltaRow[];
};
