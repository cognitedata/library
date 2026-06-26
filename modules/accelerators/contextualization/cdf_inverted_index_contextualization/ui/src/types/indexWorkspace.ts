export type IndexTabKind =
  | "overview"
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
  scope_enabled?: boolean;
  scope_fallback?: string;
  subscription_enabled?: boolean;
  watch_property?: string;
  index_field_count?: number;
  instance_spaces?: string[] | null;
};

export type OverviewSubTab = "summary" | "configuration";
