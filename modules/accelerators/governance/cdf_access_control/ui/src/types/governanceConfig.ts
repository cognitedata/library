export type JsonObject = Record<string, unknown>;

export type LocationNode = {
  id?: string;
  name?: string;
  description?: string;
  locations?: LocationNode[];
};

export type HierarchyDimension = {
  order?: number;
  type: "hierarchy";
  levels?: string[];
  locations?: LocationNode[];
  name?: string;
  description?: string;
};

export type ListDimensionItem = { id: string; name?: string; description?: string };

export type ListDimension = {
  order?: number;
  type: "list";
  items?: ListDimensionItem[];
  name?: string;
  description?: string;
};

export type DimensionBlock = HierarchyDimension | ListDimension | JsonObject;

export type SourceIdsMap = Record<string, string>;

export type GlobalBindings = {
  source_ids?: SourceIdsMap;
  sourceId?: string;
  template?: string;
  name_template?: string;
  instance_space_id_template?: string;
};

export type OrgWideGroup = {
  enabled?: boolean;
  name?: string;
  description?: string;
  scope_id?: string;
  scopeId?: string;
  extra_instance_spaces?: string[] | string;
  extraInstanceSpaces?: string[] | string;
};

export type SpacesConfig = {
  scope_dimension?: string;
  from_dimension?: string;
  combine_with?: string[];
  template?: string;
  output_dir?: string;
  nodes?: "leaves" | "all" | string;
  instance_space_id_template?: string;
  name_template?: string;
  expansion?: JsonObject;
  global?: GlobalBindings;
};

export type GroupsConfig = {
  scope_dimension?: string;
  combine_with?: string[];
  template?: string;
  output_dir?: string;
  nodes?: "leaves" | "all" | string;
  name_template?: string;
  expansion?: JsonObject;
  global?: GlobalBindings;
  org_wide?: OrgWideGroup;
  orgWide?: OrgWideGroup;
};

export type GovernanceDocument = {
  access_control_ui?: { mirror_config_paths?: string[] };
  dimensions?: Record<string, DimensionBlock>;
  spaces?: SpacesConfig;
  groups?: GroupsConfig;
  toolkit?: JsonObject;
};

export function emptyLocationNode(): LocationNode {
  return { id: "", name: "", description: "", locations: [] };
}

export function emptyGovernanceDocument(): GovernanceDocument {
  return {
    access_control_ui: { mirror_config_paths: [] },
    dimensions: {},
    spaces: {
      scope_dimension: "",
      template: "",
      output_dir: "spaces",
      nodes: "leaves",
      global: { source_ids: {}, sourceId: "" },
    },
    groups: {
      scope_dimension: "",
      template: "",
      output_dir: "auth",
      nodes: "leaves",
      global: { source_ids: {}, sourceId: "" },
    },
  };
}

export function hierarchyDimensionKeys(dimensions: Record<string, DimensionBlock> | undefined): string[] {
  if (!dimensions) return [];
  return Object.entries(dimensions)
    .filter(([, b]) => b && typeof b === "object" && (b as JsonObject).type === "hierarchy")
    .map(([k]) => k);
}

export function listDimensionKeys(dimensions: Record<string, DimensionBlock> | undefined): string[] {
  if (!dimensions) return [];
  return Object.entries(dimensions)
    .filter(([, b]) => b && typeof b === "object" && (b as JsonObject).type === "list")
    .map(([k]) => k);
}

export function sortedDimensionKeys(dimensions: Record<string, DimensionBlock>): string[] {
  return Object.entries(dimensions)
    .sort(([, a], [, b]) => {
      const ao = typeof a === "object" && a && "order" in a ? Number((a as JsonObject).order) : 0;
      const bo = typeof b === "object" && b && "order" in b ? Number((b as JsonObject).order) : 0;
      return ao - bo;
    })
    .map(([k]) => k);
}

export function reindexDimensionOrders(dimensions: Record<string, DimensionBlock>, keys: string[]): void {
  keys.forEach((k, i) => {
    const block = dimensions[k];
    if (block && typeof block === "object") {
      (block as JsonObject).order = i;
    }
  });
}
