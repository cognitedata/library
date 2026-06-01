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

/** CDF naming element (see naming_conventions docs). */
export type NamingElement =
  | "data_type"
  | "source"
  | "pipeline_type"
  | "operation_type"
  | "access_type"
  | string;

export type ListDimension = {
  order?: number;
  type: "list";
  items?: ListDimensionItem[];
  name?: string;
  description?: string;
  naming_element?: NamingElement;
};

export type DimensionBlock = HierarchyDimension | ListDimension | JsonObject;

export type SourceIdsMap = Record<string, string>;

export type GlobalBindings = {
  source_ids?: SourceIdsMap;
  sourceId?: string;
  source_id?: string;
  template?: string;
  name_template?: string;
  instance_space_id_template?: string;
};

export type SpacesConfig = {
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
  combine_with?: string[];
  template?: string;
  output_dir?: string;
  nodes?: "leaves" | "all" | string;
  name_template?: string;
  display_name_template?: string;
  expansion?: JsonObject;
  global?: GlobalBindings;
};

export type GovernanceDocument = {
  environment?: JsonObject;
  variables?: Record<string, unknown>;
  governance_ui?: { mirror_config_paths?: string[] };
  scope_hierarchy?: HierarchyDimension;
  dimensions?: Record<string, DimensionBlock>;
  spaces?: SpacesConfig;
  groups?: GroupsConfig;
  toolkit?: JsonObject;
};

export const DEFAULT_INSTANCE_SPACE_ID_TEMPLATE =
  "inst_{{ data_type_id }}_{{ source_system_id }}_{{ scope_id_snake }}";

export const DEFAULT_SPACE_NAME_TEMPLATE =
  "{{ data_type }}:{{ site_id | default(scope_id) }}:{{ source_system_id }}";

export const DEFAULT_GROUP_NAME_TEMPLATE =
  "gp_{{ data_type_id }}_{{ location_id }}_{{ access_type_id }}";

export const DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE =
  "{{ data_type }}:{{ site_id | default(scope_id) }}:{{ access_type_id }}";

export const NAMING_DIMENSION_PRESETS: Record<
  string,
  { naming_element: NamingElement; type: "list"; name: string; items: ListDimensionItem[] }
> = {
  data_type: {
    naming_element: "data_type",
    type: "list",
    name: "Data type",
    items: [
      { id: "asset", name: "Asset" },
      { id: "timeseries", name: "Time series" },
      { id: "workorder", name: "Work order" },
      { id: "files", name: "Files" },
      { id: "3d", name: "3D" },
      { id: "dm", name: "Data model" },
    ],
  },
  source: {
    naming_element: "source",
    type: "list",
    name: "Source system",
    items: [
      { id: "sap", name: "SAP" },
      { id: "workmate", name: "Workmate" },
      { id: "aveva", name: "Aveva" },
      { id: "pi", name: "PI" },
      { id: "fileshare", name: "Fileshare" },
      { id: "sharepoint", name: "SharePoint" },
      { id: "erp", name: "ERP" },
      { id: "scada", name: "SCADA" },
    ],
  },
  pipeline_type: {
    naming_element: "pipeline_type",
    type: "list",
    name: "Pipeline type",
    items: [
      { id: "src", name: "Source data (src)" },
      { id: "ctx", name: "Contextualization (ctx)" },
      { id: "uc", name: "Use case (uc)" },
    ],
  },
  operation_type: {
    naming_element: "operation_type",
    type: "list",
    name: "Operation type",
    items: [
      { id: "extract", name: "Extract" },
      { id: "transform", name: "Transform" },
      { id: "load", name: "Load" },
      { id: "annotation", name: "Annotation" },
      { id: "asset_hierarchy", name: "Asset hierarchy" },
      { id: "metadata", name: "Metadata" },
      { id: "sync", name: "Sync" },
    ],
  },
  access_type: {
    naming_element: "access_type",
    type: "list",
    name: "Access type",
    items: [
      { id: "extractor", name: "Extractor" },
      { id: "processing", name: "Processing" },
      { id: "read", name: "Read" },
    ],
  },
};

export const NAMING_DIMENSION_PRESET_ORDER = [
  "data_type",
  "source",
  "pipeline_type",
  "operation_type",
  "access_type",
] as const;

export function defaultNamingDimensions(): Record<string, ListDimension> {
  const out: Record<string, ListDimension> = {};
  NAMING_DIMENSION_PRESET_ORDER.forEach((key, i) => {
    const preset = NAMING_DIMENSION_PRESETS[key];
    if (preset) out[key] = { order: (i + 1) * 10, ...preset };
  });
  return out;
}

export function emptyLocationNode(): LocationNode {
  return { id: "", name: "", description: "", locations: [] };
}

export function emptyGovernanceDocument(): GovernanceDocument {
  return {
    governance_ui: { mirror_config_paths: [] },
    scope_hierarchy: {
      type: "hierarchy",
      levels: ["site", "unit", "area", "system"],
      locations: [],
    },
    dimensions: defaultNamingDimensions(),
    spaces: {
      template: "templates/spaces/default.Space.template.yaml",
      output_dir: "spaces",
      nodes: "leaves",
      instance_space_id_template: DEFAULT_INSTANCE_SPACE_ID_TEMPLATE,
      name_template: DEFAULT_SPACE_NAME_TEMPLATE,
      combine_with: ["source", "data_type"],
    },
    groups: {
      template: "templates/groups/global.Group.template.yaml",
      output_dir: "auth",
      nodes: "leaves",
      name_template: DEFAULT_GROUP_NAME_TEMPLATE,
      display_name_template: DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE,
      combine_with: ["access_type", "data_type"],
      global: { source_ids: {} },
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
