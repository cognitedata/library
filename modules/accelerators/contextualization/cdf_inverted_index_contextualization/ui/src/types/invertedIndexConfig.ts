export type StorageBackend = "raw" | "dm";

export type SourceType = "asset_metadata" | "file_metadata";

export type GeneralConfig = {
  name: string;
  organization: string;
  indexStorageBackend: StorageBackend;
  indexRawDatabase: string;
};

export type ScopeResolveCandidate = {
  path: string;
  extractPattern: string;
};

export type ScopeLevelPaths = Record<string, ScopeResolveCandidate[]>;

export type ScopeConfig = {
  enabled: boolean;
  levels: string[];
  scopeKeyTemplate: string;
  strictScope: boolean;
  fallbackScopeKey: string;
  annotationScopeViaLinkedFile: boolean;
  resolveFrom: Record<string, ScopeLevelPaths>;
  resolveFromDefault: ScopeLevelPaths;
};

export type IndexFieldProperty = {
  path: string;
  sourceType: SourceType;
  extractPattern: string;
};

export type IndexFieldView = {
  view: string;
  viewSpace: string;
  version: string;
  properties: IndexFieldProperty[];
};

export type AnnotationIndexConfig = {
  view: string;
  viewSpace: string;
  version: string;
  instanceType: string;
  textProperty: string;
  confidenceProperty: string;
  statusProperty: string;
  pageProperty: string;
  bboxProperties: string[];
};

export type SubscriptionConfig = {
  enabled: boolean;
  trigger: string;
  watchProperty: string;
  instanceSpaces: string[];
  assetViews: string[];
  fileViews: string[];
  defaultInstanceType: string;
};

export type DirectRelationTopLevel = {
  enabled: boolean;
  minConfidence: number;
  allowedAnnotationStatuses: string[];
  writeOnSuggestedAnnotations: boolean;
  sourceTypes: string[];
  maxListSize: number;
  linkEnabled: Record<string, boolean>;
};

export type ConfigSection =
  | "general"
  | "scope"
  | "indexFields"
  | "annotation"
  | "targetDriven"
  | "linking";

export function emptyGeneralConfig(): GeneralConfig {
  return {
    name: "inverted_index_contextualization",
    organization: "YourOrg",
    indexStorageBackend: "raw",
    indexRawDatabase: "db_contextualization_idx",
  };
}

export function emptyScopeResolveCandidate(): ScopeResolveCandidate {
  return {
    path: "",
    extractPattern: "",
  };
}

export function emptyScopeConfig(): ScopeConfig {
  return {
    enabled: false,
    levels: [],
    scopeKeyTemplate: "site:{site}|unit:{unit}",
    strictScope: false,
    fallbackScopeKey: "global",
    annotationScopeViaLinkedFile: true,
    resolveFrom: {},
    resolveFromDefault: {},
  };
}

export function emptyIndexFieldProperty(): IndexFieldProperty {
  return {
    path: "",
    sourceType: "asset_metadata",
    extractPattern: "",
  };
}

export function emptyIndexFieldView(): IndexFieldView {
  return {
    view: "",
    viewSpace: "cdf_cdm",
    version: "v1",
    properties: [emptyIndexFieldProperty()],
  };
}

export function emptyAnnotationIndexConfig(): AnnotationIndexConfig {
  return {
    view: "CogniteDiagramAnnotation",
    viewSpace: "cdf_cdm",
    version: "v1",
    instanceType: "edge",
    textProperty: "startNodeText",
    confidenceProperty: "confidence",
    statusProperty: "status",
    pageProperty: "startNodePageNumber",
    bboxProperties: [],
  };
}

export function emptySubscriptionConfig(): SubscriptionConfig {
  return {
    enabled: true,
    trigger: "instance_subscription",
    watchProperty: "aliases",
    instanceSpaces: [],
    assetViews: ["CogniteAsset"],
    fileViews: ["CogniteFile"],
    defaultInstanceType: "asset",
  };
}

export const DIRECT_RELATION_LINK_KEYS = [
  "file_to_asset",
  "equipment_to_asset",
  "equipment_to_file",
  "timeseries_to_asset",
  "timeseries_to_equipment",
] as const;

export type DirectRelationLinkKey = (typeof DIRECT_RELATION_LINK_KEYS)[number];

export function emptyDirectRelationTopLevel(): DirectRelationTopLevel {
  return {
    enabled: true,
    minConfidence: 0.6,
    allowedAnnotationStatuses: ["Suggested", "Approved"],
    writeOnSuggestedAnnotations: true,
    sourceTypes: [],
    maxListSize: 1000,
    linkEnabled: Object.fromEntries(DIRECT_RELATION_LINK_KEYS.map((k) => [k, true])),
  };
}
