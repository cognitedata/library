import type { JsonObject } from "./jsonConfig";

export type StorageBackend = "raw" | "dm";

export type RawTermPartitionBucketMode = "script_aware" | "ascii_first_char";

export type RawTermPartitionConfig = {
  enabled: boolean;
  strategy: "first_char";
  activateAboveRows: number;
  bucketMode: RawTermPartitionBucketMode;
};

export type SourceType = "asset_metadata" | "file_metadata";

export type ExtractMode = "passthrough" | "regex";

export type GeneralConfig = {
  name: string;
  organization: string;
  indexStorageBackend: StorageBackend;
  indexRawDatabase: string;
  termPartition: RawTermPartitionConfig;
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
  extractMode: ExtractMode;
  extractPattern: string;
};

export type IndexFieldView = {
  view: string;
  viewSpace: string;
  version: string;
  instanceSpaces: string[];
  filters: JsonObject[];
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
  watchViewKeys: string[];
};

export type TargetDrivenQueryConfig = {
  queryProperty: string;
  queryPropertyFallbacks: string[];
  excludeEmptyAliases: boolean;
};

export type VirtualTagTermSelectionMode = "all" | "missing_tags_only";

export type VirtualTagCreationConfig = {
  enabled: boolean;
  incrementalEnabled: boolean;
  termSelectionMode: VirtualTagTermSelectionMode;
  instanceSpace: string;
  missingTagCriteria: {
    requirePatternDetection: boolean;
    checkExistingCogniteAsset: boolean;
    excludeWithCogniteAssetMetadata: boolean;
  };
};

/** CDM preset link keys shipped in config/direct_relation.cdm_preset.yaml */
export const CDM_PRESET_LINK_KEYS = [
  "file_to_asset",
  "equipment_to_asset",
  "equipment_to_file",
  "timeseries_to_asset",
  "timeseries_to_equipment",
] as const;

export type CdmPresetLinkKey = (typeof CDM_PRESET_LINK_KEYS)[number];

export const WRITE_MODES = ["direct_relation", "edge", "diagram_annotation"] as const;

export type WriteMode = (typeof WRITE_MODES)[number];

export type DmViewRef = {
  space: string;
  externalId: string;
  version: string;
};

export type DiagramAnnotationLinkConfig = {
  createStatus: string;
  updateEndNodeOnly: boolean;
  fileFromReference: boolean;
  annotationIdPath: string;
};

export type DirectRelationLinkConfig = {
  label?: string;
  enabled: boolean;
  writeModes: WriteMode[];
  forwardView: string;
  targetView: string;
  property: string;
  cardinality: "list" | "single";
  overwriteExisting: boolean;
  incomingViews: string[];
  sourceTypes: string[];
  edgeViewKey: string;
  diagramAnnotation: DiagramAnnotationLinkConfig;
  resolveByIncomingView: unknown;
};

export type DirectRelationConfig = {
  enabled: boolean;
  minConfidence: number;
  allowedAnnotationStatuses: string[];
  requireAnnotationStatus: string;
  sourceTypes: string[];
  maxListSize: number;
  linkOrder: string[];
  views: Record<string, DmViewRef>;
  edgeViews: Record<string, DmViewRef>;
  links: Record<string, DirectRelationLinkConfig>;
};

/** @deprecated Use DirectRelationConfig */
export type DirectRelationTopLevel = Pick<
  DirectRelationConfig,
  | "enabled"
  | "minConfidence"
  | "allowedAnnotationStatuses"
  | "sourceTypes"
  | "maxListSize"
> & { linkEnabled: Record<string, boolean> };

export type ConfigSection =
  | "general"
  | "scope"
  | "indexFields"
  | "annotation"
  | "targetDriven"
  | "virtualTags"
  | "linking";

export function emptyRawTermPartitionConfig(): RawTermPartitionConfig {
  return {
    enabled: false,
    strategy: "first_char",
    activateAboveRows: 400_000,
    bucketMode: "script_aware",
  };
}

export function emptyGeneralConfig(): GeneralConfig {
  return {
    name: "inverted_index_contextualization",
    organization: "YourOrg",
    indexStorageBackend: "raw",
    indexRawDatabase: "db_contextualization_idx",
    termPartition: emptyRawTermPartitionConfig(),
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
    extractMode: "passthrough",
    extractPattern: "",
  };
}

export function emptyIndexFieldView(): IndexFieldView {
  return {
    view: "",
    viewSpace: "cdf_cdm",
    version: "v1",
    instanceSpaces: [],
    filters: [],
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
    watchViewKeys: ["asset", "file"],
  };
}

export function emptyTargetDrivenQueryConfig(): TargetDrivenQueryConfig {
  return {
    queryProperty: "aliases",
    queryPropertyFallbacks: ["name"],
    excludeEmptyAliases: false,
  };
}

export function emptyVirtualTagCreationConfig(): VirtualTagCreationConfig {
  return {
    enabled: false,
    incrementalEnabled: true,
    termSelectionMode: "missing_tags_only",
    instanceSpace: "inst_virtual_tags",
    missingTagCriteria: {
      requirePatternDetection: true,
      checkExistingCogniteAsset: true,
      excludeWithCogniteAssetMetadata: true,
    },
  };
}

export function emptyDiagramAnnotationLinkConfig(): DiagramAnnotationLinkConfig {
  return {
    createStatus: "Suggested",
    updateEndNodeOnly: true,
    fileFromReference: true,
    annotationIdPath: "additional_metadata.annotation_external_id",
  };
}

export function emptyDmViewRef(overrides?: Partial<DmViewRef>): DmViewRef {
  return {
    space: "cdf_cdm",
    externalId: "",
    version: "v1",
    ...overrides,
  };
}

export function defaultDirectRelationLinkConfig(
  forwardView = "",
  targetView = "",
  property = ""
): DirectRelationLinkConfig {
  return {
    enabled: true,
    writeModes: ["direct_relation"],
    forwardView,
    targetView,
    property,
    cardinality: "list",
    overwriteExisting: false,
    incomingViews: forwardView && targetView ? [forwardView, targetView] : [],
    sourceTypes: [],
    edgeViewKey: "",
    diagramAnnotation: emptyDiagramAnnotationLinkConfig(),
    resolveByIncomingView: undefined,
  };
}

export const DEFAULT_DIRECT_RELATION_VIEWS: Record<string, DmViewRef> = {
  file: { space: "cdf_cdm", externalId: "CogniteFile", version: "v1" },
  asset: { space: "cdf_cdm", externalId: "CogniteAsset", version: "v1" },
  equipment: { space: "cdf_cdm", externalId: "CogniteEquipment", version: "v1" },
  timeseries: { space: "cdf_cdm", externalId: "CogniteTimeSeries", version: "v1" },
  diagram_annotation: {
    space: "cdf_cdm",
    externalId: "CogniteDiagramAnnotation",
    version: "v1",
  },
};

export const DEFAULT_EDGE_VIEWS: Record<string, DmViewRef> = {
  file_asset_link: { space: "cdf_cdm", externalId: "FileAssetLink", version: "v1" },
  equipment_asset_link: {
    space: "cdf_cdm",
    externalId: "EquipmentAssetLink",
    version: "v1",
  },
};

export function emptyDirectRelationConfig(): DirectRelationConfig {
  return {
    enabled: true,
    minConfidence: 0.6,
    allowedAnnotationStatuses: ["Suggested", "Approved"],
    requireAnnotationStatus: "",
    sourceTypes: [
      "diagram_annotation_pattern",
      "diagram_annotation_standard",
      "asset_metadata",
      "file_metadata",
    ],
    maxListSize: 1000,
    linkOrder: [...CDM_PRESET_LINK_KEYS],
    views: { ...DEFAULT_DIRECT_RELATION_VIEWS },
    edgeViews: { ...DEFAULT_EDGE_VIEWS },
    links: {},
  };
}

export function emptyDirectRelationTopLevel(): DirectRelationTopLevel {
  const cfg = emptyDirectRelationConfig();
  return {
    enabled: cfg.enabled,
    minConfidence: cfg.minConfidence,
    allowedAnnotationStatuses: cfg.allowedAnnotationStatuses,
    sourceTypes: cfg.sourceTypes,
    maxListSize: cfg.maxListSize,
    linkEnabled: Object.fromEntries(
      Object.entries(cfg.links).map(([k, v]) => [k, v.enabled])
    ),
  };
}

export function linkDisplayLabel(
  linkKey: string,
  link: DirectRelationLinkConfig | undefined
): string {
  if (link?.label?.trim()) {
    return link.label.trim();
  }
  if (link?.forwardView && link?.targetView) {
    return `${link.forwardView} → ${link.targetView}`;
  }
  return linkKey;
}
