export type InstanceProperties = Record<string, unknown>;

export type LocationConfigNode = {
  space: string;
  externalId: string;
  createdTime?: number;
  lastUpdatedTime?: number;
  properties: InstanceProperties;
};

export type LegacyConfigLocation = {
  rowId: string;
  externalId: string;
  dataSetId: number | null;
  assetExternalId: string;
  appDataInstanceSpace: string;
  sourceDataInstanceSpace: string;
  templateAdmins: string[];
  checklistAdmins: string[];
  fileFilter: string[];
  assetFilter: string[];
  generalFilter: string[];
  timeseriesFilter: string[];
  raw: unknown;
};

export type LegacyConfigKey = "APP_CONFIG_V2" | "default-config";

export type LegacyAssetKind = "legacy" | "dm";

export type LegacyAssetLookupResult = {
  kind: LegacyAssetKind;
  data: unknown;
};

export type WaveLabel = "Wave 1" | "Wave 2" | "Wave 3" | "Unassigned";

export type LegacyConfigData = {
  key: LegacyConfigKey;
  locations: LegacyConfigLocation[];
  responseItems: unknown;
  error: unknown | null;
};

export type SampleCountEstimate = {
  count: number | null;
  capped: boolean;
  accurate: boolean;
};

export type LegacyLocationEstimates = {
  appSpaceNodes: SampleCountEstimate | null;
  sourceSpaceNodes: SampleCountEstimate | null;
  assetSubtreeAssets: SampleCountEstimate | null;
  filterRootCount: number;
  filterSubtreeAssets: SampleCountEstimate | null;
  dataSetAssets: SampleCountEstimate | null;
  dataSetTimeSeries: SampleCountEstimate | null;
};

export type LegacyEstimateColumnKey = "asset" | "appSpace" | "sourceSpace" | "filter" | "dataSet";

export type LegacyMigrationViewCount = {
  view: ViewSource;
  viewLabel: string;
  metrics: SpaceMetricPair;
};

export type CellLoadOutcome<T> = { ok: true; data: T } | { ok: false; error: string };

import type { ViewSource } from "@/shared/dms-types";
export type { ViewSource };

export type SpaceCountBreakdown = {
  space: string;
  count: number;
  capped: boolean;
  accurate?: boolean;
};

export type SpaceMetricPair = {
  space: string;
  viewCount: number | null;
  viewCountCapped: boolean;
  viewSpaceBreakdown: SpaceCountBreakdown[];
  allNodesCount: number | null;
  allNodesCountCapped: boolean;
  allNodesSpaceBreakdown: SpaceCountBreakdown[];
};

export type DataFilterSpaceMetricPair = SpaceMetricPair & {
  filterKeys: string[];
};

export type MappingCountMetrics = {
  mappingKey: string;
  viewLabel: string;
  view: ViewSource;
  appInstanceSpaceMetrics: SpaceMetricPair[];
  dataFilterMetrics: DataFilterSpaceMetricPair[];
};

export type LegacyLocationMigrationCounts = {
  rowId: string;
  instanceSpace: string | null;
  views: Array<{
    mappingKey: string;
    viewLabel: string;
    view: ViewSource;
    metrics: SpaceMetricPair;
  }>;
};

export type ViewCountResult = {
  locationExternalId: string;
  locationName: string;
  locationDescription: string;
  locationUpdated: string;
  location: LocationConfigNode;
  appInstanceSpace: string | null;
  appInstanceSpaceNotInDataFilters: boolean;
  mappingMetrics: MappingCountMetrics[];
};

export type ConfiguredSpaceUsageStatus = "missing" | "empty" | "in_use";

export type SpaceProbeApiCall = {
  api: string;
  request: unknown;
  response: unknown;
};

export type SpaceProbeMetric = {
  space: string;
  status: ConfiguredSpaceUsageStatus;
  sampleExternalId: string | null;
  apiCalls?: SpaceProbeApiCall[];
  statusDetail?: string;
};

export type MappingInstanceSpaceProbeMetric = SpaceProbeMetric & {
  filterKeys: string[];
  isAppInstanceSpace: boolean;
};

export type MappingSpaceProbeMetrics = {
  mappingKey: string;
  viewLabel: string;
  view: ViewSource;
  instanceSpaceMetrics: MappingInstanceSpaceProbeMetric[];
};

export type LocationSpaceProbeResult = {
  locationExternalId: string;
  locationName: string;
  locationDescription: string;
  locationUpdated: string;
  location: LocationConfigNode;
  appInstanceSpace: string | null;
  appInstanceSpaceNotInDataFilters: boolean;
  mappingMetrics: MappingSpaceProbeMetrics[];
};

export type CountBreakdownRequest = {
  locationName: string;
  columnLabel: string;
  view: ViewSource | null;
  sampledBreakdown: SpaceCountBreakdown[];
};

export type LocationConfigValidation = {
  locationExternalId: string;
  locationName: string;
  configSpace: string;
  configExternalId: string;
  appInstanceSpace: string | null;
  referenceDataSpace: string | null;
  errors: string[];
  externalIdChecked: boolean;
};

export type ViewMappingReference = {
  key: string;
  space: string;
  externalId: string;
  version: string;
};

export type ViewExistenceResult = {
  reference: ViewMappingReference;
  exists: boolean;
  view: unknown | null;
};

export type DataStorageReference = {
  rootLocationSpace: string | null;
  rootLocationExternalId: string | null;
  appInstanceSpace: string | null;
};

export type DataStorageCounts = {
  rootLocationSpaceNodeCount: number | null;
  rootLocationSpaceNodeCountCapped: boolean;
  appInstanceSpaceNodeCount: number | null;
  appInstanceSpaceNodeCountCapped: boolean;
  rootAnchorNodeCount: number | null;
  rootAnchorNodeCountCapped: boolean;
};

import type { LoadState } from "@/shared/dms-types";
export type { LoadState };

export type SampledInstanceRow = {
  space: string;
  externalId: string;
  lastUpdatedTime?: number;
  instanceType: string;
  type?: { space: string; externalId: string };
  viewSource?: ViewSource;
  properties?: Record<string, unknown>;
};

export type InfieldDataLocationOption = {
  locationExternalId: string;
  locationName: string;
  appInstanceSpace: string;
  legacyApmaInstanceSpace: string | null;
  location?: LocationConfigNode;
};

export type LegacyViewSpaceCheckResult = {
  viewKey: string;
  view: ViewSource;
  instanceSpace: string;
  count: number | null;
  capped: boolean;
  previewRows: SampledInstanceRow[];
  errorMessage: string | null;
  mappingKey?: string;
  mappingVariant?: "configured" | "default";
  defaultView?: ViewSource;
};

export type LegacyDataQualityReport = {
  instanceSpaces: string[];
  results: LegacyViewSpaceCheckResult[];
};

export type LegacyDataQualityProgress = {
  current: number;
  total: number;
  viewKey: string;
  instanceSpace: string;
};

export type ConsistencyPropertyColumn = {
  name: string;
  displayName?: string;
  type: string;
  isList: boolean;
  isDirect: boolean;
  isSpace: boolean;
  isJson: boolean;
  expectedView: ViewSource | null;
  schemaEvidence?: ConsistencyDirectRelationSchemaEvidence;
  isSyntheticInfieldCdmAsset?: boolean;
  valueSourceView?: ViewSource;
  valueSourceProperty?: string;
  pairedColumnName?: string;
};

export type ConsistencyReferenceStatus = "valid" | "invalid" | "missing" | "not_checked" | "optional_empty";

export type ConsistencyDirectRelationConstraintOrigin =
  | "view_source_view"
  | "view_source_container"
  | "container_type"
  | "none";

export type ConsistencyDirectRelationSchemaEvidence = {
  view: ViewSource;
  propertyName: string;
  viewPropertyDefinition: unknown;
  containerPropertyDefinition: unknown | null;
  sourceField: unknown;
  containerTypeField: unknown;
  constraintOrigin: ConsistencyDirectRelationConstraintOrigin;
  constraintReason: string;
};

export type ConsistencyDirectRelationRefDetail = {
  ref: { space: string; externalId: string };
  exists: boolean;
  involvedViews: DocLookupViewRef[];
  matchingViews: DocLookupViewRef[];
  matchedExpectedView: boolean;
  foundCogniteEquipmentViews: DocLookupViewRef[];
  searchedRefs?: Array<{ space: string; externalId: string }>;
  retrieveItem: unknown | null;
  inspectItem: unknown | null;
};

export type ConsistencyInfieldCdmAssetApiCall = {
  api: string;
  request: unknown;
  response: unknown;
  refs: Array<{ space: string; externalId: string }>;
};

export type ConsistencyDirectRelationDetail = {
  expectedView: ViewSource | null;
  propertyValue: unknown;
  legacyPairedPropertyValue?: unknown;
  schema: ConsistencyDirectRelationSchemaEvidence;
  infieldCdmAssetInstanceSpaces?: string[];
  infieldCdmAssetApiCalls?: ConsistencyInfieldCdmAssetApiCall[];
  refs: ConsistencyDirectRelationRefDetail[];
  retrieveRequest: unknown;
  retrieveResponse: unknown;
  inspectRequest: unknown;
  inspectResponse: unknown;
};

export type ConsistencyCellValidation = {
  status: ConsistencyReferenceStatus;
  message?: string;
  cogniteEquipmentMismatch?: boolean;
  directRelation?: ConsistencyDirectRelationDetail;
};

export type ConsistencyAnalyzeIssueFilter =
  | "rows_with_issues"
  | "cells_invalid"
  | "cells_missing"
  | "optional_asset_empty"
  | "optional_updated_by_empty"
  | "optional_created_by_empty"
  | "infieldCdm_asset_equipment"
  | "infieldCdm_invalid_reference"
  | { kind: "column"; columnName: string };

export type ConsistencyAnalyzeMatchingRows = {
  rowsWithIssues: SampledInstanceRow[];
  cellsInvalid: SampledInstanceRow[];
  cellsMissing: SampledInstanceRow[];
  optionalAssetEmpty: SampledInstanceRow[];
  optionalUpdatedByEmpty: SampledInstanceRow[];
  optionalCreatedByEmpty: SampledInstanceRow[];
  infieldCdmAssetEquipment: SampledInstanceRow[];
  infieldCdmInvalidReference: SampledInstanceRow[];
  byColumn: Record<string, SampledInstanceRow[]>;
};

export type ConsistencyAnalyzeMatchingRowKeys = {
  rowsWithIssues: Set<string>;
  cellsInvalid: Set<string>;
  cellsMissing: Set<string>;
  optionalAssetEmpty: Set<string>;
  optionalUpdatedByEmpty: Set<string>;
  optionalCreatedByEmpty: Set<string>;
  infieldCdmAssetEquipment: Set<string>;
  infieldCdmAssetEquipmentTargets: Set<string>;
  infieldCdmInvalidReference: Set<string>;
  infieldCdmInvalidReferenceTargets: Set<string>;
  byColumn: Record<string, Set<string>>;
};

export type ConsistencyAnalyzeStats = {
  rowsWithIssues: number;
  rowsFullyValid: number;
  cellsChecked: number;
  cellsValid: number;
  cellsInvalid: number;
  cellsMissing: number;
  cellsNotChecked: number;
  optionalAssetEmptyCells: number;
  optionalUpdatedByEmptyCells: number;
  optionalCreatedByEmptyCells: number;
  infieldCdmAssetEquipmentCells: number;
  infieldCdmAssetEquipmentUniqueTargets: number;
  infieldCdmInvalidReferenceCells: number;
  infieldCdmInvalidReferenceUniqueTargets: number;
  issuesByColumn: Record<string, number>;
  matchingRows: ConsistencyAnalyzeMatchingRows;
  matchingRowKeys: ConsistencyAnalyzeMatchingRowKeys;
};

export type ConsistencyAnalyzePhase = "loading" | "validating" | "done" | "error" | "cancelled";

export type ConsistencyAnalyzeProgress = {
  phase: ConsistencyAnalyzePhase;
  rowsProcessed: number;
  rowsTotalEstimate: number | null;
  pagesProcessed: number;
  currentPageRowCount: number;
  stats: ConsistencyAnalyzeStats;
  elapsedMs: number;
  etaMs: number | null;
  error?: string;
};

import type { LoadProgress } from "@/shared/dms-types";
export type InfieldLoadProgress = LoadProgress;

/** @deprecated Use InfieldLoadProgress */
export type InfieldDataProgress = InfieldLoadProgress;

export type RelevantObjectCountProgress = {
  current: number;
  total: number;
  locationName: string;
};

export type LegacyConfigLoadProgress = {
  current: number;
  total: number;
  configKey: LegacyConfigKey;
};

import type { DocLookupViewRef } from "@/data-catalog/doc-lookup/doc-lookup-types";
export type { DocLookupViewRef };
export type {
  DocLookupNodeSummary,
  PropertyValueChange,
  DocLookupViewVersionData,
  DocLookupViewRetrieveChunk,
  DocLookupViewDefinitionData,
  DocLookupNodeResult,
  DocLookupResult,
  DocLookupProgress,
  DocLookupViewSearchResult,
} from "@/data-catalog/doc-lookup/doc-lookup-types";

export type ConnectivityEntityRef = {
  space: string;
  externalId: string;
};

export type ConnectivityGraphNode = {
  id: string;
  space: string;
  externalId: string;
  label: string;
  isCenter: boolean;
  typeLabel: string | null;
  x?: number;
  y?: number;
};

export type ConnectivityGraphLink = {
  id: string;
  sourceId: string;
  targetId: string;
  kind: "edge" | "relation" | "viewRelation";
  label: string;
  details: Record<string, unknown>;
};

export type ConnectivityGraph = {
  center: ConnectivityGraphNode;
  nodes: ConnectivityGraphNode[];
  links: ConnectivityGraphLink[];
  centerViews: Array<{
    space: string;
    externalId: string;
    version: string;
  }>;
};
