import type { ReactNode } from "react";
import type { NormalizedStatus } from "./constants";

// View configuration from pipeline config
export interface ViewConfig {
  schemaSpace: string;
  externalId: string;
  version: string;
  instanceSpace?: string;
}

export interface PipelineFilter {
  targetProperty?: string;
  operator?: string;
  values?: string | string[];
  negate?: boolean;
}

export interface QueryConfig {
  targetView?: ViewConfig;
  filters?: PipelineFilter[];
  limit?: number;
}

// Pipeline configuration
export interface PipelineConfig {
  annotationStateView?: ViewConfig;
  fileView?: ViewConfig;
  assetView?: ViewConfig;
  fileResourceProperty?: string;
  assetResourceProperty?: string;
  primaryScopeProperty?: string;
  secondaryScopeProperty?: string;
  fileSearchProperty?: string;
  targetEntitiesSearchProperty?: string;
  targetEntitiesQuery?: QueryConfig | QueryConfig[];
  fileEntitiesQuery?: QueryConfig | QueryConfig[];
  rawDb?: string;
  rawTablePatternTags?: string;
  rawTableAssetTags?: string;
  rawTableFileTags?: string;
  rawTablePatternCache?: string;
  rawManualPatternsCatalog?: string;
}

// Annotation state from DMS
export interface AnnotationState {
  externalId: string;
  space: string;
  createdTime: Date;
  lastUpdatedTime: Date;
  linkedFile?: {
    externalId: string;
    space: string;
  };
  annotationStatus?: string;
  pageCount?: number;
  annotatedPageCount?: number;
  annotationMessage?: string;
  patternModeMessage?: string;
  launchFunctionId?: number;
  launchFunctionCallId?: number;
  finalizeFunctionId?: number;
  finalizeFunctionCallId?: number;
  prepareFunctionId?: number;
  prepareFunctionCallId?: number;
  promoteFunctionId?: number;
  promoteFunctionCallId?: number;
  // Enriched from file view
  fileName?: string;
  fileSourceId?: string;
  fileMimeType?: string;
  fileResourceType?: string;
  filePrimaryScope?: string;
  fileSecondaryScope?: string;
}

// Bounding box for annotation visualization
export interface BoundingBox {
  xMin: number;
  yMin: number;
  xMax: number;
  yMax: number;
}

// Annotation record (from raw tables or pattern detection)
export interface AnnotationRecord {
  externalId?: string;
  startNode?: string;
  startNodeText?: string;
  endNode?: string;
  endNodeResourceType?: string;
  status?: string;
  tags?: string | string[];
  normalizedStatus?: NormalizedStatus;
  isFromPatternTable?: boolean;
  // Enriched file metadata
  fileExternalId?: string;
  fileName?: string;
  fileSourceId?: string;
  fileResourceType?: string;
  filePrimaryScope?: string;
  fileSecondaryScope?: string;
  // Bounding box data for canvas visualization
  page?: number;
  confidence?: number;
  boundingBox?: BoundingBox;
}

// Coverage data structure
export interface CoverageData {
  coveragePct: number;
  actualCount: number;
  potentialCount: number;
  totalPossible: number;
}

// Coverage by group
export interface GroupedCoverage extends CoverageData {
  groupKey: string;
}

export interface AnnotationOverviewMetrics {
  overallCoverage: CoverageData;
  coverageByTagResourceType: GroupedCoverage[];
  coverageByFileResourceType: GroupedCoverage[];
  coverageByPrimaryScope: GroupedCoverage[];
  coverageBySecondaryScope: GroupedCoverage[];
}

// KPI data for pipeline health
export interface PipelineKPIs {
  awaitingProcessing: number;
  processedTotal: number;
  failedTotal: number;
  failureRateTotal: number;
}

// Pattern record
export interface PatternRecord {
  sample: string;
  resourceType?: string;
  annotationType?: string;
  patternScope?: string;
  entityType?: string;
  createdBy?: string;
}

// Run record for charts
export interface RunRecord {
  timestamp: Date;
  count: number;
  type: string;
}

// Pipeline run
export interface PipelineRun {
  id: string;
  status: string;
  message?: string;
  createdTime: number;
  caller?: string;
  functionId?: string;
  callId?: string;
  total?: number;
  success?: number;
  failed?: number;
}

// File aggregation row
export interface FileAggregation {
  fileExternalId: string;
  fileName?: string;
  fileSourceId?: string;
  fileResourceType?: string;
  filePrimaryScope?: string;
  fileSecondaryScope?: string;
  actualCount: number;
  potentialCount: number;
  totalPossible: number;
  coveragePct: number;
  selected?: boolean;
  hasBoundingBox?: boolean;
}

// Annotation tag for selection
export interface AnnotationTag {
  tagText: string;
  resourceType?: string;
  secondaryScope?: string;
  status?: string;
  annotationType?: string;
}

// Tab definition
export interface TabDefinition {
  id: string;
  label: string;
  icon?: ReactNode;
}

// Filter state
export interface FilterState {
  resourceType?: string;
  patternScope?: string;
  secondaryScope?: string;
  status?: string;
  timeWindow?: string;
  callerType?: string;
}

// Table column definition
export interface ColumnDef<T> {
  key: keyof T | string;
  label: string;
  sortable?: boolean;
  render?: (value: T[keyof T], row: T) => ReactNode;
  className?: string;
}

// Chart data point
export interface ChartDataPoint {
  name: string;
  value: number;
  [key: string]: string | number;
}

// Threshold bucket for coverage visualization
export interface ThresholdBucket {
  key: string;
  label: string;
  count: number;
  percentage: number;
  color: string;
  emoji: string;
}
