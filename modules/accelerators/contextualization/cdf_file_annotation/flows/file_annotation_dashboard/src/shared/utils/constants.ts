// Field names used across the application
export const FieldNames = {
  // Camel Case Fields
  EXTERNAL_ID: "externalId",
  CREATED_TIME: "createdTime",
  LAST_UPDATED_TIME: "lastUpdatedTime",
  FILE_EXTERNAL_ID: "fileExternalId",
  FILE_SPACE: "fileSpace",
  FILE_NAME: "fileName",
  FILE_SOURCE_ID: "fileSourceId",
  FILE_MIME_TYPE: "fileMimeType",
  PAGE_COUNT: "pageCount",
  ANNOTATED_PAGE_COUNT: "annotatedPageCount",
  ANNOTATION_STATUS: "annotationStatus",
  LINKED_FILE: "linkedFile",
  START_NODE_TEXT: "startNodeText",
  START_NODE: "startNode",
  END_NODE: "endNode",
  END_NODE_RESOURCE_TYPE: "endNodeResourceType",
  NORMALIZED_STATUS: "normalizedStatus",
  STATUS: "status",
  NAME: "name",
  SOURCE_ID: "sourceId",
  TAGS: "tags",

  // View/Pipeline config fields
  LAUNCH_FUNCTION_ID: "launchFunctionId",
  LAUNCH_FUNCTION_CALL_ID: "launchFunctionCallId",
  FINALIZE_FUNCTION_ID: "finalizeFunctionId",
  FINALIZE_FUNCTION_CALL_ID: "finalizeFunctionCallId",
  PREPARE_FUNCTION_ID: "prepareFunctionId",
  PREPARE_FUNCTION_CALL_ID: "prepareFunctionCallId",
  PROMOTE_FUNCTION_ID: "promoteFunctionId",
  PROMOTE_FUNCTION_CALL_ID: "promoteFunctionCallId",

  // Computed/Display Fields
  COVERAGE_PCT: "coverage_pct",
  ACTUAL_COUNT: "actual_count",
  POTENTIAL_COUNT: "potential_count",
  TOTAL_POSSIBLE: "total_possible",
  RESOURCE_TYPE: "resource_type",
  PATTERN_SCOPE: "pattern_scope",
  ANNOTATION_TYPE: "annotation_type",
  OCCURRENCES: "occurrences",
  ASSOCIATED_FILES: "associatedFiles",
} as const;

// Field names for Raw table columns (camelCase as stored in Raw)
export const FIELD_NAMES = {
  EXTERNAL_ID: "externalId",
  START_NODE_RESOURCE: "startNodeResource",
  END_NODE_RESOURCE: "endNodeResource",
  STATUS: "status",
  START_NODE_TEXT: "startNodeText",
  START_NODE: "startNode",
  END_NODE_RESOURCE_TYPE: "endNodeResourceType",
  START_SOURCE_ID: "startSourceId",
  END_NODE: "endNode",
  END_NODE_SPACE: "endNodeSpace",
  TAGS: "tags",
  // Bounding box fields
  PAGE: "startNodePageNumber",
  CONFIDENCE: "confidence",
  START_NODE_X_MIN: "startNodeXMin",
  START_NODE_Y_MIN: "startNodeYMin",
  START_NODE_X_MAX: "startNodeXMax",
  START_NODE_Y_MAX: "startNodeYMax",
} as const;

// Annotation status values
export enum AnnotationStatus {
  APPROVED = "Approved",
  SUGGESTED = "Suggested",
  REJECTED = "Rejected",
}

// Normalized status for display
export enum NormalizedStatus {
  REGULARLY_ANNOTATED = "Regularly Annotated",
  AUTOMATICALLY_PROMOTED = "Automatically Promoted",
  MANUALLY_PROMOTED = "Manually Promoted",
  PATTERN_FOUND = "Pattern Found",
  NO_MATCH = "No Match",
  AMBIGUOUS = "Ambiguous",
}

// File annotation status
export enum FileAnnotationStatus {
  AWAITING = "Awaiting",
  ANNOTATED = "Annotated",
  FAILED = "Failed",
}

// Caller types for function runs
export enum CallerType {
  PREPARE = "Prepare",
  LAUNCH = "Launch",
  FINALIZE = "Finalize",
  PROMOTE = "Promote",
}

// Tags used for status classification
export const StatusTags = {
  PROMOTED_AUTO: "PromotedAuto",
  PROMOTED_MANUALLY: "PromotedManually",
  PROMOTE_ATTEMPTED: "PromoteAttempted",
  AMBIGUOUS_MATCH: "AmbiguousMatch",
} as const;

// Coverage thresholds for visualization
export const CoverageThresholds = {
  HIGH: { min: 90, label: "≥ 90%", color: "#22c55e", emoji: "🟢" },
  UPPER: { min: 75, max: 90, label: "75% - 89%", color: "#f97316", emoji: "🟠" },
  MID: { min: 25, max: 75, label: "25% - 74%", color: "#eab308", emoji: "🟡" },
  LOW: { max: 25, label: "< 25%", color: "#ef4444", emoji: "🔴" },
} as const;

// Chart colors
export const ChartColors = {
  PRIMARY: "#3b82f6",
  SECONDARY: "#8b5cf6",
  SUCCESS: "#22c55e",
  WARNING: "#f97316",
  DANGER: "#ef4444",
  MUTED: "#6b7280",
} as const;
