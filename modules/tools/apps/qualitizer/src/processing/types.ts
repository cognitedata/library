export type LoadState = "idle" | "loading" | "success" | "error";

/** Default max execution rows per processing series before the user opts in to Load all. */
export const DEFAULT_PROCESSING_EXECUTION_CAP = 5000;

export const PROCESSING_DIAGRAM_SERIES = [
  "functions",
  "transformations",
  "workflows",
  "extractors",
] as const;

export type ProcessingDiagramSeries = (typeof PROCESSING_DIAGRAM_SERIES)[number];

/** Page size for POST /functions/list while walking the full catalog. */
export const FUNCTION_LIST_PAGE_SIZE = 1000;

/** Counts of retried API sub-requests; used when some calls fail but partial data is shown. */
export type ProcessingRequestStats = {
  failed: number;
  total: number;
  /** Set when at least one failure was HTTP 403 (missing capabilities). */
  permissionsDenied?: boolean;
};

/** Granular progress while a processing data hook is in the loading state. */
export type ProcessingDataLoadProgress = {
  kind:
    | "functions_list"
    | "functions_runs"
    | "transformations_list"
    | "transformations_jobs"
    | "workflows_executions"
    | "extractors_list"
    | "extractors_runs";
  /** Count already loaded (functions, workflow rows, extractor configs, etc.). */
  loaded?: number;
  /** 1-based index of the entity currently being fetched (function, transformation, pipeline). */
  current?: number;
  /** Total entities to walk (known after the list step). */
  total?: number;
  /** API pages completed (catalog list endpoints). */
  pages?: number;
  /** Request page size for the current list operation. */
  pageSize?: number;
};

export type FunctionSummary = {
  id: string;
  name?: string;
};

export type FunctionRunSummary = {
  id?: string;
  functionId?: string;
  status?: string;
  startTime?: number;
  endTime?: number;
  createdTime?: number;
  lastUpdatedTime?: number;
};

export type TransformationSummary = {
  id: number | string;
  name?: string;
};

export type TransformationJobSummary = {
  id?: number | string;
  transformationId?: number | string;
  status?: string;
  startedTime?: number;
  finishedTime?: number;
};

export type WorkflowExecutionSummary = {
  id: string;
  workflowExternalId: string;
  version?: string;
  status: string;
  engineExecutionId?: string;
  createdTime: number;
  startTime?: number;
  endTime?: number;
  reasonForIncompletion?: string;
  metadata?: Record<string, string>;
};

export type ExtPipeConfigSummary = {
  externalId: string;
  name?: string;
  description?: string;
};

export type ExtPipeRunSummary = {
  id: number;
  status: string;
  message?: string;
  createdTime: number;
  endTime?: number;
};
