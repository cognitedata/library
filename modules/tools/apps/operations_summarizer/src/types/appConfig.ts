import type {
  DataTypeName,
  ViewId,
  WorkOrderSubcategory,
} from "@/types/operations";

/** Formatter names stored on the config node. Resolved to functions in dataTypes.ts. */
export type FormatterName = "assetsList" | "isoDate";

export interface AppConfigViewIds {
  summaryOutputFile: ViewId;
  file: ViewId;
  asset: ViewId;
  timeSeries: ViewId;
  activity: ViewId;
  equipment: ViewId;
}

export interface AppConfigTableColumn {
  header: string;
  property: string;
  default?: string;
  formatter?: FormatterName;
}

export interface AppConfigDataType {
  displayName: string;
  /** Key into `dataModel.views` (not `summaryOutputFile`). */
  view: Exclude<keyof AppConfigViewIds, "summaryOutputFile">;
  enabledByDefault: boolean;
  isSpecialCase: boolean;
  subcategories?: WorkOrderSubcategory[];
  tableColumns: AppConfigTableColumn[];
}

export interface AppConfig {
  schemaVersion: number;
  companyName: string;
  spaces: {
    summaries: string;
    /** Source-data instance space read by the podcast function. */
    dataInstances: string;
  };
  externalIdPrefixes: {
    operationSummary: string;
    testReport: string;
    /** Podcast script/audio files live in `spaces.summaries` under this prefix. */
    podcast: string;
  };
  functions: {
    summarizer: {
      externalId: string;
      pollIntervalMs: number;
      timeoutMs: number;
      maxConsecutivePollErrors: number;
    };
  };
  agent: {
    name: string;
    externalIdSuffix: string;
    defaultLanguageModel: string;
    maxConcurrentTasks: number;
  };
  dataModel: {
    views: AppConfigViewIds;
    summaryListViewProperties: string[];
    cdfAlphaVersionHeader: string;
  };
  limits: {
    chunkSize: number;
    /** DMS `instances.query` page size. Hard ceiling is 1000. */
    instanceQueryLimit: number;
    maxInstancesPerDatatype: number;
    summaryListPageSize: number;
    operationSummaryPickerMax: number;
    defaultPageSize: number;
    retry: {
      baseMs: number;
      maxDelayMs: number;
    };
    concurrency: {
      default: number;
      delete: number;
      agent: number;
    };
  };
  dataTypes: Record<DataTypeName, AppConfigDataType>;
  prompts: {
    header: string;
    holistic: string;
    agentInstructions: string;
    dataTypes: Record<DataTypeName, string>;
  };
  defaults: {
    timezoneOffsetHours: number;
    summaryPeriodDays: number;
    personaPresets: Record<string, string[]>;
  };
}
