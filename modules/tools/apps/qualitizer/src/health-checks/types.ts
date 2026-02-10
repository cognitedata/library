export type LoadState = "idle" | "loading" | "success" | "error";

export type ViewDetail = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
  properties?: Record<
    string,
    {
      container?: { space: string; externalId: string };
    }
  >;
};

export type ContainerSummary = {
  space: string;
  externalId: string;
  name?: string;
};

export type SpaceSummary = {
  space: string;
  name?: string;
};

export type RawDatabaseSummary = {
  name: string;
};

export type RawTableSummary = {
  dbName: string;
  name: string;
  rowCount?: number;
  lastUpdatedTime?: number;
  createdTime?: number;
  sampleRowCount?: number;
  sampleLastUpdatedTime?: number;
};

export type FunctionSummary = {
  id: string;
  name?: string;
  runtime?: string;
};

export type GroupSummary = {
  id: number;
  name?: string;
  capabilities: Array<Record<string, unknown>>;
};

export type NormalizedCapability = {
  name: string;
  actions?: string[];
  scope?: Record<string, unknown>;
};

export type ScheduleEntry = {
  type: "function" | "transformation" | "workflow";
  id: string;
  name: string;
  cron: string;
};
