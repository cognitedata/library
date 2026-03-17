export type LoadState = "idle" | "loading" | "success" | "error";

export type GroupSummary = {
  id: number;
  name?: string;
  sourceId?: string;
  capabilities: Array<Record<string, unknown>>;
};

export type DataSetSummary = {
  id: number;
  name?: string;
};

export type SpaceSummary = {
  space: string;
  name?: string;
};

export type NormalizedCapability = {
  name: string;
  actions?: string[];
  scope?: Record<string, unknown>;
};

export type CellDetails = {
  shortText: string;
  titleText: string;
  color: string;
};

export type AccessInfoProject = {
  projectUrlName?: string;
  groups?: number[];
};

export type AccessInfo = {
  subject: string;
  projects: AccessInfoProject[];
};

export type UploadedUser = {
  id: string;
  label: string;
  data: AccessInfo;
};
