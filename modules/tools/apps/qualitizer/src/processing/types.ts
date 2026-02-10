export type LoadState = "idle" | "loading" | "success" | "error";

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
