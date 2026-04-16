export type RunEntry = {
  status: string;
  timeMs?: number;
  message?: string;
};

export type ResourceHealth = {
  id: string;
  name: string;
  externalId?: string;
  datasetId?: number;
  lastStatus?: string;
  lastRunMs?: number;
  runsInWindow: number;
  successful: number;
  failed: number;
  uptimePercentage: number;
  recentRuns: RunEntry[];
  fusionUrl?: string;
  extraLabel?: string;
};

export type ResourceKindLabel =
  | "Extraction pipeline"
  | "Workflow"
  | "Transformation"
  | "Function";

export type ResourceReport = {
  kindLabel: ResourceKindLabel;
  resources: ResourceHealth[];
  summary: {
    total: number;
    healthy: number;
    unhealthy: number;
    noRuns: number;
    aggregateUptime: number;
  };
  errors: Array<{
    resource: string;
    status: string;
    timeMs?: number;
    message?: string;
  }>;
  error: string | null;
};
