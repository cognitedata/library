export type MonitorRunStatus = "running" | "succeeded" | "failed" | "skipped" | "unknown" | string;

export type MonitorWorkflowRun = {
  source: "cdf" | "local" | string;
  run_id: string;
  workflow_id: string;
  workflow_version?: string | null;
  status: MonitorRunStatus;
  start_time?: string | null;
  end_time?: string | null;
  duration_ms?: number | null;
  failed_tasks?: number;
  total_tasks?: number;
  error_summary?: string | null;
  tasks?: Array<{
    task_id: string;
    status: MonitorRunStatus;
    error_summary?: string | null;
  }>;
};

export type MonitorWorkflowListItem = {
  workflow_id: string;
  label: string;
  sources: string[];
  run_count: number;
  failed_count: number;
  succeeded_count: number;
  running_count: number;
  failure_rate: number;
  latest_status: MonitorRunStatus;
  last_run_time?: string | null;
  degraded?: boolean;
};

export type MonitorWorkflowSummary = {
  workflow_count: number;
  run_count: number;
  running_workflows: number;
  succeeded_workflows: number;
  failed_workflows: number;
  degraded_workflows: number;
  status_counts: Record<string, number>;
};

export type MonitorWorkflowDetail = {
  workflow: MonitorWorkflowListItem;
  runs: MonitorWorkflowRun[];
  task_status_counts: Record<string, number>;
};

export type MonitorScheduleItem = {
  trigger_id: string;
  workflow_id: string;
  workflow_version?: string | null;
  cron_expression: string;
  entity_type: "pipeline" | "workflow" | string;
  entity_label: string;
  pipeline_refs: Array<{
    pipeline_id: string;
    pipeline_label: string;
    scope_suffix?: string;
  }>;
  recent_runs_count: number;
  avg_runtime_ms_7d?: number | null;
  last_run_time?: string | null;
  last_status?: MonitorRunStatus;
};
