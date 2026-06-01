import type { MonitorWorkflowListItem } from "../types/monitorWorkflowState";

export type MonitorFilterStatus = "all" | "running" | "succeeded" | "failed";
export type MonitorFilterSource = "all" | "cdf" | "local";

export function filterMonitorWorkflows(
  workflows: MonitorWorkflowListItem[],
  opts: {
    search: string;
    status: MonitorFilterStatus;
    source: MonitorFilterSource;
  }
): MonitorWorkflowListItem[] {
  const query = opts.search.trim().toLowerCase();
  return workflows.filter((row) => {
    if (opts.status !== "all" && row.latest_status !== opts.status) return false;
    if (opts.source !== "all" && !row.sources.includes(opts.source)) return false;
    if (!query) return true;
    return (
      row.workflow_id.toLowerCase().includes(query) ||
      row.label.toLowerCase().includes(query)
    );
  });
}
