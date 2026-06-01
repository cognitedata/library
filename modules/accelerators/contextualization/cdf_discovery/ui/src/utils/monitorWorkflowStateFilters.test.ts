import { describe, expect, it } from "vitest";
import { filterMonitorWorkflows } from "./monitorWorkflowStateFilters";

describe("filterMonitorWorkflows", () => {
  const rows = [
    {
      workflow_id: "wf_a",
      label: "Workflow A",
      sources: ["cdf"],
      run_count: 3,
      failed_count: 1,
      succeeded_count: 2,
      running_count: 0,
      failure_rate: 0.33,
      latest_status: "failed",
    },
    {
      workflow_id: "wf_b",
      label: "Workflow B",
      sources: ["local"],
      run_count: 1,
      failed_count: 0,
      succeeded_count: 0,
      running_count: 1,
      failure_rate: 0,
      latest_status: "running",
    },
  ];

  it("filters by status", () => {
    const out = filterMonitorWorkflows(rows, {
      search: "",
      status: "running",
      source: "all",
    });
    expect(out).toHaveLength(1);
    expect(out[0].workflow_id).toBe("wf_b");
  });

  it("filters by source", () => {
    const out = filterMonitorWorkflows(rows, {
      search: "",
      status: "all",
      source: "cdf",
    });
    expect(out).toHaveLength(1);
    expect(out[0].workflow_id).toBe("wf_a");
  });

  it("filters by search against id and label", () => {
    const byId = filterMonitorWorkflows(rows, {
      search: "wf_b",
      status: "all",
      source: "all",
    });
    const byLabel = filterMonitorWorkflows(rows, {
      search: "workflow a",
      status: "all",
      source: "all",
    });
    expect(byId).toHaveLength(1);
    expect(byLabel).toHaveLength(1);
    expect(byId[0].workflow_id).toBe("wf_b");
    expect(byLabel[0].workflow_id).toBe("wf_a");
  });
});
