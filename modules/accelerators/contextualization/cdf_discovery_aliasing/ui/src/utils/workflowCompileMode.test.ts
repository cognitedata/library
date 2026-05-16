import { describe, expect, it } from "vitest";
import {
  patchCompileWorkflowDagMode,
  readCompileWorkflowDagMode,
} from "./workflowCompileMode";

describe("workflowCompileMode", () => {
  it("defaults to canvas", () => {
    expect(readCompileWorkflowDagMode({})).toBe("canvas");
  });

  it("reads root compile_workflow_dag", () => {
    expect(readCompileWorkflowDagMode({ compile_workflow_dag: "canvas" })).toBe("canvas");
  });

  it("rejects auto", () => {
    expect(() => readCompileWorkflowDagMode({ compile_workflow_dag: "auto" })).toThrow(/auto/);
  });

  it("ignores workflow.compile_dag_mode (nested mode is unsupported)", () => {
    expect(
      readCompileWorkflowDagMode({
        compile_workflow_dag: "canvas",
        workflow: { compile_dag_mode: "legacy" },
      })
    ).toBe("canvas");
  });

  it("patch sets root canvas and strips workflow.compile_dag_mode", () => {
    const next = patchCompileWorkflowDagMode({
      workflow: { compile_dag_mode: "canvas", other: 1 },
      compile_workflow_dag: "canvas",
    });
    expect(next.compile_workflow_dag).toBe("canvas");
    expect((next.workflow as Record<string, unknown>).compile_dag_mode).toBeUndefined();
    expect((next.workflow as Record<string, unknown>).other).toBe(1);
  });
});
