import { describe, expect, it } from "vitest";
import {
  patchCompileWorkflowDagMode,
  readCompileWorkflowDagMode,
} from "./workflowCompileMode";

describe("workflowCompileMode", () => {
  it("defaults to auto", () => {
    expect(readCompileWorkflowDagMode({})).toBe("auto");
  });

  it("reads root compile_workflow_dag", () => {
    expect(readCompileWorkflowDagMode({ compile_workflow_dag: "canvas" })).toBe("canvas");
  });

  it("maps removed legacy compile mode to auto", () => {
    expect(
      readCompileWorkflowDagMode({
        workflow: { compile_dag_mode: "legacy" },
      })
    ).toBe("auto");
  });

  it("prefers root key over workflow.compile_dag_mode", () => {
    expect(
      readCompileWorkflowDagMode({
        compile_workflow_dag: "auto",
        workflow: { compile_dag_mode: "canvas" },
      })
    ).toBe("auto");
  });

  it("patch sets root and strips workflow.compile_dag_mode", () => {
    const next = patchCompileWorkflowDagMode(
      {
        workflow: { compile_dag_mode: "canvas", other: 1 },
        compile_workflow_dag: "auto",
      },
      "canvas"
    );
    expect(next.compile_workflow_dag).toBe("canvas");
    expect((next.workflow as Record<string, unknown>).compile_dag_mode).toBeUndefined();
    expect((next.workflow as Record<string, unknown>).other).toBe(1);
  });
});
