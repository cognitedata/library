import { describe, expect, it } from "vitest";
import {
  scopeConfigRelFromWorkflowTriggerPath,
  scopeRelToCanvasRel,
  workflowTriggerRelFromScopedCanvasRel,
} from "./canvasDocumentPath";

describe("scopeConfigRelFromWorkflowTriggerPath", () => {
  it("maps a scoped WorkflowTrigger path to the synthetic scope config rel", () => {
    expect(
      scopeConfigRelFromWorkflowTriggerPath(
        "workflows/bob/key_extraction_aliasing.bob.WorkflowTrigger.yaml"
      )
    ).toBe("workflows/bob/key_extraction_aliasing.bob.config.yaml");
  });

  it("returns null for non-WorkflowTrigger paths", () => {
    expect(scopeConfigRelFromWorkflowTriggerPath("workflows/b/foo.yaml")).toBeNull();
  });

  it("maps lowercase .workflowtrigger.yaml (matches server path checks)", () => {
    expect(
      scopeConfigRelFromWorkflowTriggerPath(
        "workflows/bob/key_extraction_aliasing.bob.workflowtrigger.yaml"
      )
    ).toBe("workflows/bob/key_extraction_aliasing.bob.config.yaml");
  });
});

describe("workflowTriggerRelFromScopedCanvasRel", () => {
  it("maps scoped canvas to WorkflowTrigger path", () => {
    expect(
      workflowTriggerRelFromScopedCanvasRel(
        "workflows/site_01/key_extraction_aliasing.site_01.canvas.yaml"
      )
    ).toBe("workflows/site_01/key_extraction_aliasing.site_01.WorkflowTrigger.yaml");
  });

  it("returns null for non-scoped paths", () => {
    expect(workflowTriggerRelFromScopedCanvasRel("workflow.local.canvas.yaml")).toBeNull();
  });

  it("maps canvas path with varied casing to canonical WorkflowTrigger suffix", () => {
    expect(
      workflowTriggerRelFromScopedCanvasRel(
        "workflows/site_01/key_extraction_aliasing.site_01.Canvas.yaml"
      )
    ).toBe("workflows/site_01/key_extraction_aliasing.site_01.WorkflowTrigger.yaml");
  });
});

describe("scopeRelToCanvasRel", () => {
  it("is consistent with the synthetic path from a trigger", () => {
    const scope = "workflows/a/key_extraction_aliasing.a.config.yaml";
    expect(scopeRelToCanvasRel(scope)).toBe("workflows/a/key_extraction_aliasing.a.canvas.yaml");
  });
});
