import { describe, expect, it } from "vitest";
import { scopeConfigRelFromWorkflowTriggerPath } from "./canvasDocumentPath";

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
