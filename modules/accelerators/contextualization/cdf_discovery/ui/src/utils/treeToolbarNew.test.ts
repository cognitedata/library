import { describe, expect, it } from "vitest";
import { resolveTreeToolbarNewAction } from "./treeToolbarNew";
import { DATA_SAVED_QUERIES, GOVERNANCE_SPACES, TRANSFORM_ROOT } from "./treeNodeIds";

describe("resolveTreeToolbarNewAction", () => {
  it("offers new pipeline under transform root and pipeline items", () => {
    expect(resolveTreeToolbarNewAction({ id: TRANSFORM_ROOT, kind: "folder" })).toEqual({
      kind: "transform_pipeline",
    });
    expect(
      resolveTreeToolbarNewAction({ id: "transform:pipeline:my_pipe", kind: "etl_pipeline" })
    ).toEqual({ kind: "transform_pipeline" });
  });

  it("offers new pipeline from template when a template is selected", () => {
    expect(
      resolveTreeToolbarNewAction({ id: "transform:template:aliasing", kind: "etl_template" })
    ).toEqual({ kind: "transform_pipeline_from_template", templateId: "aliasing" });
  });

  it("offers new saved query under saved queries branch", () => {
    expect(resolveTreeToolbarNewAction({ id: DATA_SAVED_QUERIES, kind: "folder" })).toEqual({
      kind: "saved_query",
    });
    expect(
      resolveTreeToolbarNewAction({ id: "data:sq:item:my_q", kind: "saved_query" })
    ).toEqual({ kind: "saved_query" });
  });

  it("returns null for unrelated nodes", () => {
    expect(resolveTreeToolbarNewAction({ id: "dm", kind: "folder" })).toBeNull();
    expect(resolveTreeToolbarNewAction({ id: "transform:templates", kind: "folder" })).toBeNull();
  });

  it("returns governance space artifact action under Spaces", () => {
    expect(resolveTreeToolbarNewAction({ id: GOVERNANCE_SPACES, kind: "folder" })).toEqual({
      kind: "governance_space_artifact",
      parentRel: "spaces",
    });
  });
});
