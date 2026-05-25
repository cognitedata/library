import { describe, expect, it } from "vitest";
import { governanceArtifactCreateContextFromNode } from "./governanceTreeNew";
import { GOVERNANCE_GROUPS, GOVERNANCE_SPACES } from "./treeNodeIds";

describe("governanceArtifactCreateContextFromNode", () => {
  it("returns spaces context for Spaces branch and artifact dirs", () => {
    expect(governanceArtifactCreateContextFromNode({ id: GOVERNANCE_SPACES, kind: "folder" })).toEqual({
      kind: "spaces",
      parentRel: "spaces",
    });
    expect(
      governanceArtifactCreateContextFromNode({
        id: "gov:spaces:adir:spaces%2Fsite_a",
        kind: "folder",
        meta: { artifact_prefix: "spaces/site_a" },
      })
    ).toEqual({ kind: "spaces", parentRel: "spaces/site_a" });
  });

  it("returns groups context for Groups branch", () => {
    expect(governanceArtifactCreateContextFromNode({ id: GOVERNANCE_GROUPS, kind: "folder" })).toEqual({
      kind: "groups",
      parentRel: "auth",
    });
  });

  it("returns null for live CDF leaves", () => {
    expect(
      governanceArtifactCreateContextFromNode({
        id: "gov:space:inst_x",
        kind: "gov_space",
        meta: { live_cdf: true },
      })
    ).toBeNull();
  });
});
