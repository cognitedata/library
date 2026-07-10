import { describe, expect, it } from "vitest";
import { GOVERNANCE_GROUPS_OUTPUT_DIR, GOVERNANCE_SPACES_OUTPUT_DIR } from "../types/governanceConfig";
import { governanceArtifactCreateContextFromNode } from "./governanceTreeNew";
import { GOVERNANCE_GROUPS, GOVERNANCE_SPACES } from "./treeNodeIds";

describe("governanceArtifactCreateContextFromNode", () => {
  it("returns spaces context for Spaces branch and artifact dirs", () => {
    expect(governanceArtifactCreateContextFromNode({ id: GOVERNANCE_SPACES, kind: "folder" })).toEqual({
      kind: "spaces",
      parentRel: GOVERNANCE_SPACES_OUTPUT_DIR,
    });
    expect(
      governanceArtifactCreateContextFromNode({
        id: "gov:spaces:adir:data_modeling%2Fspaces%2Fsite_a",
        kind: "folder",
        meta: { artifact_prefix: "data_modeling/spaces/site_a" },
      })
    ).toEqual({ kind: "spaces", parentRel: "data_modeling/spaces/site_a" });
  });

  it("returns groups context for Groups branch", () => {
    expect(governanceArtifactCreateContextFromNode({ id: GOVERNANCE_GROUPS, kind: "folder" })).toEqual({
      kind: "groups",
      parentRel: GOVERNANCE_GROUPS_OUTPUT_DIR,
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
