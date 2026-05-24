/** @vitest-environment node */
import { describe, expect, it } from "vitest";
import { sqlQueryForOpenTarget } from "./sqlQuerySeed";

describe("sqlQuerySeed", () => {
  it("uses cdf_nodes for node views", () => {
    expect(
      sqlQueryForOpenTarget({
        type: "dm_instances",
        view_space: "cdf_cdm",
        view_external_id: "CogniteAsset",
        view_version: "v1",
        instance_kind: "node",
      })
    ).toBe("SELECT * FROM cdf_nodes('cdf_cdm', 'CogniteAsset', 'v1')");
  });

  it("uses cdf_edges for edge views such as annotations", () => {
    expect(
      sqlQueryForOpenTarget({
        type: "dm_instances",
        view_space: "cdf_cdm",
        view_external_id: "CogniteAnnotation",
        view_version: "v1",
        instance_kind: "edge",
      })
    ).toBe("SELECT * FROM cdf_edges('cdf_cdm', 'CogniteAnnotation', 'v1')");
  });
});
