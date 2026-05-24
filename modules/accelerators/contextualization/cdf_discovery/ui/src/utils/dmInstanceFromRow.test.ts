/** @vitest-environment node */
import { describe, expect, it } from "vitest";
import {
  dmInstanceKindFromOpenTarget,
  parseDmInstanceRefFromRow,
  containerRefFromNodeId,
} from "./dmInstanceFromRow";

describe("dmInstanceFromRow", () => {
  it("maps open targets to instance kinds", () => {
    expect(
      dmInstanceKindFromOpenTarget({
        type: "dm_instances",
        view_space: "s",
        view_external_id: "V",
        view_version: "v1",
        instance_kind: "node",
      })
    ).toBe("node");
    expect(
      dmInstanceKindFromOpenTarget({
        type: "dm_instances",
        view_space: "cdf_cdm",
        view_external_id: "CogniteAnnotation",
        view_version: "v1",
        instance_kind: "edge",
      })
    ).toBe("edge");
    expect(
      dmInstanceKindFromOpenTarget({ type: "fusion_dm_all", entity: "nodes" })
    ).toBe("node");
    expect(
      dmInstanceKindFromOpenTarget({ type: "fusion_dm_all", entity: "edges" })
    ).toBe("edge");
  });

  it("parses node and edge refs from rows", () => {
    expect(parseDmInstanceRefFromRow({ space: "inst", externalId: "n1" }, "node")).toEqual({
      space: "inst",
      externalId: "n1",
    });
    expect(parseDmInstanceRefFromRow({ space: "inst", external_id: "e1" }, "edge")).toEqual({
      space: "inst",
      externalId: "e1",
    });
  });

  it("parses container refs from tree node ids", () => {
    expect(containerRefFromNodeId("fusion:dm:space:cdf_cdm:container:CogniteAsset")).toEqual({
      space: "cdf_cdm",
      externalId: "CogniteAsset",
    });
  });
});
