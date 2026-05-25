import { describe, expect, it } from "vitest";
import type { TreeNode } from "../types/discoveryNodes";
import {
  entityDropMenuOptions,
  entityDropStages,
  seedConfigForEntityDrop,
} from "./dataTreeEntityDrop";

const dmView: TreeNode = {
  id: "v1",
  label: "Asset",
  kind: "dm_view",
  open_target: {
    type: "dm_instances",
    view_space: "cdf_cdm",
    view_external_id: "CogniteAsset",
    view_version: "v1",
  },
};

describe("dataTreeEntityDrop", () => {
  it("maps dm view to query_view and save_view", () => {
    expect(entityDropStages(dmView)).toEqual({ query: "query_view", save: "save_view" });
    const q = seedConfigForEntityDrop(dmView, "query_view");
    expect(q).toMatchObject({
      description: "Asset",
      view_external_id: "CogniteAsset",
      batch_size: 1000,
    });
    const s = seedConfigForEntityDrop(dmView, "save_view");
    expect(s).toMatchObject({
      description: "Asset",
      view_external_id: "CogniteAsset",
    });
    expect(s).not.toHaveProperty("batch_size");
  });

  it("offers query, save, and query+save pair menu options for droppable entities", () => {
    const options = entityDropMenuOptions(dmView);
    expect(options).toHaveLength(3);
    expect(options?.map((o) => o.kind)).toEqual(["stage", "stage", "query_save_pair"]);
    expect(options?.filter((o) => o.kind === "stage").map((o) => o.stage)).toEqual([
      "query_view",
      "save_view",
    ]);
  });
});
