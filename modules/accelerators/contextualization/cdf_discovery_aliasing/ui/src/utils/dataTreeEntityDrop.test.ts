import { describe, expect, it } from "vitest";
import {
  canDropDataTreeEntity,
  entityDropStages,
  seedConfigForEntityDrop,
} from "./dataTreeEntityDrop";
import type { TreeNode } from "../types/dataTree";

describe("dataTreeEntityDrop", () => {
  const dmView: TreeNode = {
    id: "dm:model:cdf_cdm:CogniteCore:v1:view:cdf_cdm:CogniteAsset:v1",
    label: "CogniteAsset (v1)",
    kind: "dm_view",
    has_children: false,
    open_target: {
      type: "dm_instances",
      view_space: "cdf_cdm",
      view_external_id: "CogniteAsset",
      view_version: "v1",
    },
  };

  it("maps dm view to query_view and save_view", () => {
    expect(canDropDataTreeEntity(dmView)).toBe(true);
    expect(entityDropStages(dmView)).toEqual({ query: "query_view", save: "save_view" });
    const q = seedConfigForEntityDrop(dmView, "query_view");
    expect(q.view_external_id).toBe("CogniteAsset");
    expect(q.incremental_change_processing).toBe(true);
  });

  const rawTable: TreeNode = {
    id: "raw:db:db1:table:t1",
    label: "t1",
    kind: "raw_table",
    has_children: false,
    open_target: { type: "raw_rows", database: "db1", table: "t1" },
  };

  it("maps raw table to query_raw and save_raw", () => {
    expect(entityDropStages(rawTable)).toEqual({ query: "query_raw", save: "save_raw" });
    const cfg = seedConfigForEntityDrop(rawTable, "query_raw");
    expect(cfg.source_raw_db).toBe("db1");
    expect(cfg.source_raw_table_key).toBe("t1");
  });
});
