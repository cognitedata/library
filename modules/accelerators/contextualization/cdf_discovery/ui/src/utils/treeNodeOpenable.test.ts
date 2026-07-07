/** @vitest-environment node */
import { describe, expect, it } from "vitest";
import type { TreeNode } from "../types/discoveryNodes";
import { treeNodeOpenable, treeNodeOpensDocumentTab } from "./treeNodeOpenable";

function node(kind: string, extra: Partial<TreeNode> = {}): TreeNode {
  return {
    id: "test",
    label: "Test",
    kind,
    has_children: false,
    ...extra,
  };
}

describe("treeNodeOpenable", () => {
  it("returns true for queryable classic resources", () => {
    const n = node("classic_resource", {
      open_target: { type: "classic_list", resource_type: "assets" },
    });
    expect(treeNodeOpenable(n)).toBe(true);
    expect(treeNodeOpensDocumentTab(n)).toBe(false);
  });

  it("returns true for record streams and governance cdf leaves", () => {
    expect(
      treeNodeOpenable(
        node("record_stream", {
          open_target: { type: "record_stream", stream_external_id: "s1" },
        })
      )
    ).toBe(true);
    expect(treeNodeOpensDocumentTab(node("gov_space", { meta: { space: "sp" } }))).toBe(true);
  });

  it("returns true for data model diagram nodes", () => {
    expect(treeNodeOpenable(node("dm_data_model"))).toBe(true);
  });
});
