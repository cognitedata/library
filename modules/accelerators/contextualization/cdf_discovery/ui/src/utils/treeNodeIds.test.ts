import { describe, expect, it } from "vitest";
import { DATA_SAVED_QUERIES, dedupeNodeIds } from "./treeNodeIds";

describe("dedupeNodeIds", () => {
  it("trims, dedupes, and drops empty entries", () => {
    expect(dedupeNodeIds([" dm ", "dm", "", "data:sq", "data:sq"])).toEqual(["dm", "data:sq"]);
  });

  it("does not rewrite legacy ids", () => {
    expect(dedupeNodeIds(["sq", DATA_SAVED_QUERIES])).toEqual(["sq", DATA_SAVED_QUERIES]);
  });
});
