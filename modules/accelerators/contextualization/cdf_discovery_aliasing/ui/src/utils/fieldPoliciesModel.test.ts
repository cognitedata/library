import { describe, expect, it } from "vitest";
import { parsePoliciesJson, policiesToConfig, rowsFromConfig } from "./fieldPoliciesModel";

describe("fieldPoliciesModel", () => {
  it("round-trips merge_list policies", () => {
    const raw = [
      {
        property: "aliases",
        strategy: "merge_list",
        merge_list: { unique: true, branch_order: "by_score" },
      },
    ];
    const rows = rowsFromConfig(raw);
    expect(rows).toEqual([
      {
        property: "aliases",
        strategy: "merge_list",
        merge_unique: true,
        branch_order: "by_score",
      },
    ]);
    expect(policiesToConfig(rows)).toEqual(raw);
  });

  it("parses JSON array", () => {
    const parsed = parsePoliciesJson(
      '[{"property":"indexKey","strategy":"merge_list","merge_list":{"unique":false,"branch_order":"by_dependency"}}]'
    );
    expect(parsed).toEqual([
      {
        property: "indexKey",
        strategy: "merge_list",
        merge_unique: false,
        branch_order: "by_dependency",
      },
    ]);
  });
});
