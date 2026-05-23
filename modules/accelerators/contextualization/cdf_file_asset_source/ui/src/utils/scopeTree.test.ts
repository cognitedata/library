import { describe, expect, it } from "vitest";
import { collectDocumentRows, addFileAtPath, removeFileFromTree } from "./scopeTree";
import type { ScopeHierarchyData } from "../types/assetConfig";

const sample: ScopeHierarchyData = {
  hierarchy_levels: ["site", "system"],
  scope: [
    {
      name: "SITE",
      locations: [
        {
          name: "SYS_A",
          files: ["F-001", "F-002"],
          locations: [],
        },
      ],
    },
  ],
};

describe("scopeTree", () => {
  it("collects document rows from leaf nodes", () => {
    const rows = collectDocumentRows(sample.scope);
    expect(rows).toHaveLength(2);
    expect(rows[0]?.fileId).toBe("F-001");
    expect(rows[0]?.systemName).toBe("SYS_A");
  });

  it("adds and removes files at path", () => {
    const path = [0, 0];
    let next = addFileAtPath(sample, path, "F-003");
    expect(collectDocumentRows(next.scope)).toHaveLength(3);
    next = removeFileFromTree(next, path, "F-001");
    expect(collectDocumentRows(next.scope).map((r) => r.fileId)).toEqual(["F-002", "F-003"]);
  });
});
