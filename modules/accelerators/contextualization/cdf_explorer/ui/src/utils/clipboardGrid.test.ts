import { describe, expect, it } from "vitest";
import { gridRowsToTsv } from "./clipboardGrid";

describe("gridRowsToTsv", () => {
  it("formats header and tab-separated rows", () => {
    const tsv = gridRowsToTsv(
      ["id", "name"],
      [
        { id: 1, name: "A" },
        { id: 2, name: "B,C" },
      ]
    );
    expect(tsv).toContain("id\tname");
    expect(tsv).toContain("2\t\"B,C\"");
  });
});
