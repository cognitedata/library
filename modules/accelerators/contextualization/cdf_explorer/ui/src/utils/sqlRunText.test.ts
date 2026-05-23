import { describe, expect, it } from "vitest";
import { queryTextForRun } from "./sqlRunText";

describe("queryTextForRun", () => {
  it("returns trimmed selection when non-empty", () => {
    const q = "SELECT * FROM a;\nSELECT id FROM b;";
    const start = q.indexOf("SELECT id");
    const end = start + "SELECT id FROM b;".length;
    expect(queryTextForRun(q, start, end)).toBe("SELECT id FROM b;");
  });

  it("returns full query when selection is empty or whitespace", () => {
    const q = "SELECT * FROM assets";
    expect(queryTextForRun(q, 0, 0)).toBe("SELECT * FROM assets");
    expect(queryTextForRun(q, 5, 5)).toBe("SELECT * FROM assets");
    expect(queryTextForRun(q, 6, 7)).toBe("SELECT * FROM assets");
  });
});
