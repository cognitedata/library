import { describe, expect, it } from "vitest";
import { formatSplitJoinIndexes, parseSplitJoinIndexes } from "./commaDelimited";

describe("parseSplitJoinIndexes", () => {
  it("parses comma-separated 0-based indexes", () => {
    expect(parseSplitJoinIndexes("3, 4")).toEqual([3, 4]);
    expect(parseSplitJoinIndexes("3,4")).toEqual([3, 4]);
  });

  it("returns undefined for empty input", () => {
    expect(parseSplitJoinIndexes("")).toBeUndefined();
    expect(parseSplitJoinIndexes("  ")).toBeUndefined();
  });

  it("formats indexes for display", () => {
    expect(formatSplitJoinIndexes([3, 4])).toBe("3, 4");
  });
});
