import { describe, expect, it } from "vitest";
import { filterGridRows } from "./sqlGridFilter";

describe("filterGridRows", () => {
  const columns = ["id", "name"];
  const items = [
    { id: 1, name: "Alpha" },
    { id: 2, name: "Beta" },
  ];

  it("returns all rows when query is empty", () => {
    expect(filterGridRows(items, columns, "")).toEqual(items);
    expect(filterGridRows(items, columns, "   ")).toEqual(items);
  });

  it("filters case-insensitively across columns", () => {
    expect(filterGridRows(items, columns, "alpha")).toHaveLength(1);
    expect(filterGridRows(items, columns, "2")).toHaveLength(1);
  });
});
