import { describe, expect, it } from "vitest";
import { paginateItems } from "./pagination";

describe("paginateItems", () => {
  it("splits items by page size", () => {
    const result = paginateItems([1, 2, 3, 4, 5], 1, 2);
    expect(result.items).toEqual([1, 2]);
    expect(result.totalPages).toBe(3);
    expect(result.currentPage).toBe(1);
    expect(result.totalItems).toBe(5);
  });
  it("clamps current page to valid range", () => {
    const result = paginateItems([1, 2], 10, 2);
    expect(result.currentPage).toBe(1);
  });
});
