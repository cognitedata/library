import { describe, expect, it } from "vitest";
import {
  getLatestByCreatedTime,
  sortByCreatedTimeDesc,
  type WithCreatedTime,
} from "./array";

describe("sortByCreatedTimeDesc", () => {
  it("returns new array sorted by createdTime descending", () => {
    const items: WithCreatedTime[] = [
      { createdTime: 100 },
      { createdTime: 300 },
      { createdTime: 200 },
    ];
    const sorted = sortByCreatedTimeDesc(items);
    expect(sorted.map((x) => x.createdTime)).toEqual([300, 200, 100]);
    expect(sorted).not.toBe(items);
  });

  it("handles Date createdTime", () => {
    const items: WithCreatedTime[] = [
      { createdTime: new Date("2024-01-01") },
      { createdTime: new Date("2024-01-03") },
      { createdTime: new Date("2024-01-02") },
    ];
    const sorted = sortByCreatedTimeDesc(items);
    expect(sorted[0].createdTime).toEqual(new Date("2024-01-03"));
  });

  it("treats missing createdTime as 0", () => {
    const items: WithCreatedTime[] = [{}, { createdTime: 10 }];
    const sorted = sortByCreatedTimeDesc(items);
    expect(sorted[0].createdTime).toBe(10);
  });

  it("returns copy of empty array", () => {
    const items: WithCreatedTime[] = [];
    const sorted = sortByCreatedTimeDesc(items);
    expect(sorted).toEqual([]);
    expect(sorted).not.toBe(items);
  });
});

describe("getLatestByCreatedTime", () => {
  it("returns latest item by createdTime", () => {
    const items: WithCreatedTime[] = [
      { createdTime: 100 },
      { createdTime: 300 },
      { createdTime: 200 },
    ];
    expect(getLatestByCreatedTime(items)?.createdTime).toBe(300);
  });

  it("returns null for empty array", () => {
    expect(getLatestByCreatedTime([])).toBeNull();
  });

  it("returns only item for single-element array", () => {
    const items = [{ createdTime: 42 }];
    expect(getLatestByCreatedTime(items)).toEqual(items[0]);
  });
});
