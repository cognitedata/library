import { describe, expect, it } from "vitest";
import {
  DEFAULT_VIEW_QUERY_LIMIT,
  defaultQueryViewNodeConfig,
  readViewQueryLimit,
} from "./viewQueryConfigModel";

describe("viewQueryConfigModel", () => {
  it("seeds new query_view nodes with limit 1000", () => {
    expect(defaultQueryViewNodeConfig({ description: "Assets" })).toEqual({
      limit: DEFAULT_VIEW_QUERY_LIMIT,
      description: "Assets",
    });
  });

  it("defaults to 1000 when limit is unset", () => {
    expect(readViewQueryLimit({})).toBe(1000);
  });

  it("reads explicit limit and read_limit", () => {
    expect(readViewQueryLimit({ limit: 50 })).toBe(50);
    expect(readViewQueryLimit({ read_limit: 25 })).toBe(25);
    expect(readViewQueryLimit({ limit: 0 })).toBe(0);
  });
});
